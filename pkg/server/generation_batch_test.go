package server

import (
	"context"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"

	"github.com/osrg/gobgp/v4/api"
	"github.com/osrg/gobgp/v4/internal/pkg/table"
	"github.com/osrg/gobgp/v4/pkg/apiutil"
	"github.com/osrg/gobgp/v4/pkg/packet/bgp"
)

func deleteMany(t *testing.T, c api.GoBgpServiceClient, namespace string, generations ...uint64) *api.DeleteGenerationsResponse {
	t.Helper()
	r, err := c.DeleteGenerations(context.Background(), &api.DeleteGenerationsRequest{Namespace: namespace, Generations: generations, InstanceId: fenceInstance})
	require.NoError(t, err)
	return r
}

// outcomesByUUID maps a result's outcomes by UUID.
func outcomesByUUID(t *testing.T, r *api.GenerationDeleteResult) map[uuid.UUID]api.GenerationDeleteOutcome {
	t.Helper()
	m := map[uuid.UUID]api.GenerationDeleteOutcome{}
	for _, o := range r.Outcomes {
		m[mustUUID(t, o.Uuid)] = o.Outcome
	}
	return m
}

func withStepPaths(t *testing.T, n int) {
	old := DeleteGenerationsStepPaths
	DeleteGenerationsStepPaths = n
	t.Cleanup(func() { DeleteGenerationsStepPaths = old })
}

func withStepHook(t *testing.T, f func(step int)) {
	deleteGenerationsStepHook = f
	t.Cleanup(func() { deleteGenerationsStepHook = nil })
}

// genPrefix is the j-th prefix of generation g in a test: 10.<64+g/256>.<g%256>.<j>/32.
func genPrefix(g uint64, j int) string {
	return fmt.Sprintf("10.%d.%d.%d/32", 64+g/256, g%256, j)
}

func addGenerationPaths(t *testing.T, c api.GoBgpServiceClient, g uint64, n int) []uuid.UUID {
	t.Helper()
	ids := make([]uuid.UUID, n)
	for j := range n {
		r, err := c.AddPath(context.Background(), &api.AddPathRequest{TableType: api.TableType_TABLE_TYPE_GLOBAL, Path: ucPath(t, genPrefix(g, j)), GenerationTag: fenceTag("t", g, fmt.Sprintf("p%d", j))})
		require.NoError(t, err)
		ids[j] = mustUUID(t, r.Uuid)
	}
	return ids
}

func generationKeys(t *testing.T, gens []uint64, n int) []testKey {
	var keys []testKey
	for _, g := range gens {
		for j := range n {
			keys = append(keys, destinationKey(t, bgp.RF_IPv4_UC, genPrefix(g, j)))
		}
	}
	return keys
}

// AddPaths of the generations, a batch fence and a batch delete all queue
// behind a busy loop and run in whatever order the loop takes them. For
// every generation: when the delete ran after the fence, it deleted exactly
// the paths the fence listed and the RIB holds none; when it ran before, the
// generation is refused and its paths are the fence's. Every AddPath that
// ran after the fence is refused, and after both calls nothing lands.
func TestDeleteGenerationsAckOrdersQueuedCommands(t *testing.T) {
	h := startFenceServer(t, fenceConfig(t))
	a, b, c := h.dial(), h.dial(), h.dial()
	ctx := context.Background()
	const generations, commands = 4, 6
	for round := range 8 {
		gens := make([]uint64, generations)
		for i := range gens {
			gens[i] = uint64(1 + round*generations + i)
		}
		type cmd struct {
			gen  uint64
			path *api.Path
			id   uuid.UUID
			err  error
		}
		var cmds []*cmd
		for _, g := range gens {
			for j := range commands {
				cmds = append(cmds, &cmd{gen: g, path: ucPath(t, genPrefix(g, j))})
			}
		}
		release := blockLoop(t, h.s)
		var wg sync.WaitGroup
		var fenced *api.FenceGenerationsResponse
		var deleted *api.DeleteGenerationsResponse
		var fenceErr, deleteErr error
		for i, cm := range cmds {
			wg.Add(1)
			go func() {
				defer wg.Done()
				r, err := a.AddPath(ctx, &api.AddPathRequest{TableType: api.TableType_TABLE_TYPE_GLOBAL, Path: cm.path, GenerationTag: fenceTag("t", cm.gen, fmt.Sprint(i))})
				if err != nil {
					cm.err = err
					return
				}
				cm.id, _ = uuid.FromBytes(r.Uuid)
			}()
			switch i {
			case len(cmds) / 3:
				wg.Add(1)
				go func() {
					defer wg.Done()
					fenced, fenceErr = b.FenceGenerations(ctx, &api.FenceGenerationsRequest{Namespace: "t", Generations: gens, InstanceId: fenceInstance})
				}()
			case 2 * len(cmds) / 3:
				wg.Add(1)
				go func() {
					defer wg.Done()
					deleted, deleteErr = c.DeleteGenerations(ctx, &api.DeleteGenerationsRequest{Namespace: "t", Generations: gens, InstanceId: fenceInstance})
				}()
			}
		}
		time.Sleep(200 * time.Millisecond)
		release()
		wg.Wait()
		require.NoError(t, fenceErr)
		require.NoError(t, deleteErr)

		for i, g := range gens {
			var applied []uuid.UUID
			for _, cm := range cmds {
				if cm.gen != g {
					continue
				}
				if cm.err != nil {
					requireCode(t, cm.err, codes.FailedPrecondition)
				} else {
					applied = append(applied, cm.id)
				}
			}
			slices.SortFunc(applied, func(a, b uuid.UUID) int { return slices.Compare(a[:], b[:]) })
			require.Equal(t, applied, pathUUIDs(t, fenced.Results[i].Status, api.GenerationPathState_GENERATION_PATH_STATE_CURRENT), "round %d generation %d", round, g)
			res := deleted.Results[i]
			require.Equal(t, g, res.Generation)
			outcomes := outcomesByUUID(t, res)
			var present []string
			for _, d := range lookup(t, h, generationKeys(t, []uint64{g}, commands)...).Destinations {
				for _, p := range d.Paths {
					present = append(present, fmt.Sprint(p.Path.Nlri))
				}
			}
			if res.Refused {
				// The delete ran before the fence: nothing of it was touched.
				for id, o := range outcomes {
					require.Equal(t, api.GenerationDeleteOutcome_GENERATION_DELETE_OUTCOME_REFUSED, o)
					require.Contains(t, applied, id)
				}
				require.Len(t, present, len(applied))
				continue
			}
			require.Len(t, outcomes, len(applied))
			for _, id := range applied {
				require.Equal(t, api.GenerationDeleteOutcome_GENERATION_DELETE_OUTCOME_DELETED, outcomes[id])
			}
			require.Zero(t, res.Status.CurrentPaths)
			require.Empty(t, present, "round %d generation %d", round, g)
		}
		// A second delete finishes whatever the first refused, and nothing
		// more lands.
		again := deleteMany(t, c, "t", gens...)
		for _, res := range again.Results {
			require.False(t, res.Refused)
			require.Zero(t, res.Status.CurrentPaths)
		}
		for _, g := range gens {
			_, err := a.AddPath(ctx, &api.AddPathRequest{TableType: api.TableType_TABLE_TYPE_GLOBAL, Path: ucPath(t, genPrefix(g, 99)), GenerationTag: fenceTag("t", g, "late")})
			requireCode(t, err, codes.FailedPrecondition)
		}
		for _, d := range lookup(t, h, generationKeys(t, gens, commands)...).Destinations {
			require.Empty(t, d.Paths)
		}
	}
}

// A path that another generation took over is kept, older or newer, also
// when it is taken over between two steps of the delete.
func TestDeleteGenerationsKeepsReplacedPaths(t *testing.T) {
	withStepPaths(t, 1)
	h := startFenceServer(t, fenceConfig(t))
	c := h.dial()
	ctx := context.Background()
	g10 := addGenerationPaths(t, c, 10, 3)
	// Generation 11 (newer) takes key 0, generation 9 (older) takes key 1.
	r11, err := c.AddPath(ctx, &api.AddPathRequest{TableType: api.TableType_TABLE_TYPE_GLOBAL, Path: fenceTestPath(t, bgp.RF_IPv4_UC, genPrefix(10, 0), "10.0.0.2"), GenerationTag: fenceTag("t", 11, "x")})
	require.NoError(t, err)
	r9, err := c.AddPath(ctx, &api.AddPathRequest{TableType: api.TableType_TABLE_TYPE_GLOBAL, Path: fenceTestPath(t, bgp.RF_IPv4_UC, genPrefix(10, 1), "10.0.0.3"), GenerationTag: fenceTag("t", 9, "x")})
	require.NoError(t, err)
	g12 := addGenerationPaths(t, c, 12, 2)
	fenceMany(t, c, "t", 10, 12)

	// Between the first and the second step, generation 13 takes key 1 of
	// generation 12.
	var taken *api.AddPathResponse
	withStepHook(t, func(step int) {
		if step == 1 {
			r, err := c.AddPath(ctx, &api.AddPathRequest{TableType: api.TableType_TABLE_TYPE_GLOBAL, Path: fenceTestPath(t, bgp.RF_IPv4_UC, genPrefix(12, 1), "10.0.0.4"), GenerationTag: fenceTag("t", 13, "x")})
			require.NoError(t, err)
			taken = r
		}
	})
	// Order: generation 12 first, so its key 0 goes in step 0 and key 1 in step 1.
	r := deleteMany(t, c, "t", 12, 10)
	require.NotNil(t, taken)
	require.GreaterOrEqual(t, r.Steps, uint32(3))

	o12 := outcomesByUUID(t, r.Results[0])
	require.Equal(t, api.GenerationDeleteOutcome_GENERATION_DELETE_OUTCOME_DELETED, o12[g12[0]])
	require.Equal(t, api.GenerationDeleteOutcome_GENERATION_DELETE_OUTCOME_REPLACED, o12[g12[1]])
	o10 := outcomesByUUID(t, r.Results[1])
	require.Equal(t, api.GenerationDeleteOutcome_GENERATION_DELETE_OUTCOME_REPLACED, o10[g10[0]])
	require.Equal(t, api.GenerationDeleteOutcome_GENERATION_DELETE_OUTCOME_REPLACED, o10[g10[1]])
	require.Equal(t, api.GenerationDeleteOutcome_GENERATION_DELETE_OUTCOME_DELETED, o10[g10[2]])

	l := lookup(t, h, destinationKey(t, bgp.RF_IPv4_UC, genPrefix(10, 0)), destinationKey(t, bgp.RF_IPv4_UC, genPrefix(10, 1)),
		destinationKey(t, bgp.RF_IPv4_UC, genPrefix(10, 2)), destinationKey(t, bgp.RF_IPv4_UC, genPrefix(12, 0)), destinationKey(t, bgp.RF_IPv4_UC, genPrefix(12, 1)))
	owner := func(i int) (uint64, []byte) {
		require.Len(t, l.Destinations[i].Paths, 1)
		return l.Destinations[i].Paths[0].Owner.Generation, l.Destinations[i].Paths[0].Uuid
	}
	g, id := owner(0)
	require.EqualValues(t, 11, g)
	require.Equal(t, r11.Uuid, id)
	g, id = owner(1)
	require.EqualValues(t, 9, g)
	require.Equal(t, r9.Uuid, id)
	require.Empty(t, l.Destinations[2].Paths)
	require.Empty(t, l.Destinations[3].Paths)
	g, id = owner(4)
	require.EqualValues(t, 13, g)
	require.Equal(t, taken.Uuid, id)

	// Idempotent: a second call changes nothing and reports the same keys.
	again := deleteMany(t, c, "t", 12, 10)
	require.Equal(t, api.GenerationDeleteOutcome_GENERATION_DELETE_OUTCOME_ALREADY_DELETED, outcomesByUUID(t, again.Results[0])[g12[0]])
	require.Equal(t, api.GenerationDeleteOutcome_GENERATION_DELETE_OUTCOME_REPLACED, outcomesByUUID(t, again.Results[0])[g12[1]])
	require.Equal(t, api.GenerationDeleteOutcome_GENERATION_DELETE_OUTCOME_ALREADY_DELETED, outcomesByUUID(t, again.Results[1])[g10[2]])
	require.Len(t, lookup(t, h, destinationKey(t, bgp.RF_IPv4_UC, genPrefix(10, 0))).Destinations[0].Paths, 1)
}

// A path the index names current but the RIB no longer holds is reported
// already absent.
func TestDeleteGenerationsAlreadyAbsent(t *testing.T) {
	h := startFenceServer(t, fenceConfig(t))
	c := h.dial()
	ids := addGenerationPaths(t, c, 20, 2)
	require.NoError(t, h.s.mgmtOperation(func() error {
		gp := h.s.generations.byUUID[ids[0]]
		h.s.propagateUpdate(nil, []*table.Path{h.s.localPathAt(gp.path, gp.key.id).Clone(true)})
		return nil
	}, false))
	fence(t, c, "t", 20)
	o := outcomesByUUID(t, deleteMany(t, c, "t", 20).Results[0])
	require.Equal(t, api.GenerationDeleteOutcome_GENERATION_DELETE_OUTCOME_ALREADY_ABSENT, o[ids[0]])
	require.Equal(t, api.GenerationDeleteOutcome_GENERATION_DELETE_OUTCOME_DELETED, o[ids[1]])
	require.Empty(t, globalPrefixes(t, h.s))
}

// The speaker stops in the middle of a batch. What was deleted stays
// deleted, the started speaker holds none of the paths, and the batch is
// repeated on it: every generation is fenced, owns nothing, and refuses
// commands.
func TestDeleteGenerationsRestartMidBatch(t *testing.T) {
	withStepPaths(t, 5)
	cfg := fenceConfig(t)
	h1 := startFenceServer(t, cfg)
	c1 := h1.dial()
	gens := make([]uint64, 10)
	for i := range gens {
		gens[i] = uint64(30 + i)
		addGenerationPaths(t, c1, gens[i], 3)
	}
	fenceMany(t, c1, "t", gens...)
	withStepHook(t, func(step int) {
		if step == 2 {
			h1.stop()
		}
	})
	_, err := h1.s.DeleteGenerations(context.Background(), &api.DeleteGenerationsRequest{Namespace: "t", Generations: gens, InstanceId: fenceInstance})
	require.Error(t, err, "the speaker stopped halfway")
	deleteGenerationsStepHook = nil

	h2 := startFenceServer(t, cfg)
	c2 := h2.dial()
	st, err := c2.GetGenerations(context.Background(), &api.GetGenerationsRequest{Namespace: "t", Generations: gens})
	require.NoError(t, err)
	for _, s := range st.Statuses {
		require.Equal(t, api.GenerationState_GENERATION_STATE_FENCED, s.State)
		require.Empty(t, s.Paths)
	}
	r := deleteMany(t, c2, "t", gens...)
	require.Len(t, r.Results, len(gens))
	for _, res := range r.Results {
		require.False(t, res.Refused)
		require.Empty(t, res.Outcomes)
		require.Equal(t, api.GenerationState_GENERATION_STATE_FENCED, res.Status.State)
	}
	for _, d := range lookup(t, h2, generationKeys(t, gens, 3)...).Destinations {
		require.Empty(t, d.Paths)
	}
	_, err = c2.AddPath(context.Background(), &api.AddPathRequest{TableType: api.TableType_TABLE_TYPE_GLOBAL, Path: ucPath(t, genPrefix(30, 0)), GenerationTag: fenceTag("t", 30, "late")})
	requireCode(t, err, codes.FailedPrecondition)
}

// Validation refuses the whole call and changes nothing; a generation that
// is not fenced is refused alone.
func TestDeleteGenerationsChecks(t *testing.T) {
	cfg := fenceConfig(t)
	cfg.InstanceID = fenceInstance
	h := startFenceServer(t, cfg)
	c := h.dial()
	ctx := context.Background()
	fencedIDs := addGenerationPaths(t, c, 1, 2)
	liveIDs := addGenerationPaths(t, c, 2, 2)
	fence(t, c, "t", 1)
	untouched := func() {
		t.Helper()
		st := getGeneration(t, c, "t", 1)
		require.EqualValues(t, 2, st.CurrentPaths)
	}

	// 1,031 entries, 1,024 distinct: the limit counts entries as sent.
	big := make([]uint64, 0, 1031)
	for g := range uint64(1024) {
		big = append(big, g+1)
	}
	big = append(big, 1, 2, 3, 4, 5, 6, 7)
	require.Len(t, big, 1031)
	_, err := c.DeleteGenerations(ctx, &api.DeleteGenerationsRequest{Namespace: "t", Generations: big, InstanceId: fenceInstance})
	requireCode(t, err, codes.InvalidArgument)
	require.ErrorContains(t, err, "at most 1024")
	untouched()
	for _, gens := range [][]uint64{nil, {1, 0}} {
		_, err = c.DeleteGenerations(ctx, &api.DeleteGenerationsRequest{Namespace: "t", Generations: gens, InstanceId: fenceInstance})
		requireCode(t, err, codes.InvalidArgument)
		untouched()
	}
	_, err = c.DeleteGenerations(ctx, &api.DeleteGenerationsRequest{Namespace: "t", Generations: []uint64{1}, InstanceId: "instance-2"})
	requireCode(t, err, codes.FailedPrecondition)
	untouched()
	_, err = c.DeleteGenerations(ctx, &api.DeleteGenerationsRequest{Namespace: strings.Repeat("n", 257), Generations: []uint64{1}, InstanceId: fenceInstance})
	requireCode(t, err, codes.InvalidArgument)
	untouched()

	// 1,024 entries with repeats: 1,000 distinct results, in the order of
	// first mention; generation 2 is not fenced and is refused alone.
	batch := make([]uint64, 0, 1024)
	for g := range uint64(1000) {
		batch = append(batch, 1000-g)
	}
	for len(batch) < 1024 {
		batch = append(batch, 1, 2)
	}
	r, err := c.DeleteGenerations(ctx, &api.DeleteGenerationsRequest{Namespace: "t", Generations: batch, InstanceId: fenceInstance})
	require.NoError(t, err)
	require.Len(t, r.Results, 1000)
	require.EqualValues(t, 1000, r.Results[0].Generation)
	byGen := map[uint64]*api.GenerationDeleteResult{}
	for _, res := range r.Results {
		byGen[res.Generation] = res
		require.Equal(t, fenceInstance, res.Status.InstanceId)
	}
	o1 := outcomesByUUID(t, byGen[1])
	require.False(t, byGen[1].Refused)
	for _, id := range fencedIDs {
		require.Equal(t, api.GenerationDeleteOutcome_GENERATION_DELETE_OUTCOME_DELETED, o1[id])
	}
	require.True(t, byGen[2].Refused)
	o2 := outcomesByUUID(t, byGen[2])
	for _, id := range liveIDs {
		require.Equal(t, api.GenerationDeleteOutcome_GENERATION_DELETE_OUTCOME_REFUSED, o2[id])
	}
	require.EqualValues(t, 2, byGen[2].Status.CurrentPaths)
	require.True(t, byGen[3].Refused, "a generation never written and never fenced is refused")
	require.Empty(t, byGen[3].Outcomes)

	off := startFenceServer(t, nil)
	_, err = off.dial().DeleteGenerations(ctx, &api.DeleteGenerationsRequest{Namespace: "t", Generations: []uint64{1}})
	requireCode(t, err, codes.FailedPrecondition)
}

// GetGenerations returns, for each generation, what GetGeneration returns.
func TestGetGenerations(t *testing.T) {
	h := startFenceServer(t, fenceConfig(t))
	c := h.dial()
	addGenerationPaths(t, c, 1, 2)
	addGenerationPaths(t, c, 2, 1)
	fence(t, c, "t", 2)
	r, err := c.GetGenerations(context.Background(), &api.GetGenerationsRequest{Namespace: "t", Generations: []uint64{2, 1, 3, 2}})
	require.NoError(t, err)
	require.Len(t, r.Statuses, 3)
	for _, st := range r.Statuses {
		require.True(t, proto.Equal(getGeneration(t, c, "t", st.Generation), st), "generation %d", st.Generation)
	}
	_, err = c.GetGenerations(context.Background(), &api.GetGenerationsRequest{Namespace: "t", Generations: make([]uint64, MaxFenceGenerations+1)})
	requireCode(t, err, codes.InvalidArgument)
	_, err = c.GetGenerations(context.Background(), &api.GetGenerationsRequest{Namespace: "t"})
	requireCode(t, err, codes.InvalidArgument)
}

// At scale: 1,024 fenced generations of 2 paths each, deleted
// in one call over a table of 200k untagged paths, all absent after it.
func TestDeleteGenerationsAtScale(t *testing.T) {
	if testing.Short() {
		t.Skip("large table")
	}
	h := startFenceServer(t, fenceConfig(t))
	fillTable(t, h.s, largeTablePaths(200000))
	gens := make([]uint64, MaxFenceGenerations)
	for i := range gens {
		gens[i] = uint64(i + 1)
		for j := range 2 {
			_, err := h.s.AddPath(apiutil.AddPathRequest{
				Paths:      []*apiutil.Path{mustApi2apiutilPath(ucPath(t, genPrefix(gens[i], j)))},
				Generation: &apiutil.GenerationTag{Namespace: "t", Generation: gens[i], CommandID: fmt.Sprint(j), InstanceID: fenceInstance},
			})
			require.NoError(t, err)
		}
	}
	ctx := context.Background()
	start := time.Now()
	_, err := h.s.FenceGenerations(ctx, &api.FenceGenerationsRequest{Namespace: "t", Generations: gens, InstanceId: fenceInstance})
	require.NoError(t, err)
	r, err := h.s.DeleteGenerations(ctx, &api.DeleteGenerationsRequest{Namespace: "t", Generations: gens, InstanceId: fenceInstance})
	require.NoError(t, err)
	st, err := h.s.GetGenerations(ctx, &api.GetGenerationsRequest{Namespace: "t", Generations: gens})
	require.NoError(t, err)
	elapsed := time.Since(start)
	t.Logf("fence + delete + status read of %d generations (%d paths) in %d steps: %v", len(gens), 2*len(gens), r.Steps, elapsed)
	for _, s := range st.Statuses {
		require.Zero(t, s.CurrentPaths)
	}
	for _, d := range lookup(t, h, generationKeys(t, gens, 2)...).Destinations {
		require.Empty(t, d.Paths)
	}
	for _, res := range r.Results {
		require.Zero(t, res.Status.CurrentPaths)
		require.Len(t, res.Outcomes, 2)
	}
	require.Less(t, elapsed, 2*time.Second)
}

func fenceMany(t *testing.T, c api.GoBgpServiceClient, namespace string, generations ...uint64) *api.FenceGenerationsResponse {
	t.Helper()
	r, err := c.FenceGenerations(context.Background(), &api.FenceGenerationsRequest{Namespace: namespace, Generations: generations, InstanceId: fenceInstance})
	require.NoError(t, err)
	return r
}

// A batch fence races commands of all its generations queued behind a busy
// loop on another connection. For each generation, the commands that
// succeed are exactly the ones its status in the answer lists, the others
// are refused, the RIB holds only the listed paths, and nothing lands after.
func TestFenceGenerationsAckOrdersQueuedCommands(t *testing.T) {
	h := startFenceServer(t, fenceConfig(t))
	a, b := h.dial(), h.dial()
	ctx := context.Background()
	const generations, commands = 4, 8
	for round := range 6 {
		base := uint64(100 + round*generations)
		type cmd struct {
			gen    uint64
			prefix string
			path   *api.Path
			id     uuid.UUID
			err    error
		}
		var cmds []*cmd
		for g := range uint64(generations) {
			for i := range commands {
				p := fmt.Sprintf("10.%d.%d.0/24", 120+round, int(g)*commands+i)
				cmds = append(cmds, &cmd{gen: base + g, prefix: p, path: ucPath(t, p)})
			}
		}
		gens := make([]uint64, generations)
		for g := range gens {
			gens[g] = base + uint64(g)
		}
		release := blockLoop(t, h.s)
		var wg sync.WaitGroup
		var fenced *api.FenceGenerationsResponse
		var fenceErr error
		for i, c := range cmds {
			wg.Add(1)
			go func() {
				defer wg.Done()
				r, err := a.AddPath(ctx, &api.AddPathRequest{TableType: api.TableType_TABLE_TYPE_GLOBAL, Path: c.path, GenerationTag: fenceTag("t", c.gen, c.prefix)})
				if err != nil {
					c.err = err
					return
				}
				c.id, _ = uuid.FromBytes(r.Uuid)
			}()
			if i == len(cmds)/2 {
				wg.Add(1)
				go func() {
					defer wg.Done()
					fenced, fenceErr = b.FenceGenerations(ctx, &api.FenceGenerationsRequest{Namespace: "t", Generations: gens, InstanceId: fenceInstance})
				}()
			}
		}
		time.Sleep(200 * time.Millisecond)
		release()
		wg.Wait()
		require.NoError(t, fenceErr)
		require.Len(t, fenced.Results, generations)

		var present []string
		for gi, res := range fenced.Results {
			require.Equal(t, gens[gi], res.Status.Generation)
			require.Equal(t, api.GenerationState_GENERATION_STATE_FENCED, res.Status.State)
			var applied []uuid.UUID
			for _, c := range cmds {
				if c.gen != gens[gi] {
					continue
				}
				if c.err != nil {
					requireCode(t, c.err, codes.FailedPrecondition)
					continue
				}
				applied = append(applied, c.id)
				present = append(present, c.prefix)
			}
			slices.SortFunc(applied, func(a, b uuid.UUID) int { return slices.Compare(a[:], b[:]) })
			require.Equal(t, applied, pathUUIDs(t, res.Status, api.GenerationPathState_GENERATION_PATH_STATE_CURRENT), "round %d generation %d", round, gens[gi])
			require.EqualValues(t, len(applied), res.Status.CurrentPaths)
		}
		var inRound []string
		for _, p := range globalPrefixes(t, h.s) {
			var x, y int
			if _, err := fmt.Sscanf(p, "10.%d.%d.0/24", &x, &y); err == nil && x == 120+round {
				inRound = append(inRound, p)
			}
		}
		require.ElementsMatch(t, present, inRound, "round %d", round)
		for _, g := range gens {
			_, err := a.AddPath(ctx, &api.AddPathRequest{TableType: api.TableType_TABLE_TYPE_GLOBAL, Path: ucPath(t, fmt.Sprintf("10.%d.250.0/24", 120+round)), GenerationTag: fenceTag("t", g, "late")})
			requireCode(t, err, codes.FailedPrecondition)
		}
	}
}

// Every tombstone of a batch survives a restart, beyond the cache size.
func TestFenceGenerationsSurviveRestart(t *testing.T) {
	cfg := &GenerationFenceConfig{Dir: t.TempDir(), TombstoneCacheSize: 2}
	h1 := startFenceServer(t, cfg)
	gens := make([]uint64, 50)
	for i := range gens {
		gens[i] = uint64(1000 + i)
	}
	r := fenceMany(t, h1.dial(), "t", gens...)
	require.Len(t, r.Results, len(gens))
	h1.stop()

	h2 := startFenceServer(t, cfg)
	c := h2.dial()
	for i, g := range gens {
		_, err := c.AddPath(context.Background(), &api.AddPathRequest{TableType: api.TableType_TABLE_TYPE_GLOBAL, Path: ucPath(t, fmt.Sprintf("10.17.%d.0/24", i)), GenerationTag: fenceTag("t", g, "late")})
		requireCode(t, err, codes.FailedPrecondition)
		st := getGeneration(t, c, "t", g)
		require.Equal(t, api.GenerationState_GENERATION_STATE_FENCED, st.State)
		require.True(t, st.FencedAt.AsTime().Equal(r.Results[i].Status.FencedAt.AsTime()))
	}
	require.Empty(t, globalPrefixes(t, h2.s))
}

// A batch may repeat a generation and may name generations already fenced:
// each distinct generation is reported once, and an earlier fence keeps its
// time.
func TestFenceGenerationsPartialDuplicates(t *testing.T) {
	h := startFenceServer(t, fenceConfig(t))
	c := h.dial()
	before := fence(t, c, "t", 1)
	r := fenceMany(t, c, "t", 2, 1, 2, 3, 1)
	require.Len(t, r.Results, 3)
	var got []uint64
	var newly []bool
	for _, res := range r.Results {
		got = append(got, res.Status.Generation)
		newly = append(newly, res.NewlyFenced)
	}
	require.Equal(t, []uint64{2, 1, 3}, got, "in the order of first mention")
	require.Equal(t, []bool{true, false, true}, newly)
	require.True(t, r.Results[1].Status.FencedAt.AsTime().Equal(before.Status.FencedAt.AsTime()))
	again := fenceMany(t, c, "t", 1, 2, 3)
	for _, res := range again.Results {
		require.False(t, res.NewlyFenced)
	}
}

// For each generation, the status a batch fence returns is the one a single
// fence returns.
func TestFenceGenerationsMatchesSingleFence(t *testing.T) {
	h := startFenceServer(t, fenceConfig(t))
	c := h.dial()
	ctx := context.Background()
	for g := uint64(1); g <= 3; g++ {
		for i := range 3 {
			_, err := c.AddPath(ctx, &api.AddPathRequest{TableType: api.TableType_TABLE_TYPE_GLOBAL, Path: ucPath(t, fmt.Sprintf("10.18.%d.0/24", int(g)*10+i)), GenerationTag: fenceTag("t", g, strconv.Itoa(i))})
			require.NoError(t, err)
		}
	}
	// One path of generation 2 deleted, one of generation 3 replaced by
	// generation 1, and a refused command, so every field has something.
	st := getGeneration(t, c, "t", 2)
	_, err := c.DeletePath(ctx, &api.DeletePathRequest{TableType: api.TableType_TABLE_TYPE_GLOBAL, Uuid: st.Paths[0].Uuid, GenerationTag: fenceTag("t", 2, "d")})
	require.NoError(t, err)
	_, err = c.AddPath(ctx, &api.AddPathRequest{TableType: api.TableType_TABLE_TYPE_GLOBAL, Path: ucPath(t, "10.18.30.0/24"), GenerationTag: fenceTag("t", 1, "take")})
	require.NoError(t, err)

	batch := fenceMany(t, c, "t", 1, 2, 3)
	_, err = c.AddPath(ctx, &api.AddPathRequest{TableType: api.TableType_TABLE_TYPE_GLOBAL, Path: ucPath(t, "10.18.99.0/24"), GenerationTag: fenceTag("t", 2, "late")})
	requireCode(t, err, codes.FailedPrecondition)
	for _, res := range batch.Results {
		require.True(t, res.NewlyFenced)
		single := fence(t, c, "t", res.Status.Generation)
		require.False(t, single.NewlyFenced)
		want := single.Status
		if res.Status.Generation == 2 {
			require.EqualValues(t, 1, want.RefusedAfterFence)
			want = proto.Clone(want).(*api.GenerationStatus)
			want.RefusedAfterFence = 0
		}
		require.True(t, proto.Equal(want, res.Status), "generation %d:\nbatch  %v\nsingle %v", res.Status.Generation, res.Status, single.Status)
	}
	require.Len(t, pathUUIDs(t, batch.Results[0].Status, api.GenerationPathState_GENERATION_PATH_STATE_CURRENT), 4)
	require.Len(t, pathUUIDs(t, batch.Results[1].Status, api.GenerationPathState_GENERATION_PATH_STATE_DELETED), 1)
	require.Len(t, pathUUIDs(t, batch.Results[2].Status, api.GenerationPathState_GENERATION_PATH_STATE_REPLACED), 1)
}

// The speaker is checked as for a single fence, the answer reports the
// configured speaker ID, and a refused or malformed batch fences nothing.
func TestFenceGenerationsChecks(t *testing.T) {
	cfg := fenceConfig(t)
	cfg.InstanceID = fenceInstance
	h := startFenceServer(t, cfg)
	c := h.dial()
	ctx := context.Background()
	_, err := c.FenceGenerations(ctx, &api.FenceGenerationsRequest{Namespace: "t", Generations: []uint64{1, 2}, InstanceId: "instance-2"})
	requireCode(t, err, codes.FailedPrecondition)
	_, err = c.FenceGenerations(ctx, &api.FenceGenerationsRequest{Namespace: "t", Generations: []uint64{1, 0}, InstanceId: fenceInstance})
	requireCode(t, err, codes.InvalidArgument)
	_, err = c.FenceGenerations(ctx, &api.FenceGenerationsRequest{Namespace: "t", InstanceId: fenceInstance})
	requireCode(t, err, codes.InvalidArgument)
	big := make([]uint64, MaxFenceGenerations+1)
	for i := range big {
		big[i] = uint64(i + 1)
	}
	_, err = c.FenceGenerations(ctx, &api.FenceGenerationsRequest{Namespace: "t", Generations: big, InstanceId: fenceInstance})
	requireCode(t, err, codes.InvalidArgument)
	require.ErrorContains(t, err, "at most 1024")
	for _, g := range []uint64{1, 2, 1025} {
		require.Equal(t, api.GenerationState_GENERATION_STATE_UNKNOWN, getGeneration(t, c, "t", g).State, "generation %d", g)
	}

	r, err := c.FenceGenerations(ctx, &api.FenceGenerationsRequest{Namespace: "t", Generations: big[:MaxFenceGenerations], InstanceId: fenceInstance})
	require.NoError(t, err)
	require.Len(t, r.Results, MaxFenceGenerations)
	for _, res := range r.Results {
		require.Equal(t, fenceInstance, res.Status.InstanceId)
		require.Len(t, res.Status.Incarnation, 16)
	}

	off := startFenceServer(t, nil)
	_, err = off.dial().FenceGenerations(ctx, &api.FenceGenerationsRequest{Namespace: "t", Generations: []uint64{1}})
	requireCode(t, err, codes.FailedPrecondition)
}
