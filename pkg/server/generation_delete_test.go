package server

import (
	"context"
	"fmt"
	"slices"
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

func deleteMany(t *testing.T, c api.GoBgpServiceClient, tenant string, generations ...uint64) *api.DeleteGenerationsResponse {
	t.Helper()
	r, err := c.DeleteGenerations(context.Background(), &api.DeleteGenerationsRequest{Tenant: tenant, Generations: generations, Speaker: fenceSpeaker})
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

func generationKeys(t *testing.T, gens []uint64, n int) []*api.DestinationKey {
	var keys []*api.DestinationKey
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
					fenced, fenceErr = b.FenceGenerations(ctx, &api.FenceGenerationsRequest{Tenant: "t", Generations: gens, Speaker: fenceSpeaker})
				}()
			case 2 * len(cmds) / 3:
				wg.Add(1)
				go func() {
					defer wg.Done()
					deleted, deleteErr = c.DeleteGenerations(ctx, &api.DeleteGenerationsRequest{Tenant: "t", Generations: gens, Speaker: fenceSpeaker})
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
			for _, d := range lookup(t, a, generationKeys(t, []uint64{g}, commands)...).Destinations {
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
		for _, d := range lookup(t, a, generationKeys(t, gens, commands)...).Destinations {
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

	l := lookup(t, c, destinationKey(t, bgp.RF_IPv4_UC, genPrefix(10, 0)), destinationKey(t, bgp.RF_IPv4_UC, genPrefix(10, 1)),
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
	require.Len(t, lookup(t, c, destinationKey(t, bgp.RF_IPv4_UC, genPrefix(10, 0))).Destinations[0].Paths, 1)
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
	_, err := h1.s.DeleteGenerations(context.Background(), &api.DeleteGenerationsRequest{Tenant: "t", Generations: gens, Speaker: fenceSpeaker})
	require.Error(t, err, "the speaker stopped halfway")
	deleteGenerationsStepHook = nil

	h2 := startFenceServer(t, cfg)
	c2 := h2.dial()
	st, err := c2.GetGenerations(context.Background(), &api.GetGenerationsRequest{Tenant: "t", Generations: gens})
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
	for _, d := range lookup(t, c2, generationKeys(t, gens, 3)...).Destinations {
		require.Empty(t, d.Paths)
	}
	_, err = c2.AddPath(context.Background(), &api.AddPathRequest{TableType: api.TableType_TABLE_TYPE_GLOBAL, Path: ucPath(t, genPrefix(30, 0)), GenerationTag: fenceTag("t", 30, "late")})
	requireCode(t, err, codes.FailedPrecondition)
}

// Validation refuses the whole call and changes nothing; a generation that
// is not fenced is refused alone.
func TestDeleteGenerationsChecks(t *testing.T) {
	cfg := fenceConfig(t)
	cfg.SpeakerID = fenceSpeaker
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
	_, err := c.DeleteGenerations(ctx, &api.DeleteGenerationsRequest{Tenant: "t", Generations: big, Speaker: fenceSpeaker})
	requireCode(t, err, codes.InvalidArgument)
	require.ErrorContains(t, err, "at most 1024")
	untouched()
	for _, gens := range [][]uint64{nil, {1, 0}} {
		_, err = c.DeleteGenerations(ctx, &api.DeleteGenerationsRequest{Tenant: "t", Generations: gens, Speaker: fenceSpeaker})
		requireCode(t, err, codes.InvalidArgument)
		untouched()
	}
	_, err = c.DeleteGenerations(ctx, &api.DeleteGenerationsRequest{Tenant: "t", Generations: []uint64{1}, Speaker: "speaker-2"})
	requireCode(t, err, codes.FailedPrecondition)
	untouched()
	_, err = c.DeleteGenerations(ctx, &api.DeleteGenerationsRequest{Generations: []uint64{1}, Speaker: fenceSpeaker})
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
	r, err := c.DeleteGenerations(ctx, &api.DeleteGenerationsRequest{Tenant: "t", Generations: batch, Speaker: fenceSpeaker})
	require.NoError(t, err)
	require.Len(t, r.Results, 1000)
	require.EqualValues(t, 1000, r.Results[0].Generation)
	byGen := map[uint64]*api.GenerationDeleteResult{}
	for _, res := range r.Results {
		byGen[res.Generation] = res
		require.Equal(t, fenceSpeaker, res.Status.SpeakerId)
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
	_, err = off.dial().DeleteGenerations(ctx, &api.DeleteGenerationsRequest{Tenant: "t", Generations: []uint64{1}})
	requireCode(t, err, codes.FailedPrecondition)
}

// GetGenerations returns, for each generation, what GetGeneration returns.
func TestGetGenerations(t *testing.T) {
	h := startFenceServer(t, fenceConfig(t))
	c := h.dial()
	addGenerationPaths(t, c, 1, 2)
	addGenerationPaths(t, c, 2, 1)
	fence(t, c, "t", 2)
	r, err := c.GetGenerations(context.Background(), &api.GetGenerationsRequest{Tenant: "t", Generations: []uint64{2, 1, 3, 2}})
	require.NoError(t, err)
	require.Len(t, r.Statuses, 3)
	for _, st := range r.Statuses {
		require.True(t, proto.Equal(getGeneration(t, c, "t", st.Generation), st), "generation %d", st.Generation)
	}
	_, err = c.GetGenerations(context.Background(), &api.GetGenerationsRequest{Tenant: "t", Generations: make([]uint64, MaxFenceGenerations+1)})
	requireCode(t, err, codes.InvalidArgument)
	_, err = c.GetGenerations(context.Background(), &api.GetGenerationsRequest{Tenant: "t"})
	requireCode(t, err, codes.InvalidArgument)
}

// A kill switch at scale: 1,024 fenced generations of 2 paths each, deleted
// in one call over a table of 200k untagged paths, all absent after it.
func TestDeleteGenerationsKillSwitchScale(t *testing.T) {
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
				Generation: &apiutil.GenerationTag{Tenant: "t", Generation: gens[i], CommandID: fmt.Sprint(j), Speaker: fenceSpeaker},
			})
			require.NoError(t, err)
		}
	}
	ctx := context.Background()
	start := time.Now()
	_, err := h.s.FenceGenerations(ctx, &api.FenceGenerationsRequest{Tenant: "t", Generations: gens, Speaker: fenceSpeaker})
	require.NoError(t, err)
	r, err := h.s.DeleteGenerations(ctx, &api.DeleteGenerationsRequest{Tenant: "t", Generations: gens, Speaker: fenceSpeaker})
	require.NoError(t, err)
	keys := generationKeys(t, gens, 2)
	var absent int
	for i := 0; i < len(keys); i += MaxLookupDestinations {
		l, err := h.s.LookupDestinations(ctx, &api.LookupDestinationsRequest{TableType: api.TableType_TABLE_TYPE_GLOBAL, Keys: keys[i:min(i+MaxLookupDestinations, len(keys))]})
		require.NoError(t, err)
		for _, d := range l.Destinations {
			if len(d.Paths) == 0 {
				absent++
			}
		}
	}
	elapsed := time.Since(start)
	t.Logf("fence + delete + lookup of %d generations (%d paths) in %d steps: %v", len(gens), 2*len(gens), r.Steps, elapsed)
	require.Equal(t, len(keys), absent)
	for _, res := range r.Results {
		require.Zero(t, res.Status.CurrentPaths)
		require.Len(t, res.Outcomes, 2)
	}
	require.Less(t, elapsed, 2*time.Second)
}
