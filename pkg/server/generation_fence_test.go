package server

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/netip"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	"github.com/osrg/gobgp/v4/api"
	"github.com/osrg/gobgp/v4/internal/pkg/genfence"
	"github.com/osrg/gobgp/v4/internal/pkg/table"
	"github.com/osrg/gobgp/v4/pkg/apiutil"
	"github.com/osrg/gobgp/v4/pkg/packet/bgp"
)

const fenceInstance = "instance-1"

// fenceHarness is a server with its gRPC API on an in-memory listener, so a
// test can open as many client connections as it wants.
type fenceHarness struct {
	t       *testing.T
	s       *BgpServer
	gs      *grpc.Server
	lis     *bufconn.Listener
	stopped bool
}

func startFenceServer(t *testing.T, cfg *GenerationFenceConfig, interceptors ...grpc.UnaryServerInterceptor) *fenceHarness {
	t.Helper()
	var opts []ServerOption
	if cfg != nil {
		opts = append(opts, GenerationFenceOption(*cfg))
	}
	s := NewBgpServer(opts...)
	go s.Serve()
	require.NoError(t, s.StartBgp(context.Background(), &api.StartBgpRequest{
		Global: &api.Global{Asn: 1, RouterId: "1.1.1.1", ListenPort: -1},
	}))
	gs := grpc.NewServer(grpc.ChainUnaryInterceptor(interceptors...))
	api.RegisterGoBgpServiceServer(gs, &server{bgpServer: s, shared: s.shared})
	lis := bufconn.Listen(1 << 20)
	go func() {
		//nolint: errcheck // ends when the test stops the server
		gs.Serve(lis)
	}()
	h := &fenceHarness{t: t, s: s, gs: gs, lis: lis}
	t.Cleanup(h.stop)
	return h
}

func (h *fenceHarness) stop() {
	if h.stopped {
		return
	}
	h.stopped = true
	h.gs.Stop()
	//nolint: errcheck // ignore error
	h.s.StopBgp(context.Background(), &api.StopBgpRequest{})
	h.s.bfdServer.Stop()
}

// dial opens a new client connection.
func (h *fenceHarness) dial() api.GoBgpServiceClient {
	h.t.Helper()
	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return h.lis.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(h.t, err)
	h.t.Cleanup(func() { conn.Close() })
	return api.NewGoBgpServiceClient(conn)
}

func fenceConfig(t *testing.T) *GenerationFenceConfig {
	return &GenerationFenceConfig{Dir: t.TempDir()}
}

func fenceTag(namespace string, generation uint64, command string) *api.GenerationTag {
	return &api.GenerationTag{Namespace: namespace, Generation: generation, CommandId: command, InstanceId: fenceInstance}
}

func fenceTestPath(t testing.TB, family bgp.Family, prefix, nexthop string) *api.Path {
	t.Helper()
	nh, err := bgp.NewPathAttributeNextHop(netip.MustParseAddr(nexthop))
	require.NoError(t, err)
	var nlri bgp.NLRI
	switch family {
	case bgp.RF_IPv4_UC:
		nlri, err = bgp.NewIPAddrPrefix(netip.MustParsePrefix(prefix))
	case bgp.RF_IPv4_MPLS:
		nlri, err = bgp.NewLabeledIPAddrPrefix(netip.MustParsePrefix(prefix), *bgp.NewMPLSLabelStack(100))
	default:
		t.Fatalf("unsupported family %s", family)
	}
	require.NoError(t, err)
	path, err := apiutil.NewPath(family, nlri, false, []bgp.PathAttributeInterface{bgp.NewPathAttributeOrigin(0), nh}, time.Now())
	require.NoError(t, err)
	return path
}

func ucPath(t testing.TB, prefix string) *api.Path {
	return fenceTestPath(t, bgp.RF_IPv4_UC, prefix, "10.0.0.1")
}

func apiFamily(f bgp.Family) *api.Family {
	return &api.Family{Afi: api.Family_Afi(f.Afi()), Safi: api.Family_Safi(f.Safi())}
}

// testKey names a destination of the global table.
type testKey struct {
	family bgp.Family
	prefix string
}

func destinationKey(_ testing.TB, family bgp.Family, prefix string) testKey {
	return testKey{family, prefix}
}

func requireCode(t *testing.T, err error, code codes.Code) {
	t.Helper()
	require.Error(t, err)
	require.Equal(t, code, status.Code(err), "error %v", err)
}

func mustUUID(t *testing.T, b []byte) uuid.UUID {
	t.Helper()
	id, err := uuid.FromBytes(b)
	require.NoError(t, err)
	return id
}

func pathUUIDs(t *testing.T, st *api.GenerationStatus, state api.GenerationPathState) []uuid.UUID {
	t.Helper()
	var ids []uuid.UUID
	for _, p := range st.Paths {
		if state == api.GenerationPathState_GENERATION_PATH_STATE_UNSPECIFIED || p.State == state {
			ids = append(ids, mustUUID(t, p.Uuid))
		}
	}
	slices.SortFunc(ids, func(a, b uuid.UUID) int { return slices.Compare(a[:], b[:]) })
	return ids
}

type lookedUpPath struct {
	Path  *api.Path
	Uuid  []byte
	Owner *api.GenerationTag
}

type lookedUpDestination struct {
	Paths []lookedUpPath
}

type lookupResult struct {
	Destinations []lookedUpDestination
}

// lookup reads the global table at each key, in one management operation,
// with the uuid and the generation the index names for each local path.
func lookup(t testing.TB, h *fenceHarness, keys ...testKey) *lookupResult {
	t.Helper()
	paths := make([]*table.Path, len(keys))
	for i, k := range keys {
		p, err := api2Path(api.TableType_TABLE_TYPE_GLOBAL, fenceTestPath(t, k.family, k.prefix, "10.0.0.1"))
		require.NoError(t, err)
		paths[i] = p
	}
	r := &lookupResult{Destinations: make([]lookedUpDestination, len(keys))}
	require.NoError(t, h.s.mgmtOperation(func() error {
		for i, p := range paths {
			dest := h.s.globalRib.GetDestination(p)
			if dest == nil {
				continue
			}
			for _, rp := range dest.GetAllKnownPathList() {
				lp := lookedUpPath{Path: toPathApi(toPathApiUtil(rp), false, false, false)}
				if gp, ok := h.s.generations.byRoute[routeKeyOf(rp)]; ok && rp.IsLocal() {
					lp.Uuid = uuidBytes(gp.id)
					lp.Owner = &api.GenerationTag{Namespace: gp.rec.key.namespace, Generation: gp.rec.key.generation, CommandId: gp.commandID, InstanceId: gp.instanceID}
				}
				r.Destinations[i].Paths = append(r.Destinations[i].Paths, lp)
			}
		}
		return nil
	}, false))
	return r
}

func getGeneration(t *testing.T, c api.GoBgpServiceClient, namespace string, generation uint64) *api.GenerationStatus {
	t.Helper()
	r, err := c.GetGeneration(context.Background(), &api.GetGenerationRequest{Namespace: namespace, Generation: generation})
	require.NoError(t, err)
	return r.Status
}

func fence(t *testing.T, c api.GoBgpServiceClient, namespace string, generation uint64) *api.FenceGenerationResponse {
	t.Helper()
	r, err := c.FenceGeneration(context.Background(), &api.FenceGenerationRequest{Namespace: namespace, Generation: generation, InstanceId: fenceInstance})
	require.NoError(t, err)
	return r
}

// A command for a fenced generation is refused whatever connection it comes
// on, and the paths it would have written never reach the RIB.
func TestFenceRefusesLaterCommandsFromAnyConnection(t *testing.T) {
	h := startFenceServer(t, fenceConfig(t))
	a, b := h.dial(), h.dial()
	ctx := context.Background()

	r, err := a.AddPath(ctx, &api.AddPathRequest{TableType: api.TableType_TABLE_TYPE_GLOBAL, Path: ucPath(t, "10.1.0.0/24"), GenerationTag: fenceTag("t", 1, "c1")})
	require.NoError(t, err)
	u1 := mustUUID(t, r.Uuid)
	require.False(t, r.Replayed)
	require.Len(t, r.Incarnation, 16)

	f := fence(t, b, "t", 1)
	require.True(t, f.NewlyFenced)
	require.Equal(t, api.GenerationState_GENERATION_STATE_FENCED, f.Status.State)
	require.Equal(t, []uuid.UUID{u1}, pathUUIDs(t, f.Status, api.GenerationPathState_GENERATION_PATH_STATE_CURRENT))
	require.True(t, f.Status.Paths[0].Present)
	require.Len(t, f.Status.Receipts, 1)
	require.Equal(t, "c1", f.Status.Receipts[0].CommandId)

	_, err = a.AddPath(ctx, &api.AddPathRequest{TableType: api.TableType_TABLE_TYPE_GLOBAL, Path: ucPath(t, "10.1.1.0/24"), GenerationTag: fenceTag("t", 1, "c2")})
	requireCode(t, err, codes.FailedPrecondition)
	// A retry of the command applied before the fence is refused too.
	_, err = a.AddPath(ctx, &api.AddPathRequest{TableType: api.TableType_TABLE_TYPE_GLOBAL, Path: ucPath(t, "10.1.0.0/24"), GenerationTag: fenceTag("t", 1, "c1")})
	requireCode(t, err, codes.FailedPrecondition)

	stream, err := a.AddPathStream(ctx)
	require.NoError(t, err)
	require.NoError(t, stream.Send(&api.AddPathStreamRequest{TableType: api.TableType_TABLE_TYPE_GLOBAL, Paths: []*api.Path{ucPath(t, "10.1.2.0/24")}, GenerationTag: fenceTag("t", 1, "c3")}))
	_, err = stream.CloseAndRecv()
	requireCode(t, err, codes.FailedPrecondition)

	again := fence(t, a, "t", 1)
	require.False(t, again.NewlyFenced)
	require.True(t, again.Status.FencedAt.AsTime().Equal(f.Status.FencedAt.AsTime()))
	require.EqualValues(t, 3, again.Status.RefusedAfterFence)

	// Other generations and untagged writes are not affected.
	_, err = b.AddPath(ctx, &api.AddPathRequest{TableType: api.TableType_TABLE_TYPE_GLOBAL, Path: ucPath(t, "10.1.3.0/24"), GenerationTag: fenceTag("t", 2, "c1")})
	require.NoError(t, err)
	_, err = a.AddPath(ctx, &api.AddPathRequest{TableType: api.TableType_TABLE_TYPE_GLOBAL, Path: ucPath(t, "10.1.4.0/24")})
	require.NoError(t, err)
	require.ElementsMatch(t, []string{"10.1.0.0/24", "10.1.3.0/24", "10.1.4.0/24"}, globalPrefixes(t, h.s))
}

// Commands queued behind a busy management loop, on one connection, race a
// fence on another. Whatever order the loop takes them in, the commands that
// succeed are exactly the ones the fence's answer lists, the others are
// refused, and the RIB holds only the listed paths.
func TestFenceAckOrdersQueuedCommands(t *testing.T) {
	h := startFenceServer(t, fenceConfig(t))
	a, b := h.dial(), h.dial()
	ctx := context.Background()
	const commands = 24
	for round := uint64(1); round <= 8; round++ {
		paths := make([]*api.Path, commands)
		for i := range paths {
			paths[i] = ucPath(t, fmt.Sprintf("10.%d.%d.0/24", 100+round, i))
		}
		release := blockLoop(t, h.s)
		var wg sync.WaitGroup
		ok := make([]uuid.UUID, commands)
		errs := make([]error, commands)
		var fenced *api.FenceGenerationResponse
		var fenceErr error
		for i := range commands {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				r, err := a.AddPath(ctx, &api.AddPathRequest{
					TableType:     api.TableType_TABLE_TYPE_GLOBAL,
					Path:          paths[i],
					GenerationTag: fenceTag("t", round, "c"+strconv.Itoa(i)),
				})
				if err != nil {
					errs[i] = err
					return
				}
				ok[i], _ = uuid.FromBytes(r.Uuid)
			}(i)
			if i == commands/2 {
				wg.Add(1)
				go func() {
					defer wg.Done()
					fenced, fenceErr = b.FenceGeneration(ctx, &api.FenceGenerationRequest{Namespace: "t", Generation: round, InstanceId: fenceInstance})
				}()
			}
		}
		// Let every request reach the queue, then let the loop run them.
		time.Sleep(200 * time.Millisecond)
		release()
		wg.Wait()
		require.NoError(t, fenceErr)

		var applied []uuid.UUID
		var prefixes []string
		for i := range commands {
			if errs[i] != nil {
				requireCode(t, errs[i], codes.FailedPrecondition)
				require.Contains(t, errs[i].Error(), ErrGenerationFenced.Error())
				continue
			}
			applied = append(applied, ok[i])
			prefixes = append(prefixes, fmt.Sprintf("10.%d.%d.0/24", 100+round, i))
		}
		slices.SortFunc(applied, func(a, b uuid.UUID) int { return slices.Compare(a[:], b[:]) })
		require.Equal(t, applied, pathUUIDs(t, fenced.Status, api.GenerationPathState_GENERATION_PATH_STATE_CURRENT),
			"round %d: the fence lists exactly the commands that succeeded", round)
		require.EqualValues(t, commands-len(applied), getGeneration(t, a, "t", round).RefusedAfterFence)

		var inRound []string
		for _, p := range globalPrefixes(t, h.s) {
			var x, y int
			if _, err := fmt.Sscanf(p, "10.%d.%d.0/24", &x, &y); err == nil && x == int(100+round) {
				inRound = append(inRound, p)
			}
		}
		require.ElementsMatch(t, prefixes, inRound, "round %d", round)

		// After the acknowledgement nothing more lands.
		_, err := a.AddPath(ctx, &api.AddPathRequest{TableType: api.TableType_TABLE_TYPE_GLOBAL, Path: ucPath(t, fmt.Sprintf("10.%d.250.0/24", 100+round)), GenerationTag: fenceTag("t", round, "late")})
		requireCode(t, err, codes.FailedPrecondition)
	}
}

// A command whose client deadline passes while it waits for the loop.
//
// The server's copy of the deadline starts when the request arrives, so it
// ends a little after the client's. A command the loop reaches in between
// is applied although its client saw DEADLINE_EXCEEDED. The fence settles
// it either way: what the fence lists is exactly what the RIB holds, and
// the command is refused after it. Once the server's deadline has passed
// too, the command is never applied.
func TestFenceAndExpiredQueuedCommand(t *testing.T) {
	for _, tc := range []struct {
		name       string
		wait       time.Duration
		neverAdded bool
	}{
		{"released at the client deadline", 0, false},
		{"released after the server deadline", 300 * time.Millisecond, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := startFenceServer(t, fenceConfig(t))
			a, b := h.dial(), h.dial()
			add := &api.AddPathRequest{TableType: api.TableType_TABLE_TYPE_GLOBAL, Path: ucPath(t, "10.3.0.0/24"), GenerationTag: fenceTag("t", 3, "c1")}
			release := blockLoop(t, h.s)
			ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
			defer cancel()
			_, err := a.AddPath(ctx, add)
			requireCode(t, err, codes.DeadlineExceeded)
			time.Sleep(tc.wait)
			release()
			f := fence(t, b, "t", 3)
			require.Equal(t, api.GenerationState_GENERATION_STATE_FENCED, f.Status.State)
			listed := len(pathUUIDs(t, f.Status, api.GenerationPathState_GENERATION_PATH_STATE_CURRENT))
			if tc.neverAdded {
				require.Zero(t, listed)
			}
			if listed == 0 {
				require.Empty(t, globalPrefixes(t, h.s))
			} else {
				require.Equal(t, 1, listed)
				require.Equal(t, []string{"10.3.0.0/24"}, globalPrefixes(t, h.s))
			}
			_, err = a.AddPath(context.Background(), add)
			requireCode(t, err, codes.FailedPrecondition)
		})
	}
}

// Tombstones are durable: a speaker started on the same directory refuses
// the fenced generations from its very first write, even when they are not
// in its cache, and keeps the original fence time.
func TestFenceSurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	cfg := &GenerationFenceConfig{Dir: dir, TombstoneCacheSize: 2}
	h1 := startFenceServer(t, cfg)
	c1 := h1.dial()
	ctx := context.Background()
	fencedAt := map[uint64]time.Time{}
	for g := uint64(1); g <= 6; g++ {
		_, err := c1.AddPath(ctx, &api.AddPathRequest{TableType: api.TableType_TABLE_TYPE_GLOBAL, Path: ucPath(t, fmt.Sprintf("10.4.%d.0/24", g)), GenerationTag: fenceTag("t", g, "c1")})
		require.NoError(t, err)
		if g <= 5 {
			fencedAt[g] = fence(t, c1, "t", g).Status.FencedAt.AsTime()
		}
	}
	inc1 := getGeneration(t, c1, "t", 6).Incarnation
	h1.stop()

	h2 := startFenceServer(t, cfg)
	c2 := h2.dial()
	// The first write after the start is already refused.
	_, err := c2.AddPath(ctx, &api.AddPathRequest{TableType: api.TableType_TABLE_TYPE_GLOBAL, Path: ucPath(t, "10.4.1.0/24"), GenerationTag: fenceTag("t", 1, "late")})
	requireCode(t, err, codes.FailedPrecondition)
	for g := uint64(2); g <= 5; g++ {
		_, err := c2.AddPath(ctx, &api.AddPathRequest{TableType: api.TableType_TABLE_TYPE_GLOBAL, Path: ucPath(t, fmt.Sprintf("10.4.%d.0/24", g)), GenerationTag: fenceTag("t", g, "late")})
		requireCode(t, err, codes.FailedPrecondition)
		st := getGeneration(t, c2, "t", g)
		require.Equal(t, api.GenerationState_GENERATION_STATE_FENCED, st.State)
		require.True(t, st.FencedAt.AsTime().Equal(fencedAt[g]), "generation %d keeps its fence time", g)
	}
	_, err = c2.AddPath(ctx, &api.AddPathRequest{TableType: api.TableType_TABLE_TYPE_GLOBAL, Path: ucPath(t, "10.4.6.0/24"), GenerationTag: fenceTag("t", 6, "c2")})
	require.NoError(t, err, "a generation that was never fenced still writes")
	st := getGeneration(t, c2, "t", 6)
	require.NotEqual(t, inc1, st.Incarnation, "a restart is a new incarnation")
	require.Len(t, st.Receipts, 1, "receipts belong to one incarnation")
	require.Equal(t, []string{"10.4.6.0/24"}, globalPrefixes(t, h2.s))

	// A second speaker cannot open the store while this one holds it.
	old := genfence.OpenTimeout
	genfence.OpenTimeout = 100 * time.Millisecond
	defer func() { genfence.OpenTimeout = old }()
	s3 := NewBgpServer(GenerationFenceOption(*cfg))
	t.Cleanup(s3.bfdServer.Stop)
	go s3.Serve()
	err = s3.StartBgp(ctx, &api.StartBgpRequest{Global: &api.Global{Asn: 1, RouterId: "1.1.1.1", ListenPort: -1}})
	require.Error(t, err)
	_, err = s3.AddPath(apiutil.AddPathRequest{Paths: []*apiutil.Path{mustApi2apiutilPath(ucPath(t, "10.4.7.0/24"))}})
	require.Error(t, err, "a speaker whose store did not open accepts no write")
	//nolint: errcheck // ignore error
	s3.StopBgp(ctx, &api.StopBgpRequest{})
}

// An AddPath whose answer is lost leaves a receipt: the writer finds the
// UUID with GetGeneration, a retry with the same command ID is not applied
// twice, and the path can be deleted.
func TestGenerationReceiptRecoversLostAddPathResponse(t *testing.T) {
	var drop atomic.Bool
	drop.Store(true)
	lose := func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		resp, err := handler(ctx, req)
		if err == nil && (info.FullMethod == "/api.GoBgpService/AddPath" || info.FullMethod == "/api.GoBgpService/DeletePath") && drop.CompareAndSwap(true, false) {
			return nil, status.Error(codes.Unavailable, "the answer was lost")
		}
		return resp, err
	}
	h := startFenceServer(t, fenceConfig(t), lose)
	a, b := h.dial(), h.dial()
	ctx := context.Background()
	add := &api.AddPathRequest{TableType: api.TableType_TABLE_TYPE_GLOBAL, Path: ucPath(t, "10.7.0.0/24"), GenerationTag: fenceTag("t", 7, "c1")}
	_, err := a.AddPath(ctx, add)
	requireCode(t, err, codes.Unavailable)

	st := getGeneration(t, b, "t", 7)
	require.Equal(t, api.GenerationState_GENERATION_STATE_LIVE, st.State)
	require.Len(t, st.Receipts, 1)
	require.Equal(t, "c1", st.Receipts[0].CommandId)
	require.Equal(t, fenceInstance, st.Receipts[0].InstanceId)
	require.Equal(t, api.GenerationCommandKind_GENERATION_COMMAND_KIND_ADD_PATH, st.Receipts[0].Kind)
	require.Len(t, st.Receipts[0].Uuids, 1)
	u := mustUUID(t, st.Receipts[0].Uuids[0])
	require.Len(t, st.Paths, 1)
	require.Equal(t, u, mustUUID(t, st.Paths[0].Uuid))
	require.True(t, st.Paths[0].Present)
	require.Equal(t, "10.7.0.0/24", st.Paths[0].Nlri.GetPrefix().GetPrefix()+"/"+strconv.Itoa(int(st.Paths[0].Nlri.GetPrefix().GetPrefixLen())))

	r, err := a.AddPath(ctx, add)
	require.NoError(t, err)
	require.True(t, r.Replayed)
	require.Equal(t, u, mustUUID(t, r.Uuid))
	require.Len(t, getGeneration(t, b, "t", 7).Paths, 1, "the retry was not applied again")

	conflict := &api.AddPathRequest{TableType: api.TableType_TABLE_TYPE_GLOBAL, Path: ucPath(t, "10.7.1.0/24"), GenerationTag: fenceTag("t", 7, "c1")}
	_, err = a.AddPath(ctx, conflict)
	requireCode(t, err, codes.AlreadyExists)

	f := fence(t, b, "t", 7)
	require.Equal(t, []uuid.UUID{u}, pathUUIDs(t, f.Status, api.GenerationPathState_GENERATION_PATH_STATE_CURRENT))

	// The delete's answer is lost as well; its retry reports the outcome.
	drop.Store(true)
	del := &api.DeletePathRequest{TableType: api.TableType_TABLE_TYPE_GLOBAL, Uuid: u[:], GenerationTag: fenceTag("t", 7, "d1")}
	_, err = b.DeletePath(ctx, del)
	requireCode(t, err, codes.Unavailable)
	d, err := b.DeletePath(ctx, del)
	require.NoError(t, err)
	require.True(t, d.Replayed)
	require.Equal(t, api.GenerationDeleteOutcome_GENERATION_DELETE_OUTCOME_DELETED, d.Outcome)
	d, err = b.DeletePath(ctx, &api.DeletePathRequest{TableType: api.TableType_TABLE_TYPE_GLOBAL, Uuid: u[:], GenerationTag: fenceTag("t", 7, "d2")})
	require.NoError(t, err)
	require.False(t, d.Replayed)
	require.Equal(t, api.GenerationDeleteOutcome_GENERATION_DELETE_OUTCOME_ALREADY_DELETED, d.Outcome)

	st = getGeneration(t, b, "t", 7)
	require.Equal(t, []uuid.UUID{u}, pathUUIDs(t, st, api.GenerationPathState_GENERATION_PATH_STATE_DELETED))
	require.False(t, st.Paths[0].Present)
	require.Zero(t, st.CurrentPaths)
	require.Empty(t, lookup(t, h, destinationKey(t, bgp.RF_IPv4_UC, "10.7.0.0/24")).Destinations[0].Paths)
	require.Empty(t, globalPrefixes(t, h.s))
}

// A path that another generation's AddPath replaced is not deleted by a
// delete of the first generation.
func TestGenerationReplacementByAnotherGenerationIsNotDeleted(t *testing.T) {
	h := startFenceServer(t, fenceConfig(t))
	c := h.dial()
	ctx := context.Background()
	r1, err := c.AddPath(ctx, &api.AddPathRequest{TableType: api.TableType_TABLE_TYPE_GLOBAL, Path: fenceTestPath(t, bgp.RF_IPv4_UC, "10.8.0.0/24", "10.0.0.1"), GenerationTag: fenceTag("t", 1, "a")})
	require.NoError(t, err)
	u1 := mustUUID(t, r1.Uuid)
	r2, err := c.AddPath(ctx, &api.AddPathRequest{TableType: api.TableType_TABLE_TYPE_GLOBAL, Path: fenceTestPath(t, bgp.RF_IPv4_UC, "10.8.0.0/24", "10.0.0.2"), GenerationTag: fenceTag("t", 2, "a")})
	require.NoError(t, err)
	u2 := mustUUID(t, r2.Uuid)

	f := fence(t, c, "t", 1)
	require.Empty(t, pathUUIDs(t, f.Status, api.GenerationPathState_GENERATION_PATH_STATE_CURRENT))
	d, err := c.DeletePath(ctx, &api.DeletePathRequest{TableType: api.TableType_TABLE_TYPE_GLOBAL, Uuid: u1[:], GenerationTag: fenceTag("t", 1, "d")})
	require.NoError(t, err)
	require.Equal(t, api.GenerationDeleteOutcome_GENERATION_DELETE_OUTCOME_REPLACED, d.Outcome)

	l := lookup(t, h, destinationKey(t, bgp.RF_IPv4_UC, "10.8.0.0/24")).Destinations[0]
	require.Len(t, l.Paths, 1)
	require.Equal(t, u2, mustUUID(t, l.Paths[0].Uuid))
	require.EqualValues(t, 2, l.Paths[0].Owner.Generation)
	nh, err := apiutil.GetNativePathAttributes(l.Paths[0].Path)
	require.NoError(t, err)
	require.Contains(t, fmt.Sprint(nh), "10.0.0.2")

	st1 := getGeneration(t, c, "t", 1)
	require.Len(t, st1.Paths, 1)
	require.Equal(t, api.GenerationPathState_GENERATION_PATH_STATE_REPLACED, st1.Paths[0].State)
	require.Equal(t, u2, mustUUID(t, st1.Paths[0].ReplacedByUuid))
	require.EqualValues(t, 2, st1.Paths[0].ReplacedBy.Generation)
	require.False(t, st1.Paths[0].Present)
	st2 := getGeneration(t, c, "t", 2)
	require.Equal(t, []uuid.UUID{u2}, pathUUIDs(t, st2, api.GenerationPathState_GENERATION_PATH_STATE_CURRENT))
	require.True(t, st2.Paths[0].Present)

	// Neither an untagged delete by the old UUID nor a delete under the
	// wrong generation touches it.
	_, err = c.DeletePath(ctx, &api.DeletePathRequest{TableType: api.TableType_TABLE_TYPE_GLOBAL, Uuid: u1[:]})
	require.ErrorContains(t, err, "can't find")
	_, err = c.DeletePath(ctx, &api.DeletePathRequest{TableType: api.TableType_TABLE_TYPE_GLOBAL, Uuid: u2[:], GenerationTag: fenceTag("t", 1, "d2")})
	requireCode(t, err, codes.FailedPrecondition)
	require.Equal(t, []string{"10.8.0.0/24"}, globalPrefixes(t, h.s))
}

// The route key has the family: a unicast and a labeled unicast path for
// the same prefix are two keys, and deleting one leaves the other.
func TestGenerationKeysIncludeFamily(t *testing.T) {
	h := startFenceServer(t, fenceConfig(t))
	c := h.dial()
	ctx := context.Background()
	r1, err := c.AddPath(ctx, &api.AddPathRequest{TableType: api.TableType_TABLE_TYPE_GLOBAL, Path: fenceTestPath(t, bgp.RF_IPv4_UC, "10.5.0.0/24", "10.0.0.1"), GenerationTag: fenceTag("t", 1, "uc")})
	require.NoError(t, err)
	r2, err := c.AddPath(ctx, &api.AddPathRequest{TableType: api.TableType_TABLE_TYPE_GLOBAL, Path: fenceTestPath(t, bgp.RF_IPv4_MPLS, "10.5.0.0/24", "10.0.0.1"), GenerationTag: fenceTag("t", 1, "lu")})
	require.NoError(t, err)
	st := getGeneration(t, c, "t", 1)
	require.Len(t, pathUUIDs(t, st, api.GenerationPathState_GENERATION_PATH_STATE_CURRENT), 2)

	d, err := c.DeletePath(ctx, &api.DeletePathRequest{TableType: api.TableType_TABLE_TYPE_GLOBAL, Uuid: r1.Uuid, GenerationTag: fenceTag("t", 1, "d")})
	require.NoError(t, err)
	require.Equal(t, api.GenerationDeleteOutcome_GENERATION_DELETE_OUTCOME_DELETED, d.Outcome)
	l := lookup(t, h, destinationKey(t, bgp.RF_IPv4_UC, "10.5.0.0/24"), destinationKey(t, bgp.RF_IPv4_MPLS, "10.5.0.0/24"))
	require.Empty(t, l.Destinations[0].Paths)
	require.Len(t, l.Destinations[1].Paths, 1)
	require.Equal(t, r2.Uuid, l.Destinations[1].Paths[0].Uuid)
}

// A legacy delete by UUID, which matches every family with the same prefix,
// does not take a path a tagged AddPath wrote.
func TestLegacyUUIDDeleteLeavesTaggedPaths(t *testing.T) {
	h := startFenceServer(t, fenceConfig(t))
	c := h.dial()
	ctx := context.Background()
	legacy, err := c.AddPath(ctx, &api.AddPathRequest{TableType: api.TableType_TABLE_TYPE_GLOBAL, Path: fenceTestPath(t, bgp.RF_IPv4_UC, "10.6.0.0/24", "10.0.0.1")})
	require.NoError(t, err)
	tagged, err := c.AddPath(ctx, &api.AddPathRequest{TableType: api.TableType_TABLE_TYPE_GLOBAL, Path: fenceTestPath(t, bgp.RF_IPv4_MPLS, "10.6.0.0/24", "10.0.0.1"), GenerationTag: fenceTag("t", 1, "lu")})
	require.NoError(t, err)
	_, err = c.DeletePath(ctx, &api.DeletePathRequest{TableType: api.TableType_TABLE_TYPE_GLOBAL, Uuid: legacy.Uuid})
	require.NoError(t, err)
	l := lookup(t, h, destinationKey(t, bgp.RF_IPv4_UC, "10.6.0.0/24"), destinationKey(t, bgp.RF_IPv4_MPLS, "10.6.0.0/24"))
	require.Empty(t, l.Destinations[0].Paths)
	require.Len(t, l.Destinations[1].Paths, 1)
	require.Equal(t, tagged.Uuid, l.Destinations[1].Paths[0].Uuid)
}

// Untagged writes that take or delete the route key of a tagged path keep
// the index right.
func TestUntaggedWritesEndTaggedPaths(t *testing.T) {
	h := startFenceServer(t, fenceConfig(t))
	c := h.dial()
	ctx := context.Background()
	for i, p := range []string{"10.9.1.0/24", "10.9.2.0/24", "10.9.3.0/24", "10.9.4.0/24"} {
		_, err := c.AddPath(ctx, &api.AddPathRequest{TableType: api.TableType_TABLE_TYPE_GLOBAL, Path: ucPath(t, p), GenerationTag: fenceTag("t", 1, strconv.Itoa(i))})
		require.NoError(t, err)
	}
	// Taken by an untagged AddPath, deleted by key, taken by an untagged
	// stream, deleted with the whole family.
	_, err := c.AddPath(ctx, &api.AddPathRequest{TableType: api.TableType_TABLE_TYPE_GLOBAL, Path: fenceTestPath(t, bgp.RF_IPv4_UC, "10.9.1.0/24", "10.0.0.9")})
	require.NoError(t, err)
	_, err = c.DeletePath(ctx, &api.DeletePathRequest{TableType: api.TableType_TABLE_TYPE_GLOBAL, Path: ucPath(t, "10.9.2.0/24")})
	require.NoError(t, err)
	stream, err := c.AddPathStream(ctx)
	require.NoError(t, err)
	require.NoError(t, stream.Send(&api.AddPathStreamRequest{TableType: api.TableType_TABLE_TYPE_GLOBAL, Paths: []*api.Path{fenceTestPath(t, bgp.RF_IPv4_UC, "10.9.3.0/24", "10.0.0.9")}}))
	_, err = stream.CloseAndRecv()
	require.NoError(t, err)

	st := getGeneration(t, c, "t", 1)
	states := map[string]api.GenerationPathState{}
	for _, p := range st.Paths {
		states[p.Nlri.GetPrefix().GetPrefix()] = p.State
		if p.State == api.GenerationPathState_GENERATION_PATH_STATE_REPLACED {
			require.Nil(t, p.ReplacedBy, "an untagged write has no tag")
		}
	}
	require.Equal(t, map[string]api.GenerationPathState{
		"10.9.1.0": api.GenerationPathState_GENERATION_PATH_STATE_REPLACED,
		"10.9.2.0": api.GenerationPathState_GENERATION_PATH_STATE_DELETED,
		"10.9.3.0": api.GenerationPathState_GENERATION_PATH_STATE_REPLACED,
		"10.9.4.0": api.GenerationPathState_GENERATION_PATH_STATE_CURRENT,
	}, states)

	_, err = c.DeletePath(ctx, &api.DeletePathRequest{TableType: api.TableType_TABLE_TYPE_GLOBAL, Family: apiFamily(bgp.RF_IPv4_UC)})
	require.NoError(t, err)
	st = getGeneration(t, c, "t", 1)
	require.Zero(t, st.CurrentPaths)
	require.Empty(t, globalPrefixes(t, h.s))
}

// With families that require a tag, untagged writes to them are refused and
// everything else behaves as before.
func TestGenerationRequireTag(t *testing.T) {
	cfg := fenceConfig(t)
	cfg.RequireTagFamilies = []bgp.Family{bgp.RF_IPv4_UC}
	h := startFenceServer(t, cfg)
	c := h.dial()
	ctx := context.Background()

	_, err := c.AddPath(ctx, &api.AddPathRequest{TableType: api.TableType_TABLE_TYPE_GLOBAL, Path: ucPath(t, "10.10.0.0/24")})
	requireCode(t, err, codes.FailedPrecondition)
	_, err = h.s.AddPath(apiutil.AddPathRequest{Paths: []*apiutil.Path{mustApi2apiutilPath(ucPath(t, "10.10.0.0/24"))}})
	require.ErrorIs(t, err, ErrGenerationTagRequired)

	lu, err := c.AddPath(ctx, &api.AddPathRequest{TableType: api.TableType_TABLE_TYPE_GLOBAL, Path: fenceTestPath(t, bgp.RF_IPv4_MPLS, "10.10.0.0/24", "10.0.0.1")})
	require.NoError(t, err, "a family that is not listed takes untagged writes")

	stream, err := c.AddPathStream(ctx)
	require.NoError(t, err)
	require.NoError(t, stream.Send(&api.AddPathStreamRequest{TableType: api.TableType_TABLE_TYPE_GLOBAL, Paths: []*api.Path{ucPath(t, "10.10.1.0/24")}}))
	_, err = stream.CloseAndRecv()
	requireCode(t, err, codes.FailedPrecondition)

	tagged, err := c.AddPath(ctx, &api.AddPathRequest{TableType: api.TableType_TABLE_TYPE_GLOBAL, Path: ucPath(t, "10.10.2.0/24"), GenerationTag: fenceTag("t", 1, "a")})
	require.NoError(t, err)

	_, err = c.DeletePath(ctx, &api.DeletePathRequest{TableType: api.TableType_TABLE_TYPE_GLOBAL, Path: ucPath(t, "10.10.2.0/24")})
	requireCode(t, err, codes.FailedPrecondition)
	_, err = c.DeletePath(ctx, &api.DeletePathRequest{TableType: api.TableType_TABLE_TYPE_GLOBAL, Uuid: tagged.Uuid})
	requireCode(t, err, codes.FailedPrecondition)
	require.ErrorIs(t, h.s.DeletePath(apiutil.DeletePathRequest{DeleteAll: true}), ErrGenerationTagRequired)
	ucFamily := bgp.RF_IPv4_UC
	require.ErrorIs(t, h.s.DeletePath(apiutil.DeletePathRequest{DeleteAll: true, DeleteFamily: &ucFamily}), ErrGenerationTagRequired)
	require.ElementsMatch(t, []string{"10.10.2.0/24"}, globalPrefixes(t, h.s))

	_, err = c.DeletePath(ctx, &api.DeletePathRequest{TableType: api.TableType_TABLE_TYPE_GLOBAL, Uuid: lu.Uuid})
	require.NoError(t, err)
	d, err := c.DeletePath(ctx, &api.DeletePathRequest{TableType: api.TableType_TABLE_TYPE_GLOBAL, Uuid: tagged.Uuid, GenerationTag: fenceTag("t", 1, "d")})
	require.NoError(t, err)
	require.Equal(t, api.GenerationDeleteOutcome_GENERATION_DELETE_OUTCOME_DELETED, d.Outcome)
	require.Empty(t, globalPrefixes(t, h.s))
}

func TestGenerationRequireTagNeedsDirectory(t *testing.T) {
	s := NewBgpServer(GenerationFenceOption(GenerationFenceConfig{RequireTagFamilies: []bgp.Family{bgp.RF_IPv4_UC}}))
	t.Cleanup(s.bfdServer.Stop)
	go s.Serve()
	require.Error(t, s.StartBgp(context.Background(), &api.StartBgpRequest{Global: &api.Global{Asn: 1, RouterId: "1.1.1.1", ListenPort: -1}}))
	//nolint: errcheck // ignore error
	s.StopBgp(context.Background(), &api.StopBgpRequest{})
}

// Without a fence directory, tagged writes and fences are refused and
// untagged writes behave as before.
func TestGenerationFenceDisabled(t *testing.T) {
	h := startFenceServer(t, nil)
	c := h.dial()
	ctx := context.Background()
	_, err := c.AddPath(ctx, &api.AddPathRequest{TableType: api.TableType_TABLE_TYPE_GLOBAL, Path: ucPath(t, "10.11.0.0/24"), GenerationTag: fenceTag("t", 1, "a")})
	requireCode(t, err, codes.FailedPrecondition)
	_, err = c.FenceGeneration(ctx, &api.FenceGenerationRequest{Namespace: "t", Generation: 1})
	requireCode(t, err, codes.FailedPrecondition)
	_, err = c.GetGeneration(ctx, &api.GetGenerationRequest{Namespace: "t", Generation: 1})
	requireCode(t, err, codes.FailedPrecondition)
	r, err := c.AddPath(ctx, &api.AddPathRequest{TableType: api.TableType_TABLE_TYPE_GLOBAL, Path: ucPath(t, "10.11.0.0/24")})
	require.NoError(t, err)
	require.False(t, r.Replayed)
	require.Empty(t, r.Incarnation)
	l := lookup(t, h, destinationKey(t, bgp.RF_IPv4_UC, "10.11.0.0/24"))
	require.Len(t, l.Destinations[0].Paths, 1)
	require.Empty(t, l.Destinations[0].Paths[0].Uuid)
	_, err = c.DeletePath(ctx, &api.DeletePathRequest{TableType: api.TableType_TABLE_TYPE_GLOBAL, Uuid: r.Uuid})
	require.NoError(t, err)
	require.Empty(t, globalPrefixes(t, h.s))
}

func TestGenerationCommandValidation(t *testing.T) {
	cfg := fenceConfig(t)
	cfg.InstanceID = fenceInstance
	h := startFenceServer(t, cfg)
	c := h.dial()
	ctx := context.Background()
	add := func(tag *api.GenerationTag, p *api.Path) error {
		_, err := c.AddPath(ctx, &api.AddPathRequest{TableType: api.TableType_TABLE_TYPE_GLOBAL, Path: p, GenerationTag: tag})
		return err
	}
	for _, tc := range []struct {
		name string
		tag  *api.GenerationTag
		code codes.Code
	}{
		{"namespace too long", &api.GenerationTag{Namespace: strings.Repeat("n", 257), Generation: 1, CommandId: "a", InstanceId: fenceInstance}, codes.InvalidArgument},
		{"generation 0", &api.GenerationTag{Namespace: "t", CommandId: "a", InstanceId: fenceInstance}, codes.InvalidArgument},
		{"no command", &api.GenerationTag{Namespace: "t", Generation: 1, InstanceId: fenceInstance}, codes.InvalidArgument},
		{"other instance", &api.GenerationTag{Namespace: "t", Generation: 1, CommandId: "a", InstanceId: "instance-2"}, codes.FailedPrecondition},
	} {
		t.Run(tc.name, func(t *testing.T) {
			requireCode(t, add(tc.tag, ucPath(t, "10.12.0.0/24")), tc.code)
		})
	}
	// Namespace and instance ID are optional.
	r, err := c.AddPath(ctx, &api.AddPathRequest{TableType: api.TableType_TABLE_TYPE_GLOBAL, Path: ucPath(t, "10.12.1.0/24"), GenerationTag: &api.GenerationTag{Generation: 1, CommandId: "a"}})
	require.NoError(t, err)
	require.Equal(t, fenceInstance, r.InstanceId)
	st, err := c.GetGeneration(ctx, &api.GetGenerationRequest{Generation: 1})
	require.NoError(t, err)
	require.Len(t, st.Status.Paths, 1)
	w := ucPath(t, "10.12.0.0/24")
	w.IsWithdraw = true
	requireCode(t, add(fenceTag("t", 1, "w"), w), codes.InvalidArgument)
	_, err = c.FenceGeneration(ctx, &api.FenceGenerationRequest{Namespace: "t", Generation: 1, InstanceId: "instance-2"})
	requireCode(t, err, codes.FailedPrecondition)
	_, err = c.DeletePath(ctx, &api.DeletePathRequest{TableType: api.TableType_TABLE_TYPE_GLOBAL, Path: ucPath(t, "10.12.0.0/24"), GenerationTag: fenceTag("t", 1, "d")})
	requireCode(t, err, codes.InvalidArgument)
	id := uuid.New()
	_, err = c.DeletePath(ctx, &api.DeletePathRequest{TableType: api.TableType_TABLE_TYPE_GLOBAL, Uuid: id[:], GenerationTag: fenceTag("t", 1, "d")})
	requireCode(t, err, codes.NotFound)
	require.Equal(t, []string{"10.12.1.0/24"}, globalPrefixes(t, h.s))
}

// Generations that own no current path are kept up to the retention, oldest
// dropped first; a generation that owns a current path is never dropped.
func TestSettledGenerationsAreBounded(t *testing.T) {
	cfg := fenceConfig(t)
	cfg.SettledGenerationRetention = 2
	h := startFenceServer(t, cfg)
	c := h.dial()
	ctx := context.Background()
	_, err := c.AddPath(ctx, &api.AddPathRequest{TableType: api.TableType_TABLE_TYPE_GLOBAL, Path: ucPath(t, "10.13.0.0/24"), GenerationTag: fenceTag("t", 1, "a")})
	require.NoError(t, err)
	for g := uint64(2); g <= 10; g++ {
		r, err := c.AddPath(ctx, &api.AddPathRequest{TableType: api.TableType_TABLE_TYPE_GLOBAL, Path: ucPath(t, fmt.Sprintf("10.13.%d.0/24", g)), GenerationTag: fenceTag("t", g, "a")})
		require.NoError(t, err)
		_, err = c.DeletePath(ctx, &api.DeletePathRequest{TableType: api.TableType_TABLE_TYPE_GLOBAL, Uuid: r.Uuid, GenerationTag: fenceTag("t", g, "d")})
		require.NoError(t, err)
	}
	var records, uuids int
	require.NoError(t, h.s.mgmtOperation(func() error {
		records, uuids = len(h.s.generations.records), len(h.s.generations.byUUID)
		return nil
	}, false))
	require.Equal(t, 3, records, "generation 1 and the two newest settled ones")
	require.Equal(t, 3, uuids)
	require.Equal(t, api.GenerationState_GENERATION_STATE_LIVE, getGeneration(t, c, "t", 1).State)
	require.Equal(t, api.GenerationState_GENERATION_STATE_LIVE, getGeneration(t, c, "t", 10).State)
	require.Equal(t, api.GenerationState_GENERATION_STATE_UNKNOWN, getGeneration(t, c, "t", 2).State)
}

// A tagged stream batch is one command: its paths get UUIDs that
// GetGeneration lists, and a replay of the batch is not applied again.
func TestGenerationTaggedStream(t *testing.T) {
	h := startFenceServer(t, fenceConfig(t))
	c := h.dial()
	ctx := context.Background()
	send := func() error {
		stream, err := c.AddPathStream(ctx)
		require.NoError(t, err)
		require.NoError(t, stream.Send(&api.AddPathStreamRequest{
			TableType:     api.TableType_TABLE_TYPE_GLOBAL,
			Paths:         []*api.Path{ucPath(t, "10.14.0.0/24"), ucPath(t, "10.14.1.0/24")},
			GenerationTag: fenceTag("t", 1, "batch"),
		}))
		_, err = stream.CloseAndRecv()
		if errors.Is(err, io.EOF) {
			return nil
		}
		return err
	}
	require.NoError(t, send())
	require.NoError(t, send())
	st := getGeneration(t, c, "t", 1)
	require.Len(t, st.Receipts, 1)
	require.Equal(t, api.GenerationCommandKind_GENERATION_COMMAND_KIND_ADD_PATH_STREAM, st.Receipts[0].Kind)
	require.Len(t, st.Receipts[0].Uuids, 2)
	require.Len(t, pathUUIDs(t, st, api.GenerationPathState_GENERATION_PATH_STATE_CURRENT), 2)
	require.ElementsMatch(t, []string{"10.14.0.0/24", "10.14.1.0/24"}, globalPrefixes(t, h.s))
}

// largeTablePaths is the size of the table the indexed tests and
// benchmarks run on. GOBGP_FENCE_TABLE_PATHS overrides it.
func largeTablePaths(def int) int {
	if v, err := strconv.ParseUint(os.Getenv("GOBGP_FENCE_TABLE_PATHS"), 10, 31); err == nil && v > 0 {
		return int(v)
	}
	return def
}

// fillTable adds n untagged local /32 paths in 20.0.0.0/8 and beyond.
func fillTable(tb testing.TB, s *BgpServer, n int) {
	tb.Helper()
	nh, err := bgp.NewPathAttributeNextHop(netip.MustParseAddr("10.0.0.1"))
	require.NoError(tb, err)
	attrs := []bgp.PathAttributeInterface{bgp.NewPathAttributeOrigin(0), nh}
	const batch = 10000
	base := uint32(20) << 24
	now := time.Now()
	for i := 0; i < n; i += batch {
		paths := make([]*table.Path, 0, batch)
		for j := i; j < i+batch && j < n; j++ {
			a := base + uint32(j)
			addr := netip.AddrFrom4([4]byte{byte(a >> 24), byte(a >> 16), byte(a >> 8), byte(a)})
			nlri, err := bgp.NewIPAddrPrefix(netip.PrefixFrom(addr, 32))
			require.NoError(tb, err)
			paths = append(paths, table.NewPath(bgp.RF_IPv4_UC, nil, bgp.PathNLRI{NLRI: nlri}, false, attrs, now, false))
		}
		require.NoError(tb, s.addPathStream(context.Background(), "", paths))
	}
}

func tableSize(tb testing.TB, s *BgpServer) int {
	tb.Helper()
	n := 0
	require.NoError(tb, s.mgmtOperation(func() error {
		n = len(s.globalRib.GetPathList(table.GLOBAL_RIB_NAME, 0, []bgp.Family{bgp.RF_IPv4_UC}))
		return nil
	}, false))
	return n
}

// On a large table, deleting tagged paths by UUID and proving them absent
// costs less than one walk of the table, which is what the legacy delete
// by UUID does for each UUID.
func TestGenerationIndexedDeleteOnLargeTable(t *testing.T) {
	if testing.Short() {
		t.Skip("large table")
	}
	n := largeTablePaths(200000)
	h := startFenceServer(t, fenceConfig(t))
	fillTable(t, h.s, n)
	const tagged = 100
	ids := make([]uuid.UUID, tagged)
	keys := make([]testKey, tagged)
	for i := range tagged {
		prefix := fmt.Sprintf("10.15.%d.0/24", i)
		r, err := h.s.AddPath(apiutil.AddPathRequest{
			Paths:      []*apiutil.Path{mustApi2apiutilPath(ucPath(t, prefix))},
			Generation: &apiutil.GenerationTag{Namespace: "t", Generation: 1, CommandID: "a" + strconv.Itoa(i), InstanceID: fenceInstance},
		})
		require.NoError(t, err)
		ids[i] = r[0].UUID
		keys[i] = destinationKey(t, bgp.RF_IPv4_UC, prefix)
	}
	require.Equal(t, n+tagged, tableSize(t, h.s))

	start := time.Now()
	require.NoError(t, h.s.mgmtOperation(func() error {
		_ = h.s.globalRib.GetPathList(table.GLOBAL_RIB_NAME, 0, h.s.globalRib.GetRFlist())
		return nil
	}, false))
	walk := time.Since(start)

	ctx := context.Background()
	_, err := h.s.FenceGeneration(ctx, &api.FenceGenerationRequest{Namespace: "t", Generation: 1, InstanceId: fenceInstance})
	require.NoError(t, err)
	start = time.Now()
	for i, id := range ids {
		outcome, _, err := h.s.deletePathOutcome(ctx, apiutil.DeletePathRequest{
			UUIDs:      []uuid.UUID{id},
			Generation: &apiutil.GenerationTag{Namespace: "t", Generation: 1, CommandID: "d" + strconv.Itoa(i), InstanceID: fenceInstance},
		})
		require.NoError(t, err)
		require.Equal(t, api.GenerationDeleteOutcome_GENERATION_DELETE_OUTCOME_DELETED, outcome)
	}
	l := lookup(t, h, keys...)
	st, err := h.s.GetGeneration(ctx, &api.GetGenerationRequest{Namespace: "t", Generation: 1})
	require.NoError(t, err)
	indexed := time.Since(start)
	t.Logf("table of %d paths: one walk %v; %d deletes by UUID, the lookup of their keys and the generation status %v", n+tagged, walk, tagged, indexed)

	for _, d := range l.Destinations {
		require.Empty(t, d.Paths)
	}
	require.Zero(t, st.Status.CurrentPaths)
	for _, p := range st.Status.Paths {
		require.False(t, p.Present)
	}
	require.Equal(t, n, tableSize(t, h.s), "the untagged paths are untouched")
	require.Less(t, indexed, walk, "deletes and lookups do not walk the table")
}

// Every answer of the fence carries the configured instance ID, and an empty
// one when none is configured, so a client can check which server answered.
func TestGenerationResponsesReportInstanceID(t *testing.T) {
	for _, tc := range []struct {
		name string
		cfg  func(t *testing.T) *GenerationFenceConfig
		want string
	}{
		{"configured", func(t *testing.T) *GenerationFenceConfig {
			c := fenceConfig(t)
			c.InstanceID = fenceInstance
			return c
		}, fenceInstance},
		{"unset", fenceConfig, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := startFenceServer(t, tc.cfg(t))
			c := h.dial()
			ctx := context.Background()
			tagged, err := c.AddPath(ctx, &api.AddPathRequest{TableType: api.TableType_TABLE_TYPE_GLOBAL, Path: ucPath(t, "10.16.0.0/24"), GenerationTag: fenceTag("t", 1, "a")})
			require.NoError(t, err)
			require.Equal(t, tc.want, tagged.InstanceId)
			untagged, err := c.AddPath(ctx, &api.AddPathRequest{TableType: api.TableType_TABLE_TYPE_GLOBAL, Path: ucPath(t, "10.16.1.0/24")})
			require.NoError(t, err)
			require.Equal(t, tc.want, untagged.InstanceId)
			require.Equal(t, tc.want, getGeneration(t, c, "t", 1).InstanceId)
			require.Equal(t, tc.want, fence(t, c, "t", 1).Status.InstanceId)
		})
	}
	// Without the fence there is no instance ID either.
	h := startFenceServer(t, nil)
	c := h.dial()
	r, err := c.AddPath(context.Background(), &api.AddPathRequest{TableType: api.TableType_TABLE_TYPE_GLOBAL, Path: ucPath(t, "10.16.2.0/24")})
	require.NoError(t, err)
	require.Empty(t, r.InstanceId)
}
