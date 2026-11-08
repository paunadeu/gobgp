package server

import (
	"context"
	"fmt"
	"net/netip"
	"os"
	"strconv"
	"sync"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/osrg/gobgp/v4/api"
	"github.com/osrg/gobgp/v4/pkg/apiutil"
	"github.com/osrg/gobgp/v4/pkg/packet/bgp"
)

// The benchmarks share one server whose table holds largeTablePaths(1M)
// untagged local paths, the shape of a full speaker table.
var benchFence struct {
	once sync.Once
	s    *BgpServer
	n    int
	seq  int
}

func benchServer(b *testing.B) *BgpServer {
	b.Helper()
	benchFence.once.Do(func() {
		dir, err := os.MkdirTemp("", "gobgp-fence-bench")
		require.NoError(b, err)
		s := NewBgpServer(GenerationFenceOption(GenerationFenceConfig{Dir: dir}))
		go s.Serve()
		require.NoError(b, s.StartBgp(context.Background(), &api.StartBgpRequest{
			Global: &api.Global{Asn: 1, RouterId: "1.1.1.1", ListenPort: -1},
		}))
		benchFence.n = largeTablePaths(1000000)
		fillTable(b, s, benchFence.n)
		benchFence.s = s
	})
	return benchFence.s
}

// benchPrefix gives each call a prefix of its own in 30.0.0.0/8, outside
// the filled table.
func benchPrefix() string {
	benchFence.seq++
	a := uint32(30)<<24 + uint32(benchFence.seq)
	return netip.PrefixFrom(netip.AddrFrom4([4]byte{byte(a >> 24), byte(a >> 16), byte(a >> 8), byte(a)}), 32).String()
}

func benchTag(generation uint64, command string) *apiutil.GenerationTag {
	return &apiutil.GenerationTag{Tenant: "bench", Generation: generation, CommandID: command, Speaker: fenceSpeaker}
}

func benchAdd(b *testing.B, s *BgpServer, generation uint64, tagged bool) uuid.UUID {
	b.Helper()
	req := apiutil.AddPathRequest{Paths: []*apiutil.Path{mustApi2apiutilPath(ucPath(b, benchPrefix()))}}
	if tagged {
		req.Generation = benchTag(generation, "a"+strconv.Itoa(benchFence.seq))
	}
	r, err := s.AddPath(req)
	require.NoError(b, err)
	return r[0].UUID
}

func reportTable(b *testing.B, s *BgpServer) {
	b.ReportMetric(float64(tableSize(b, s)), "table-paths")
}

// A tagged delete by UUID: one index lookup and one destination read.
func BenchmarkGenerationDeleteByUUID(b *testing.B) {
	s := benchServer(b)
	ctx := context.Background()
	b.ResetTimer()
	for range b.N {
		b.StopTimer()
		id := benchAdd(b, s, 1, true)
		b.StartTimer()
		outcome, _, err := s.deletePathOutcome(ctx, apiutil.DeletePathRequest{UUIDs: []uuid.UUID{id}, Generation: benchTag(1, "d"+id.String())})
		if err != nil || outcome != api.GenerationDeleteOutcome_GENERATION_DELETE_OUTCOME_DELETED {
			b.Fatalf("delete: %v %v", outcome, err)
		}
	}
	b.StopTimer()
	reportTable(b, s)
}

// The legacy delete by UUID walks the table for each UUID. Run it with a
// small -benchtime=Nx.
func BenchmarkLegacyDeleteByUUID(b *testing.B) {
	s := benchServer(b)
	b.ResetTimer()
	for range b.N {
		b.StopTimer()
		id := benchAdd(b, s, 0, false)
		b.StartTimer()
		if err := s.DeletePath(apiutil.DeletePathRequest{UUIDs: []uuid.UUID{id}}); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	reportTable(b, s)
}

// A tagged AddPath, with its fence check.
func BenchmarkGenerationAddPath(b *testing.B) {
	s := benchServer(b)
	paths := make([]*apiutil.Path, b.N)
	first := benchFence.seq
	for i := range paths {
		paths[i] = mustApi2apiutilPath(ucPath(b, benchPrefix()))
	}
	b.ResetTimer()
	for i := range b.N {
		if _, err := s.AddPath(apiutil.AddPathRequest{Paths: paths[i : i+1], Generation: benchTag(2, fmt.Sprintf("add-%d-%d", first, i))}); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	reportTable(b, s)
}

// An exact read of one destination, the absence proof for one key.
func BenchmarkLookupDestination(b *testing.B) {
	s := benchServer(b)
	key := destinationKey(b, bgp.RF_IPv4_UC, "20.1.2.3/32")
	ctx := context.Background()
	req := &api.LookupDestinationsRequest{TableType: api.TableType_TABLE_TYPE_GLOBAL, Keys: []*api.DestinationKey{key}}
	b.ResetTimer()
	for range b.N {
		r, err := s.LookupDestinations(ctx, req)
		if err != nil || len(r.Destinations[0].Paths) != 1 {
			b.Fatalf("lookup: %v %v", r, err)
		}
	}
	b.StopTimer()
	reportTable(b, s)
}

// The status of a generation that owns 100 paths, each checked in the RIB.
func BenchmarkGetGeneration100(b *testing.B) {
	s := benchServer(b)
	// A generation of its own for each run.
	benchFence.seq++
	generation := uint64(1000000000 + benchFence.seq)
	for range 100 {
		benchAdd(b, s, generation, true)
	}
	ctx := context.Background()
	req := &api.GetGenerationRequest{Tenant: "bench", Generation: generation}
	b.ResetTimer()
	for range b.N {
		r, err := s.GetGeneration(ctx, req)
		if err != nil || r.Status.CurrentPaths < 100 {
			b.Fatalf("status: %v %v", r, err)
		}
	}
	b.StopTimer()
	reportTable(b, s)
}

// A fence of a new generation: one synced write of the tombstone file.
func BenchmarkFenceGeneration(b *testing.B) {
	s := benchServer(b)
	ctx := context.Background()
	// New generations for each run, so that none is a cache hit.
	benchFence.seq += b.N
	base := uint64(2000000000 + benchFence.seq)
	b.ResetTimer()
	for i := range b.N {
		if _, err := s.FenceGeneration(ctx, &api.FenceGenerationRequest{Tenant: "bench", Generation: base + uint64(i), Speaker: fenceSpeaker}); err != nil {
			b.Fatal(err)
		}
	}
}
