package server

import (
	"context"
	"fmt"
	"strconv"
	"testing"

	"github.com/osrg/gobgp/v4/api"
	"github.com/osrg/gobgp/v4/pkg/apiutil"
)

// One FenceGenerations of n new generations: one management operation and
// one synced write of the tombstone store, plus a status per generation.
// "single" is n FenceGeneration calls, for comparison.
func BenchmarkFenceGenerations(b *testing.B) {
	s := benchServer(b)
	ctx := context.Background()
	for _, n := range []int{1, 20, 200, 1000} {
		b.Run(fmt.Sprintf("batch-%d", n), func(b *testing.B) {
			for range b.N {
				gens := make([]uint64, n)
				for i := range gens {
					benchFence.seq++
					gens[i] = uint64(3000000000 + benchFence.seq)
				}
				if _, err := s.FenceGenerations(ctx, &api.FenceGenerationsRequest{Namespace: "bench", Generations: gens, InstanceId: fenceInstance}); err != nil {
					b.Fatal(err)
				}
			}
		})
		b.Run(fmt.Sprintf("single-%d", n), func(b *testing.B) {
			for range b.N {
				for range n {
					benchFence.seq++
					if _, err := s.FenceGeneration(ctx, &api.FenceGenerationRequest{Namespace: "bench", Generation: uint64(3000000000 + benchFence.seq), InstanceId: fenceInstance}); err != nil {
						b.Fatal(err)
					}
				}
			}
		})
	}
}

// benchFencedGenerations writes n generations of k paths each and fences
// them, outside the timer.
func benchFencedGenerations(b *testing.B, s *BgpServer, n, k int) []uint64 {
	b.Helper()
	gens := make([]uint64, n)
	for i := range gens {
		benchFence.seq++
		gens[i] = uint64(4000000000 + benchFence.seq)
		for range k {
			if _, err := s.AddPath(apiutil.AddPathRequest{
				Paths:      []*apiutil.Path{mustApi2apiutilPath(ucPath(b, benchPrefix()))},
				Generation: benchTag(gens[i], "p"+strconv.Itoa(benchFence.seq)),
			}); err != nil {
				b.Fatal(err)
			}
		}
	}
	for i := 0; i < n; i += MaxFenceGenerations {
		if _, err := s.FenceGenerations(context.Background(), &api.FenceGenerationsRequest{Namespace: "bench", Generations: gens[i:min(i+MaxFenceGenerations, n)], InstanceId: fenceInstance}); err != nil {
			b.Fatal(err)
		}
	}
	return gens
}

// DeleteGenerations of n fenced generations of k paths each, statuses
// included. "onestep" lifts the step bound, to show what one step of the
// whole batch would hold the management loop for.
func BenchmarkDeleteGenerations(b *testing.B) {
	s := benchServer(b)
	ctx := context.Background()
	for _, tc := range []struct {
		n, k    int
		oneStep bool
	}{
		{1, 1, false},
		{20, 1, false},
		{200, 1, false},
		{1000, 1, false},
		{100, 10, false},
		{100, 50, false},
		{1000, 10, false},
		{1000, 1, true},
		{1000, 10, true},
	} {
		name := fmt.Sprintf("gens-%d/paths-%d", tc.n, tc.k)
		if tc.oneStep {
			name += "/onestep"
		}
		b.Run(name, func(b *testing.B) {
			old := DeleteGenerationsStepPaths
			if tc.oneStep {
				DeleteGenerationsStepPaths = 1 << 30
			}
			defer func() { DeleteGenerationsStepPaths = old }()
			var steps uint32
			b.ResetTimer()
			for range b.N {
				b.StopTimer()
				gens := benchFencedGenerations(b, s, tc.n, tc.k)
				b.StartTimer()
				r, err := s.DeleteGenerations(ctx, &api.DeleteGenerationsRequest{Namespace: "bench", Generations: gens, InstanceId: fenceInstance})
				if err != nil {
					b.Fatal(err)
				}
				steps = r.Steps
			}
			b.StopTimer()
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*tc.n*tc.k), "ns/path")
			b.ReportMetric(float64(steps), "steps")
		})
	}
}
