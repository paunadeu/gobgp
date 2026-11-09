// Copyright (C) 2026 Pau Nadeu.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package server

import (
	"context"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc/codes"

	"github.com/osrg/gobgp/v4/api"
	"github.com/osrg/gobgp/v4/internal/pkg/table"
)

// DeleteGenerationsStepPaths bounds the paths one management-loop step of
// DeleteGenerations withdraws, so that a large batch does not hold the
// loop for long. A variable so that tests can make batches take several
// steps.
var DeleteGenerationsStepPaths = 2048

// deleteGenerationsStepHook, when set, runs between two steps of
// DeleteGenerations, outside the loop. Tests use it.
var deleteGenerationsStepHook func(step int)

// distinctGenerations checks a batch of generations and drops repeats,
// keeping the order of first mention. The limit applies to the entries as
// sent, repeats included.
func distinctGenerations(tenant string, generations []uint64, speaker string) ([]uint64, error) {
	if len(generations) == 0 {
		return nil, genErr(codes.InvalidArgument, ErrGenerationInvalid, "no generation")
	}
	if len(generations) > MaxFenceGenerations {
		return nil, genErr(codes.InvalidArgument, ErrGenerationInvalid, "%d generations, at most %d per call", len(generations), MaxFenceGenerations)
	}
	gens := make([]uint64, 0, len(generations))
	seen := make(map[uint64]struct{}, len(generations))
	for _, g := range generations {
		if err := validateGeneration(tenant, g, speaker); err != nil {
			return nil, err
		}
		if _, dup := seen[g]; !dup {
			seen[g] = struct{}{}
			gens = append(gens, g)
		}
	}
	return gens, nil
}

type deletePlan struct {
	gens     []uint64
	results  []*api.GenerationDeleteResult
	outcome  []map[*genPath]api.GenerationDeleteOutcome
	records  []*genRecord
	pending  []*genPath
	pendingG []int
}

// DeleteGenerations deletes every current path of the listed fenced
// generations. A path is withdrawn only while one of them owns it, so a
// path that another write took over stays. Validation errors refuse the
// whole call before anything changes; a generation that is not fenced is
// reported refused and the others proceed. It is idempotent.
//
// The work runs in management-loop steps of at most
// DeleteGenerationsStepPaths withdrawals each, and the statuses are taken in
// the last step. Each step rechecks the owner of every path it withdraws,
// so a write that lands between two steps is respected. When the call fails
// halfway (its context ends, or the server stops), the paths deleted so far
// stay deleted and a retry finishes the rest; the records it touched stay
// out of the settled list, so out of the retention bound, until a later
// command on them settles them again.
func (s *BgpServer) DeleteGenerations(ctx context.Context, r *api.DeleteGenerationsRequest) (*api.DeleteGenerationsResponse, error) {
	if r == nil {
		return nil, genErr(codes.InvalidArgument, ErrGenerationInvalid, "nil request")
	}
	gens, err := distinctGenerations(r.Tenant, r.Generations, r.Speaker)
	if err != nil {
		return nil, err
	}
	var plan *deletePlan
	var resp *api.DeleteGenerationsResponse
	for step := 0; resp == nil; step++ {
		if step > 0 && deleteGenerationsStepHook != nil {
			deleteGenerationsStepHook(step)
		}
		err := s.mgmtOperationWithContext(ctx, func() error {
			if plan == nil {
				p, err := s.planDeleteGenerations(r.Tenant, r.Speaker, gens)
				if err != nil {
					return err
				}
				plan = p
			}
			s.deleteGenerationsStep(plan, time.Now().UTC())
			if len(plan.pending) > 0 {
				return nil
			}
			out, err := s.finishDeleteGenerations(r.Tenant, plan)
			if err != nil {
				return err
			}
			out.Steps = uint32(step + 1)
			resp = out
			return nil
		}, true)
		if err != nil {
			return nil, err
		}
	}
	return resp, nil
}

// planDeleteGenerations runs in the first step: it checks the fence of each
// generation and lists the paths to delete.
func (s *BgpServer) planDeleteGenerations(tenant, speaker string, gens []uint64) (*deletePlan, error) {
	idx := s.generations
	if err := idx.enabled(); err != nil {
		return nil, err
	}
	if err := idx.checkSpeaker(speaker); err != nil {
		return nil, err
	}
	p := &deletePlan{
		gens:    gens,
		results: make([]*api.GenerationDeleteResult, len(gens)),
		outcome: make([]map[*genPath]api.GenerationDeleteOutcome, len(gens)),
		records: make([]*genRecord, len(gens)),
	}
	for i, g := range gens {
		k := genKey{tenant, g}
		_, fenced, err := idx.fenced(k)
		if err != nil {
			return nil, err
		}
		p.results[i] = &api.GenerationDeleteResult{Generation: g, Refused: !fenced}
		p.outcome[i] = make(map[*genPath]api.GenerationDeleteOutcome)
		rec := idx.records[k]
		if rec == nil {
			continue
		}
		p.records[i] = rec
		// Kept out of the settled list until the last step, so that no
		// eviction drops it halfway.
		idx.unsettle(rec)
		for _, gp := range rec.paths {
			switch {
			case !fenced:
				p.outcome[i][gp] = api.GenerationDeleteOutcome_GENERATION_DELETE_OUTCOME_REFUSED
			case gp.state == api.GenerationPathState_GENERATION_PATH_STATE_CURRENT:
				p.pending = append(p.pending, gp)
				p.pendingG = append(p.pendingG, i)
			case gp.state == api.GenerationPathState_GENERATION_PATH_STATE_REPLACED:
				p.outcome[i][gp] = api.GenerationDeleteOutcome_GENERATION_DELETE_OUTCOME_REPLACED
			default:
				p.outcome[i][gp] = api.GenerationDeleteOutcome_GENERATION_DELETE_OUTCOME_ALREADY_DELETED
			}
		}
	}
	return p, nil
}

// deleteGenerationsStep withdraws up to DeleteGenerationsStepPaths pending
// paths, rechecking that each is still current, with one propagation
// (propagateLocal splits it only around a slot with an untagged UUID).
func (s *BgpServer) deleteGenerationsStep(p *deletePlan, now time.Time) {
	n := min(len(p.pending), max(DeleteGenerationsStepPaths, 1))
	withdraws := make([]*table.Path, 0, n)
	for j, gp := range p.pending[:n] {
		i := p.pendingG[j]
		switch gp.state {
		case api.GenerationPathState_GENERATION_PATH_STATE_CURRENT:
			if rp := s.localPathAt(gp.path, gp.key.id); rp != nil {
				withdraws = append(withdraws, rp.Clone(true))
				p.outcome[i][gp] = api.GenerationDeleteOutcome_GENERATION_DELETE_OUTCOME_DELETED
			} else {
				p.outcome[i][gp] = api.GenerationDeleteOutcome_GENERATION_DELETE_OUTCOME_ALREADY_ABSENT
			}
			s.generations.endRoute(gp.key, api.GenerationPathState_GENERATION_PATH_STATE_DELETED, uuid.Nil, nil, now)
		case api.GenerationPathState_GENERATION_PATH_STATE_REPLACED:
			// Taken over between two steps: kept.
			p.outcome[i][gp] = api.GenerationDeleteOutcome_GENERATION_DELETE_OUTCOME_REPLACED
		default:
			p.outcome[i][gp] = api.GenerationDeleteOutcome_GENERATION_DELETE_OUTCOME_ALREADY_DELETED
		}
	}
	if len(withdraws) > 0 {
		s.propagateLocal(withdraws, false)
	}
	p.pending, p.pendingG = p.pending[n:], p.pendingG[n:]
}

// finishDeleteGenerations runs in the last step: it builds the outcomes and
// the statuses, and settles the records again.
func (s *BgpServer) finishDeleteGenerations(tenant string, p *deletePlan) (*api.DeleteGenerationsResponse, error) {
	out := &api.DeleteGenerationsResponse{Results: p.results}
	for i, g := range p.gens {
		if rec := p.records[i]; rec != nil {
			for _, gp := range rec.paths {
				if o, ok := p.outcome[i][gp]; ok {
					p.results[i].Outcomes = append(p.results[i].Outcomes, &api.GenerationPathOutcome{Uuid: uuidBytes(gp.id), Outcome: o})
				}
			}
		}
		st, err := s.generationStatus(genKey{tenant, g})
		if err != nil {
			return nil, err
		}
		p.results[i].Status = st
	}
	for _, rec := range p.records {
		if rec != nil {
			s.generations.settle(rec)
		}
	}
	return out, nil
}

// GetGenerations is GetGeneration for a batch, read in one step.
func (s *BgpServer) GetGenerations(ctx context.Context, r *api.GetGenerationsRequest) (*api.GetGenerationsResponse, error) {
	if r == nil {
		return nil, genErr(codes.InvalidArgument, ErrGenerationInvalid, "nil request")
	}
	gens, err := distinctGenerations(r.Tenant, r.Generations, "")
	if err != nil {
		return nil, err
	}
	var resp *api.GetGenerationsResponse
	err = s.mgmtOperationWithContext(ctx, func() error {
		if err := s.generations.enabled(); err != nil {
			return err
		}
		out := &api.GetGenerationsResponse{Statuses: make([]*api.GenerationStatus, len(gens))}
		for i, g := range gens {
			st, err := s.generationStatus(genKey{r.Tenant, g})
			if err != nil {
				return err
			}
			out.Statuses[i] = st
		}
		resp = out
		return nil
	}, true)
	if err != nil {
		return nil, err
	}
	return resp, nil
}
