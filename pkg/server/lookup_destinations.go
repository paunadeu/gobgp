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

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/osrg/gobgp/v4/api"
	"github.com/osrg/gobgp/v4/pkg/apiutil"
	"github.com/osrg/gobgp/v4/pkg/packet/bgp"
)

// MaxLookupDestinations is the most keys one LookupDestinations may name.
const MaxLookupDestinations = 10000

// LookupDestinations reads destinations of the global table, each by its
// exact key: one hash lookup per key, for every family, where ListPath can
// filter only some families by prefix and otherwise walks the whole table.
// It runs in one step of the management loop, so it sees every mutation
// acknowledged before it.
func (s *BgpServer) LookupDestinations(ctx context.Context, r *api.LookupDestinationsRequest) (*api.LookupDestinationsResponse, error) {
	if r == nil {
		return nil, status.Error(codes.InvalidArgument, "nil request")
	}
	if r.TableType != api.TableType_TABLE_TYPE_GLOBAL {
		return nil, status.Error(codes.InvalidArgument, "only the global table can be looked up")
	}
	if len(r.Keys) > MaxLookupDestinations {
		return nil, status.Errorf(codes.InvalidArgument, "at most %d keys", MaxLookupDestinations)
	}
	type lookupKey struct {
		key    *api.DestinationKey
		family bgp.Family
		nlri   bgp.NLRI
	}
	keys := make([]lookupKey, 0, len(r.Keys))
	for _, k := range r.Keys {
		if k == nil || k.Family == nil || k.Nlri == nil {
			return nil, status.Error(codes.InvalidArgument, "a key needs a family and an NLRI")
		}
		f := bgp.NewFamily(uint16(k.Family.Afi), uint8(k.Family.Safi))
		nlri, err := apiutil.UnmarshalNLRI(f, k.Nlri)
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "invalid NLRI: %v", err)
		}
		keys = append(keys, lookupKey{k, f, nlri})
	}
	resp := &api.LookupDestinationsResponse{Destinations: make([]*api.DestinationLookup, len(keys))}
	err := s.mgmtOperationWithContext(ctx, func() error {
		for i, k := range keys {
			d := &api.DestinationLookup{Key: k.key}
			resp.Destinations[i] = d
			tbl, ok := s.globalRib.GetTable(k.family)
			if !ok {
				continue
			}
			dest := tbl.GetDestination(k.nlri)
			if dest == nil {
				continue
			}
			for _, p := range dest.GetAllKnownPathList() {
				d.Paths = append(d.Paths, toPathApi(toPathApiUtil(p), false, false, false))
			}
		}
		return nil
	}, true)
	if err != nil {
		return nil, err
	}
	return resp, nil
}
