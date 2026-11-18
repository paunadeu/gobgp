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
	"net/netip"
	"sync"
	"sync/atomic"

	"github.com/google/uuid"

	"github.com/osrg/gobgp/v4/internal/pkg/table"
	"github.com/osrg/gobgp/v4/pkg/packet/bgp"
)

// localPathKey is the slot a local path takes in the global table: its
// destination, by the identity the table keys it by, and the source and
// path identifier that a later path must share to replace it.
type localPathKey struct {
	family        bgp.Family
	destination   string
	sourceAS      uint32
	sourceID      netip.Addr
	sourceLocalID netip.Addr
	sourceAddress netip.Addr
	pathID        uint32
}

func localPathKeyOf(p *table.Path) localPathKey {
	k := localPathKey{
		family:      p.GetFamily(),
		destination: table.DestinationIdentity(p.GetNlri()),
		pathID:      p.RemoteID(),
	}
	if src := p.GetSource(); src != nil {
		k.sourceAS, k.sourceID, k.sourceLocalID, k.sourceAddress = src.AS, src.ID, src.LocalID, src.Address
	}
	return k
}

// pathIndexEntry is the UUID AddPath gave one path. The entry names the
// path AddPath wrote, local or carrying a peer identity. The table may hold a
// copy of it instead, made by an import policy or by a zebra next-hop
// refresh; every copy has the written path as its root (table.Path.Root), so
// the entry follows them. Its fields never change after add.
type pathIndexEntry struct {
	id      uuid.UUID
	key     localPathKey
	written *table.Path
	vrf     string
}

// pathIndex maps the UUIDs AddPath gives to the paths the global table holds
// for them, so that DeletePath by UUID reads one destination instead of
// walking the table.
//
// An entry holds while the global table holds a version of its written
// path. It ends when a write or a withdrawal removes that version: another
// path that replaces it, a withdrawal, an import policy rejecting it, or an
// EVPN MAC move caused by a received route. propagateUpdate retires the
// entry of every path the global table removes in favor of a path with
// another root, and propagateLocal also reads a watched slot again after
// each write. A refresh that reinstalls a copy of the path, such as a zebra
// next-hop change, keeps it. A delete of all local paths of a family ends
// the entries of that family, and DeleteVrf the entries whose path it
// withdrew.
//
// Locking: management operations run with shared.mu held exclusively, and
// the handlers of peer messages with shared.mu held shared, several at a
// time, and those retire entries too. So every method takes mu, and the
// size counter lets the peer path skip the lock while the index is empty.
// No method holds mu across a propagation, which can retire entries.
type pathIndex struct {
	mu        sync.Mutex
	size      atomic.Int64
	byUUID    map[uuid.UUID]*pathIndexEntry
	byKey     map[localPathKey]map[*pathIndexEntry]struct{}
	byWritten map[*table.Path]*pathIndexEntry
}

func newPathIndex() *pathIndex {
	x := &pathIndex{}
	x.resetLocked()
	return x
}

// resetLocked gives the index new empty maps. Go maps keep the buckets of
// their largest size, so an index that empties drops them.
func (x *pathIndex) resetLocked() {
	x.byUUID = make(map[uuid.UUID]*pathIndexEntry)
	x.byKey = make(map[localPathKey]map[*pathIndexEntry]struct{})
	x.byWritten = make(map[*table.Path]*pathIndexEntry)
}

func (x *pathIndex) len() int { return int(x.size.Load()) }

func (x *pathIndex) removeLocked(e *pathIndexEntry) {
	if x.byUUID[e.id] != e {
		return
	}
	delete(x.byUUID, e.id)
	delete(x.byWritten, e.written)
	if set := x.byKey[e.key]; set != nil {
		delete(set, e)
		if len(set) == 0 {
			delete(x.byKey, e.key)
		}
	}
	x.size.Store(int64(len(x.byUUID)))
	if len(x.byUUID) == 0 {
		x.resetLocked()
	}
}

func (x *pathIndex) remove(e *pathIndexEntry) {
	x.mu.Lock()
	defer x.mu.Unlock()
	x.removeLocked(e)
}

// add records the UUID of a path AddPath wrote and the table holds.
func (x *pathIndex) add(id uuid.UUID, written *table.Path, vrf string) {
	x.mu.Lock()
	defer x.mu.Unlock()
	e := &pathIndexEntry{id: id, key: localPathKeyOf(written), written: written, vrf: vrf}
	x.byUUID[id] = e
	set := x.byKey[e.key]
	if set == nil {
		set = make(map[*pathIndexEntry]struct{})
		x.byKey[e.key] = set
	}
	set[e] = struct{}{}
	x.byWritten[written] = e
	x.size.Store(int64(len(x.byUUID)))
}

func (x *pathIndex) lookup(id uuid.UUID) (*pathIndexEntry, bool) {
	x.mu.Lock()
	defer x.mu.Unlock()
	e, ok := x.byUUID[id]
	return e, ok
}

// watched reports whether a slot has an entry.
func (x *pathIndex) watched(key localPathKey) bool {
	if x.len() == 0 {
		return false
	}
	x.mu.Lock()
	defer x.mu.Unlock()
	return len(x.byKey[key]) > 0
}

// retire ends the entry of a path the global table removed. Versions of one
// written path share its entry, so the removed path is looked up by its
// root.
func (x *pathIndex) retire(removed *table.Path) {
	if removed == nil || x.len() == 0 {
		return
	}
	x.mu.Lock()
	defer x.mu.Unlock()
	if e, ok := x.byWritten[removed.Root()]; ok {
		x.removeLocked(e)
	}
}

// retireReplaced ends the entry of the path an update removed, unless the
// path that took its place is a version of the same written path, such as
// a zebra next-hop refresh or a policy copy.
func (x *pathIndex) retireReplaced(removed, installed *table.Path) {
	if removed == nil || x.len() == 0 {
		return
	}
	if installed != nil && !installed.IsWithdraw && installed.Root() == removed.Root() {
		return
	}
	x.retire(removed)
}

// holds reports whether the paths of a slot include a version of written.
func holds(held []*table.Path, written *table.Path) bool {
	for _, p := range held {
		if p.Root() == written {
			return true
		}
	}
	return false
}

// reconcile ends the entries of a slot whose path is not among the paths
// the slot holds now. It is linear in the size of the slot.
func (x *pathIndex) reconcile(key localPathKey, held []*table.Path) {
	x.mu.Lock()
	defer x.mu.Unlock()
	set := x.byKey[key]
	if len(set) == 0 {
		return
	}
	in := make(map[*table.Path]struct{}, len(held))
	for _, p := range held {
		in[p.Root()] = struct{}{}
	}
	for e := range set {
		if _, ok := in[e.written]; !ok {
			x.removeLocked(e)
		}
	}
}

// entries returns the entries the function selects.
func (x *pathIndex) entries(keep func(*pathIndexEntry) bool) []*pathIndexEntry {
	x.mu.Lock()
	defer x.mu.Unlock()
	var out []*pathIndexEntry
	for _, e := range x.byUUID {
		if keep(e) {
			out = append(out, e)
		}
	}
	return out
}

// slotPathsAt returns the paths the global table holds in the slot of key,
// reading the one destination of p. A slot has the full source, so it holds
// local paths and paths AddPath wrote with a peer identity; a path learned
// from a peer carries the local router ID that AddPath leaves unset.
func (s *BgpServer) slotPathsAt(p *table.Path, key localPathKey) []*table.Path {
	dest := s.globalRib.GetDestination(p)
	if dest == nil {
		return nil
	}
	var out []*table.Path
	for _, rp := range dest.GetAllKnownPathList() {
		if localPathKeyOf(rp) == key {
			out = append(out, rp)
		}
	}
	return out
}

// installedVersion returns the version of a written path the global table
// holds, or nil. It reads the one destination of the path.
func (s *BgpServer) installedVersion(written *table.Path) *table.Path {
	return s.globalRib.GetDestination(written).PathFromOrigin(written)
}

// propagateLocal writes local paths to the global table and keeps the index
// right. With an empty index and nothing to capture, the paths go to the
// table as one list, as before. Otherwise the paths whose slot has no entry
// still go as lists, in runs between the others, and only a path whose slot
// has an entry, or that AddPath captures, is written on its own with its
// slot read after it, so that every transition of a watched slot is seen.
// With capture, it returns each path when the table holds a version of it,
// or nil when it holds none (a withdrawal, or a path the import policy
// rejected).
func (s *BgpServer) propagateLocal(paths []*table.Path, capture bool) []*table.Path {
	if !capture && s.pathIndex.len() == 0 {
		s.propagateUpdate(nil, paths)
		return nil
	}
	var installed []*table.Path
	if capture {
		installed = make([]*table.Path, len(paths))
	}
	run := 0
	flush := func(end int) {
		if run < end {
			s.propagateUpdate(nil, paths[run:end])
		}
	}
	for i, p := range paths {
		key := localPathKeyOf(p)
		watched := s.pathIndex.watched(key)
		take := capture && !p.IsWithdraw
		if !watched && !take {
			continue
		}
		flush(i)
		run = i + 1
		s.propagateUpdate(nil, []*table.Path{p})
		after := s.slotPathsAt(p, key)
		if watched {
			s.pathIndex.reconcile(key, after)
		}
		if take && holds(after, p) {
			installed[i] = p
		}
	}
	flush(len(paths))
	return installed
}

// deleteByUUIDs withdraws the paths of the UUIDs and reports whether it
// found any. A UUID deletes the one path it names, the version of its
// written path the table holds, and no other path of its slot, even when
// no_implicit_withdraw lets several share it. A path that a tagged write
// owns is deleted only by the UUID of that write. A UUID whose path is gone
// ends and deletes nothing.
func (s *BgpServer) deleteByUUIDs(ids []uuid.UUID) bool {
	found := false
	for _, id := range ids {
		e, ok := s.pathIndex.lookup(id)
		if !ok {
			continue
		}
		rp := s.installedVersion(e.written)
		if rp == nil {
			s.pathIndex.remove(e)
			continue
		}
		found = true
		if !s.generations.owns(rp) {
			// The withdrawal shares the origin of rp, so the table removes
			// exactly rp, and propagateUpdate retires the entry.
			s.propagateUpdate(nil, []*table.Path{rp.Clone(true)})
			s.pathIndex.remove(e)
		}
	}
	return found
}

// reconcileVrf ends the entries of paths written to a VRF that the table no
// longer holds. DeleteVrf does not withdraw every family: a VPN FlowSpec
// path outlives its VRF, and keeps its UUID.
func (s *BgpServer) reconcileVrf(name string) {
	for _, e := range s.pathIndex.entries(func(e *pathIndexEntry) bool { return e.vrf == name }) {
		if s.installedVersion(e.written) == nil {
			s.pathIndex.remove(e)
		}
	}
}

// uuidsOf returns the UUIDs of the paths of a table selected from the global
// table, by written path (the root of the paths ListPath reads), for
// ListPath. With a filter the selection is small and is read; without, the
// index is.
func (s *BgpServer) uuidsOf(rib *table.Table, family bgp.Family, filtered bool) map[*table.Path]uuid.UUID {
	if s.pathIndex.len() == 0 || rib == nil {
		return nil
	}
	x := s.pathIndex
	ids := make(map[*table.Path]uuid.UUID)
	if filtered {
		var roots []*table.Path
		for _, d := range rib.GetDestinations() {
			for _, rp := range d.GetAllKnownPathList() {
				roots = append(roots, rp.Root())
			}
		}
		x.mu.Lock()
		defer x.mu.Unlock()
		for _, r := range roots {
			if e, ok := x.byWritten[r]; ok {
				ids[e.written] = e.id
			}
		}
		return ids
	}
	x.mu.Lock()
	defer x.mu.Unlock()
	for p, e := range x.byWritten {
		if e.key.family == family {
			ids[p] = e.id
		}
	}
	return ids
}
