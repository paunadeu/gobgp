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

// The generation fence.
//
// A route writer that steers traffic in generations tags each AddPath with
// the generation it belongs to. Before it withdraws a generation it fences
// it: FenceGeneration writes a durable tombstone, and from then on every
// tagged command for that generation is refused, whichever connection it
// comes from and however long it was delayed. The check runs in the
// management loop, in the same closure as the mutation, and the loop runs one
// operation at a time with shared.mu held for writing. So the fence is a
// single point in the order of mutations: every command the loop ran before
// it is applied and listed in the fence's answer, and every command after it
// is refused.
//
// The server also keeps what a writer needs when an answer is lost: a receipt
// per applied command, keyed by command ID, and the UUIDs each generation was
// given with the full route key of each. A UUID is indexed by route key, so a
// tagged delete by UUID and the lookups below never walk the table.
//
// Receipts and UUIDs live in memory. Local paths do not survive a restart,
// and the incarnation, a random ID taken at StartBgp, tells a reader that the
// speaker it is asking is not the one that answered before. Tombstones are
// durable (see internal/pkg/genfence) and are restored before the first write
// is accepted, because the store is opened by StartBgp and every write waits
// for StartBgp.
//
// Untagged writes behave as before, with two additions. They keep the index
// right: an untagged write that takes or deletes the route key of a tagged
// path marks that path REPLACED or DELETED. And a server can be told to
// refuse untagged writes to some families, so that only fenced writers reach
// them.

import (
	"container/list"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	tspb "google.golang.org/protobuf/types/known/timestamppb"

	"github.com/osrg/gobgp/v4/api"
	"github.com/osrg/gobgp/v4/internal/pkg/genfence"
	"github.com/osrg/gobgp/v4/internal/pkg/table"
	"github.com/osrg/gobgp/v4/pkg/apiutil"
	"github.com/osrg/gobgp/v4/pkg/packet/bgp"
)

// GenerationFenceConfig configures the generation fence. Without Dir the
// fence is off: tagged writes and FenceGeneration are refused, and untagged
// writes behave as they always did.
type GenerationFenceConfig struct {
	// Dir holds the tombstone store. Required to enable the fence.
	Dir string
	// RequireTagFamilies are the families an untagged AddPath,
	// AddPathStream or DeletePath may not write. A delete of every local
	// path that would reach one of them is refused too.
	RequireTagFamilies []bgp.Family
	// SpeakerID, when set, is the only speaker a command may name.
	SpeakerID string
	// TombstoneCacheSize bounds the tombstone lookups kept in memory.
	// Default genfence.DefaultCacheSize.
	TombstoneCacheSize int
	// SettledGenerationRetention bounds the generations kept in memory
	// that own no current path. The oldest is dropped first, with its
	// receipts. A generation that owns a current path is never dropped.
	// Default DefaultSettledGenerationRetention.
	SettledGenerationRetention int
}

// DefaultSettledGenerationRetention is the default of
// GenerationFenceConfig.SettledGenerationRetention.
const DefaultSettledGenerationRetention = 65536

// MaxFenceGenerations is the most generations one FenceGenerations may name.
const MaxFenceGenerations = 1024

// MaxLookupDestinations is the most keys one LookupDestinations may name.
const MaxLookupDestinations = 10000

// GenerationFenceOption enables the generation fence.
func GenerationFenceOption(c GenerationFenceConfig) ServerOption {
	return func(o *options) {
		o.generationFence = &c
	}
}

var (
	// ErrGenerationFenced: the generation is fenced, nothing was applied.
	ErrGenerationFenced = errors.New("generation is fenced")
	// ErrGenerationFenceDisabled: the server has no fence directory.
	ErrGenerationFenceDisabled = errors.New("generation fence is not configured")
	// ErrGenerationTagRequired: an untagged write to a family that needs a tag.
	ErrGenerationTagRequired = errors.New("a generation tag is required to write this family")
	// ErrGenerationCommandConflict: the command ID has a receipt for another command.
	ErrGenerationCommandConflict = errors.New("command ID was used for another command")
	// ErrGenerationNotOwner: the UUID belongs to another generation.
	ErrGenerationNotOwner = errors.New("path belongs to another generation")
	// ErrGenerationSpeakerMismatch: the command names another speaker.
	ErrGenerationSpeakerMismatch = errors.New("command names another speaker")
	// ErrGenerationInvalid: the tag or the request is malformed.
	ErrGenerationInvalid = errors.New("invalid generation command")
	// ErrGenerationUnknownUUID: no tagged path of this incarnation has the UUID.
	ErrGenerationUnknownUUID = errors.New("no generation path of this incarnation has the UUID")
	// ErrGenerationStore: the tombstone store could not be read or written.
	ErrGenerationStore = errors.New("generation fence store failed")
)

// generationError carries a gRPC code, so that a client can tell a refusal
// from a transport failure, and unwraps to one of the errors above.
type generationError struct {
	code   codes.Code
	base   error
	detail string
}

func (e *generationError) Error() string {
	if e.detail == "" {
		return e.base.Error()
	}
	return e.base.Error() + ": " + e.detail
}

func (e *generationError) Unwrap() error { return e.base }

func (e *generationError) GRPCStatus() *status.Status { return status.New(e.code, e.Error()) }

func genErr(code codes.Code, base error, format string, args ...any) error {
	return &generationError{code: code, base: base, detail: fmt.Sprintf(format, args...)}
}

// routeKey is the slot a local path takes in the global RIB: one path per
// family, NLRI and path identifier.
type routeKey struct {
	family bgp.Family
	prefix string
	id     uint32
}

func routeKeyOf(p *table.Path) routeKey {
	return routeKey{family: p.GetFamily(), prefix: p.GetNlri().String(), id: p.RemoteID()}
}

type genKey struct {
	tenant     string
	generation uint64
}

type genPath struct {
	id            uuid.UUID
	rec           *genRecord
	key           routeKey
	vrf           string
	path          *table.Path
	commandID     string
	speaker       string
	state         api.GenerationPathState
	replacedBy    uuid.UUID
	replacedByTag *apiutil.GenerationTag
	addedAt       time.Time
	endedAt       time.Time
}

type genReceipt struct {
	commandID     string
	speaker       string
	kind          api.GenerationCommandKind
	appliedAt     time.Time
	uuids         []uuid.UUID
	fingerprint   [sha256.Size]byte
	deleteOutcome api.GenerationDeleteOutcome
}

type genRecord struct {
	key               genKey
	receipts          []*genReceipt
	byCommand         map[string]*genReceipt
	paths             []*genPath
	current           int
	refusedAfterFence uint64
	settled           *list.Element
}

// generationIndex is touched only from management operations, which run
// one at a time with shared.mu held, so it has no lock of its own.
type generationIndex struct {
	cfg         *GenerationFenceConfig
	requireTag  map[bgp.Family]struct{}
	store       *genfence.Store
	incarnation uuid.UUID
	records     map[genKey]*genRecord
	byUUID      map[uuid.UUID]*genPath
	byRoute     map[routeKey]*genPath
	settled     *list.List
	retention   int
}

func newGenerationIndex(cfg *GenerationFenceConfig) *generationIndex {
	idx := &generationIndex{
		cfg:        cfg,
		requireTag: make(map[bgp.Family]struct{}),
		records:    make(map[genKey]*genRecord),
		byUUID:     make(map[uuid.UUID]*genPath),
		byRoute:    make(map[routeKey]*genPath),
		settled:    list.New(),
		retention:  DefaultSettledGenerationRetention,
	}
	if cfg != nil {
		for _, f := range cfg.RequireTagFamilies {
			idx.requireTag[f] = struct{}{}
		}
		if cfg.SettledGenerationRetention > 0 {
			idx.retention = cfg.SettledGenerationRetention
		}
	}
	return idx
}

// open starts a new incarnation and opens the tombstone store unless it is
// open. StartBgp calls it before the server accepts any write. StartBgp
// builds a new, empty RIB, so the receipts and UUIDs of an earlier start are
// dropped with it.
func (idx *generationIndex) open() (opened bool, err error) {
	idx.incarnation = uuid.New()
	idx.records = make(map[genKey]*genRecord)
	idx.byUUID = make(map[uuid.UUID]*genPath)
	idx.byRoute = make(map[routeKey]*genPath)
	idx.settled = list.New()
	if idx.cfg == nil || idx.store != nil {
		return false, nil
	}
	if idx.cfg.Dir == "" {
		if len(idx.cfg.RequireTagFamilies) > 0 || idx.cfg.SpeakerID != "" {
			return false, errors.New("the generation fence needs a directory")
		}
		return false, nil
	}
	if len(idx.cfg.SpeakerID) > genfence.MaxField {
		return false, fmt.Errorf("generation fence speaker ID is longer than %d bytes", genfence.MaxField)
	}
	st, err := genfence.Open(idx.cfg.Dir, idx.cfg.TombstoneCacheSize)
	if err != nil {
		return false, err
	}
	idx.store = st
	return true, nil
}

func (idx *generationIndex) close() error {
	if idx.store == nil {
		return nil
	}
	err := idx.store.Close()
	idx.store = nil
	return err
}

func (idx *generationIndex) requiresTag(f bgp.Family) bool {
	_, ok := idx.requireTag[f]
	return ok
}

func (idx *generationIndex) enabled() error {
	if idx.store == nil {
		return genErr(codes.FailedPrecondition, ErrGenerationFenceDisabled, "start the server with a generation fence directory")
	}
	return nil
}

func validateGeneration(tenant string, generation uint64, speaker string) error {
	if tenant == "" || len(tenant) > genfence.MaxField {
		return genErr(codes.InvalidArgument, ErrGenerationInvalid, "tenant must be 1 to %d bytes", genfence.MaxField)
	}
	if generation == 0 {
		return genErr(codes.InvalidArgument, ErrGenerationInvalid, "generation must not be 0")
	}
	if len(speaker) > genfence.MaxField {
		return genErr(codes.InvalidArgument, ErrGenerationInvalid, "speaker must be at most %d bytes", genfence.MaxField)
	}
	return nil
}

func (idx *generationIndex) checkSpeaker(speaker string) error {
	if idx.cfg != nil && idx.cfg.SpeakerID != "" && speaker != idx.cfg.SpeakerID {
		return genErr(codes.FailedPrecondition, ErrGenerationSpeakerMismatch, "this speaker is %q, the command names %q", idx.cfg.SpeakerID, speaker)
	}
	return nil
}

func (idx *generationIndex) checkTag(t *apiutil.GenerationTag) error {
	if err := idx.enabled(); err != nil {
		return err
	}
	if err := validateGeneration(t.Tenant, t.Generation, t.Speaker); err != nil {
		return err
	}
	if t.CommandID == "" || len(t.CommandID) > genfence.MaxField {
		return genErr(codes.InvalidArgument, ErrGenerationInvalid, "command ID must be 1 to %d bytes", genfence.MaxField)
	}
	if t.Speaker == "" {
		return genErr(codes.InvalidArgument, ErrGenerationInvalid, "speaker must be set")
	}
	return idx.checkSpeaker(t.Speaker)
}

// fenced reads the tombstone. A read that fails refuses the command: a
// generation whose state is unknown is treated as fenced.
func (idx *generationIndex) fenced(k genKey) (genfence.Tombstone, bool, error) {
	t, ok, err := idx.store.Lookup(k.tenant, k.generation)
	if err != nil {
		return genfence.Tombstone{}, false, genErr(codes.Unavailable, ErrGenerationStore, "%v", err)
	}
	return t, ok, nil
}

func (idx *generationIndex) record(k genKey) *genRecord {
	rec, ok := idx.records[k]
	if !ok {
		rec = &genRecord{key: k, byCommand: make(map[string]*genReceipt)}
		idx.records[k] = rec
	}
	return rec
}

// settle files a record that owns no current path among the settled ones,
// or takes it out once it owns one again, and drops the oldest settled
// records beyond the retention.
// unsettle takes a record out of the settled ones while a command works on
// it, so that no eviction can drop it halfway.
func (idx *generationIndex) unsettle(rec *genRecord) {
	if rec.settled != nil {
		idx.settled.Remove(rec.settled)
		rec.settled = nil
	}
}

func (idx *generationIndex) settle(rec *genRecord) {
	if rec.current > 0 {
		if rec.settled != nil {
			idx.settled.Remove(rec.settled)
			rec.settled = nil
		}
		return
	}
	if rec.settled != nil {
		idx.settled.MoveToFront(rec.settled)
	} else {
		rec.settled = idx.settled.PushFront(rec)
	}
	for idx.settled.Len() > idx.retention {
		old := idx.settled.Back().Value.(*genRecord)
		idx.settled.Remove(old.settled)
		old.settled = nil
		for _, p := range old.paths {
			delete(idx.byUUID, p.id)
		}
		delete(idx.records, old.key)
	}
}

// endRoute marks the tagged path at k, if there is one, as ended by
// another write.
func (idx *generationIndex) endRoute(k routeKey, state api.GenerationPathState, by uuid.UUID, byTag *apiutil.GenerationTag, now time.Time) {
	p, ok := idx.byRoute[k]
	if !ok {
		return
	}
	delete(idx.byRoute, k)
	p.state = state
	p.replacedBy = by
	p.replacedByTag = byTag
	p.endedAt = now
	p.rec.current--
	idx.settle(p.rec)
}

// owns reports whether rp, a path the global RIB holds, is a version of the
// path a current tagged write put there.
func (idx *generationIndex) owns(rp *table.Path) bool {
	gp, ok := idx.byRoute[routeKeyOf(rp)]
	return ok && gp.path == rp.Root()
}

// endFamilies marks every tagged path of the families as deleted.
func (idx *generationIndex) endFamilies(families []bgp.Family, now time.Time) {
	for k := range idx.byRoute {
		if slices.Contains(families, k.family) {
			idx.endRoute(k, api.GenerationPathState_GENERATION_PATH_STATE_DELETED, uuid.Nil, nil, now)
		}
	}
}

func fingerprint(kind api.GenerationCommandKind, vrfID string, paths []*table.Path, ids ...uuid.UUID) [sha256.Size]byte {
	h := sha256.New()
	var b [8]byte
	put := func(v uint64) {
		binary.BigEndian.PutUint64(b[:], v)
		h.Write(b[:])
	}
	putBytes := func(v []byte) {
		put(uint64(len(v)))
		h.Write(v)
	}
	put(uint64(kind))
	putBytes([]byte(vrfID))
	for _, id := range ids {
		h.Write(id[:])
	}
	put(uint64(len(paths)))
	for _, p := range paths {
		put(uint64(p.GetFamily()))
		put(uint64(p.RemoteID()))
		flags := uint64(0)
		if p.IsWithdraw {
			flags |= 1
		}
		if p.NoImplicitWithdraw() {
			flags |= 2
		}
		if p.IsFromExternal() {
			flags |= 4
		}
		put(flags)
		if src := p.GetSource(); src != nil {
			put(uint64(src.AS))
			putBytes(src.ID.AsSlice())
		}
		nlri, _ := p.GetNlri().Serialize()
		putBytes(nlri)
		attrs := p.GetPathAttrs()
		put(uint64(len(attrs)))
		for _, a := range attrs {
			v, _ := a.Serialize()
			putBytes(v)
		}
	}
	var sum [sha256.Size]byte
	copy(sum[:], h.Sum(nil))
	return sum
}

// localPathAt returns the local path the global RIB holds at the route key
// of p, or nil. It reads one destination.
func (s *BgpServer) localPathAt(p *table.Path, id uint32) *table.Path {
	dest := s.globalRib.GetDestination(p)
	if dest == nil {
		return nil
	}
	for _, rp := range dest.GetAllKnownPathList() {
		if rp.IsLocal() && rp.RemoteID() == id {
			return rp
		}
	}
	return nil
}

// addTaggedPaths applies one tagged command. It runs in the management loop.
// Nothing is applied unless every path is valid.
func (s *BgpServer) addTaggedPaths(vrfID string, paths []*table.Path, tag *apiutil.GenerationTag, kind api.GenerationCommandKind) ([]apiutil.AddPathResponse, error) {
	idx := s.generations
	if err := idx.checkTag(tag); err != nil {
		return nil, err
	}
	if len(paths) == 0 {
		return nil, genErr(codes.InvalidArgument, ErrGenerationInvalid, "no path")
	}
	for _, p := range paths {
		if p == nil {
			return nil, genErr(codes.InvalidArgument, ErrGenerationInvalid, "path is nil")
		}
		if p.IsWithdraw {
			return nil, genErr(codes.InvalidArgument, ErrGenerationInvalid, "a tagged write cannot withdraw; delete the path by UUID")
		}
	}
	fp := fingerprint(kind, vrfID, paths)
	k := genKey{tag.Tenant, tag.Generation}

	// The fence check. The mutation below runs in the same closure, so no
	// fence can come between them.
	if _, fenced, err := idx.fenced(k); err != nil {
		return nil, err
	} else if fenced {
		rec := idx.record(k)
		rec.refusedAfterFence++
		idx.settle(rec)
		return nil, genErr(codes.FailedPrecondition, ErrGenerationFenced, "tenant %q generation %d", tag.Tenant, tag.Generation)
	}

	if rec, ok := idx.records[k]; ok {
		if r, ok := rec.byCommand[tag.CommandID]; ok {
			if r.kind != kind || r.fingerprint != fp {
				return nil, genErr(codes.AlreadyExists, ErrGenerationCommandConflict, "command %q", tag.CommandID)
			}
			resps := make([]apiutil.AddPathResponse, len(r.uuids))
			for i, id := range r.uuids {
				resps[i] = apiutil.AddPathResponse{UUID: id, Replayed: true}
			}
			return resps, nil
		}
	}

	if err := s.fixupApiPath(vrfID, paths); err != nil {
		return nil, genErr(codes.InvalidArgument, ErrGenerationInvalid, "%v", err)
	}
	ids := make([]uuid.UUID, len(paths))
	for i := range ids {
		id, err := uuid.NewRandom()
		if err != nil {
			return nil, err
		}
		ids[i] = id
	}

	if s.zclient != nil {
		s.zclient.cacheLock.Lock()
		s.zclient.nexthopCache.applyToNewPathList(paths)
		s.zclient.cacheLock.Unlock()
	}
	s.propagateLocal(paths, false)

	now := time.Now().UTC()
	rec := idx.record(k)
	idx.unsettle(rec)
	resps := make([]apiutil.AddPathResponse, len(paths))
	for i, p := range paths {
		rk := routeKeyOf(p)
		rec.current++
		idx.endRoute(rk, api.GenerationPathState_GENERATION_PATH_STATE_REPLACED, ids[i], tag, now)
		gp := &genPath{
			id:        ids[i],
			rec:       rec,
			key:       rk,
			vrf:       vrfID,
			path:      p,
			commandID: tag.CommandID,
			speaker:   tag.Speaker,
			state:     api.GenerationPathState_GENERATION_PATH_STATE_CURRENT,
			addedAt:   now,
		}
		idx.byRoute[rk] = gp
		idx.byUUID[gp.id] = gp
		rec.paths = append(rec.paths, gp)
		resps[i] = apiutil.AddPathResponse{UUID: ids[i]}
	}
	r := &genReceipt{
		commandID:   tag.CommandID,
		speaker:     tag.Speaker,
		kind:        kind,
		appliedAt:   now,
		uuids:       ids,
		fingerprint: fp,
	}
	rec.receipts = append(rec.receipts, r)
	rec.byCommand[r.commandID] = r
	idx.settle(rec)
	return resps, nil
}

// withdrawGenerationPath withdraws a current tagged path and marks it
// deleted. It reads one destination.
func (s *BgpServer) withdrawGenerationPath(gp *genPath, now time.Time) {
	if rp := s.localPathAt(gp.path, gp.key.id); rp != nil {
		s.propagateLocal([]*table.Path{rp.Clone(true)}, false)
	}
	s.generations.endRoute(gp.key, api.GenerationPathState_GENERATION_PATH_STATE_DELETED, uuid.Nil, nil, now)
}

// deleteTaggedPath applies one tagged delete. It runs in the management
// loop. It is not fenced: deleting a fenced generation's paths is the point
// of the fence.
func (s *BgpServer) deleteTaggedPath(req apiutil.DeletePathRequest) (api.GenerationDeleteOutcome, bool, error) {
	none := api.GenerationDeleteOutcome_GENERATION_DELETE_OUTCOME_UNSPECIFIED
	idx := s.generations
	tag := req.Generation
	if err := idx.checkTag(tag); err != nil {
		return none, false, err
	}
	if len(req.UUIDs) != 1 || req.DeleteAll || len(req.Paths) > 0 {
		return none, false, genErr(codes.InvalidArgument, ErrGenerationInvalid, "a tagged delete names exactly one UUID")
	}
	id := req.UUIDs[0]
	kind := api.GenerationCommandKind_GENERATION_COMMAND_KIND_DELETE_PATH
	fp := fingerprint(kind, req.VRFID, nil, id)
	k := genKey{tag.Tenant, tag.Generation}
	if rec, ok := idx.records[k]; ok {
		if r, ok := rec.byCommand[tag.CommandID]; ok {
			if r.kind != kind || r.fingerprint != fp {
				return none, false, genErr(codes.AlreadyExists, ErrGenerationCommandConflict, "command %q", tag.CommandID)
			}
			return r.deleteOutcome, true, nil
		}
	}
	gp, ok := idx.byUUID[id]
	if !ok {
		return none, false, genErr(codes.NotFound, ErrGenerationUnknownUUID, "%s", id)
	}
	if gp.rec.key != k {
		return none, false, genErr(codes.FailedPrecondition, ErrGenerationNotOwner, "%s belongs to tenant %q generation %d", id, gp.rec.key.tenant, gp.rec.key.generation)
	}
	now := time.Now().UTC()
	rec := gp.rec
	idx.unsettle(rec)
	var outcome api.GenerationDeleteOutcome
	switch gp.state {
	case api.GenerationPathState_GENERATION_PATH_STATE_CURRENT:
		s.withdrawGenerationPath(gp, now)
		outcome = api.GenerationDeleteOutcome_GENERATION_DELETE_OUTCOME_DELETED
	case api.GenerationPathState_GENERATION_PATH_STATE_REPLACED:
		outcome = api.GenerationDeleteOutcome_GENERATION_DELETE_OUTCOME_REPLACED
	default:
		outcome = api.GenerationDeleteOutcome_GENERATION_DELETE_OUTCOME_ALREADY_DELETED
	}
	r := &genReceipt{
		commandID:     tag.CommandID,
		speaker:       tag.Speaker,
		kind:          kind,
		appliedAt:     now,
		uuids:         []uuid.UUID{id},
		fingerprint:   fp,
		deleteOutcome: outcome,
	}
	rec.receipts = append(rec.receipts, r)
	rec.byCommand[r.commandID] = r
	idx.settle(rec)
	return outcome, false, nil
}

// deleteUntaggedUUIDs deletes, by the index, the paths of the UUIDs that a
// tagged AddPath gave, and returns the UUIDs it does not know, for the
// legacy lookup.
func (s *BgpServer) deleteUntaggedUUIDs(ids []uuid.UUID) (found bool, rest []uuid.UUID, err error) {
	idx := s.generations
	var hits []*genPath
	for _, id := range ids {
		gp, ok := idx.byUUID[id]
		if !ok {
			rest = append(rest, id)
			continue
		}
		if idx.requiresTag(gp.key.family) {
			return false, nil, genErr(codes.FailedPrecondition, ErrGenerationTagRequired, "%s", gp.key.family)
		}
		if gp.state == api.GenerationPathState_GENERATION_PATH_STATE_CURRENT {
			hits = append(hits, gp)
		}
	}
	now := time.Now().UTC()
	for _, gp := range hits {
		s.withdrawGenerationPath(gp, now)
	}
	return len(hits) > 0, rest, nil
}

func (s *BgpServer) checkUntaggedFamilies(families ...bgp.Family) error {
	for _, f := range families {
		if s.generations.requiresTag(f) {
			return genErr(codes.FailedPrecondition, ErrGenerationTagRequired, "%s", f)
		}
	}
	return nil
}

func (s *BgpServer) checkUntaggedPaths(paths []*table.Path) error {
	for _, p := range paths {
		if err := s.checkUntaggedFamilies(p.GetFamily()); err != nil {
			return err
		}
	}
	return nil
}

// noteUntaggedWrites keeps the index right after untagged paths were
// written: a tagged path whose route key they took is replaced, or deleted
// when the write was a withdrawal.
func (s *BgpServer) noteUntaggedWrites(paths []*table.Path, by uuid.UUID) {
	if len(s.generations.byRoute) == 0 {
		return
	}
	now := time.Now().UTC()
	for _, p := range paths {
		state := api.GenerationPathState_GENERATION_PATH_STATE_REPLACED
		if p.IsWithdraw {
			state = api.GenerationPathState_GENERATION_PATH_STATE_DELETED
		}
		s.generations.endRoute(routeKeyOf(p), state, by, nil, now)
	}
}

// generationIncarnation reads the incarnation, which StartBgp sets in the
// management loop.
func (s *BgpServer) generationIncarnation() []byte {
	s.shared.mu.RLock()
	defer s.shared.mu.RUnlock()
	return uuidBytes(s.generations.incarnation)
}

// generationSpeakerID is the configured speaker ID, empty when none is set.
// The configuration does not change after NewBgpServer.
func (s *BgpServer) generationSpeakerID() string {
	if c := s.generations.cfg; c != nil {
		return c.SpeakerID
	}
	return ""
}

func timestampOrNil(t time.Time) *tspb.Timestamp {
	if t.IsZero() {
		return nil
	}
	return tspb.New(t)
}

func uuidBytes(id uuid.UUID) []byte {
	if id == uuid.Nil {
		return nil
	}
	b := make([]byte, len(id))
	copy(b, id[:])
	return b
}

// generationStatus builds the status of a generation. It runs in the
// management loop.
func (s *BgpServer) generationStatus(k genKey) (*api.GenerationStatus, error) {
	idx := s.generations
	t, fenced, err := idx.fenced(k)
	if err != nil {
		return nil, err
	}
	st := &api.GenerationStatus{
		Tenant:      k.tenant,
		Generation:  k.generation,
		Incarnation: uuidBytes(idx.incarnation),
		SpeakerId:   s.generationSpeakerID(),
		State:       api.GenerationState_GENERATION_STATE_UNKNOWN,
	}
	rec := idx.records[k]
	switch {
	case fenced:
		st.State = api.GenerationState_GENERATION_STATE_FENCED
		st.FencedAt = tspb.New(t.FencedAt)
	case rec != nil:
		st.State = api.GenerationState_GENERATION_STATE_LIVE
	}
	if rec == nil {
		return st, nil
	}
	st.RefusedAfterFence = rec.refusedAfterFence
	st.CurrentPaths = uint64(rec.current)
	for _, r := range rec.receipts {
		ar := &api.GenerationReceipt{
			CommandId:     r.commandID,
			Speaker:       r.speaker,
			Kind:          r.kind,
			AppliedAt:     tspb.New(r.appliedAt),
			DeleteOutcome: r.deleteOutcome,
		}
		for _, id := range r.uuids {
			ar.Uuids = append(ar.Uuids, uuidBytes(id))
		}
		st.Receipts = append(st.Receipts, ar)
	}
	for _, gp := range rec.paths {
		nlri, _ := apiutil.MarshalNLRI(gp.path.GetNlri())
		ap := &api.GenerationPath{
			Uuid:           uuidBytes(gp.id),
			Family:         &api.Family{Afi: api.Family_Afi(gp.key.family.Afi()), Safi: api.Family_Safi(gp.key.family.Safi())},
			Nlri:           nlri,
			Identifier:     gp.key.id,
			VrfId:          gp.vrf,
			CommandId:      gp.commandID,
			State:          gp.state,
			ReplacedByUuid: uuidBytes(gp.replacedBy),
			ReplacedBy:     gp.replacedByTag.ToAPI(),
			Path:           toPathApi(toPathApiUtil(gp.path), false, false, false),
			AddedAt:        timestampOrNil(gp.addedAt),
			EndedAt:        timestampOrNil(gp.endedAt),
		}
		ap.Present = gp.state == api.GenerationPathState_GENERATION_PATH_STATE_CURRENT && s.localPathAt(gp.path, gp.key.id) != nil
		st.Paths = append(st.Paths, ap)
	}
	return st, nil
}

// FenceGeneration fences a generation: when it returns without error the
// tombstone is on disk, every command of the generation the server ran
// before it is in the returned status, and every later one is refused. It is
// idempotent.
func (s *BgpServer) FenceGeneration(ctx context.Context, r *api.FenceGenerationRequest) (*api.FenceGenerationResponse, error) {
	if r == nil {
		return nil, genErr(codes.InvalidArgument, ErrGenerationInvalid, "nil request")
	}
	var resp *api.FenceGenerationResponse
	err := s.mgmtOperationWithContext(ctx, func() error {
		idx := s.generations
		if err := idx.enabled(); err != nil {
			return err
		}
		if err := validateGeneration(r.Tenant, r.Generation, r.Speaker); err != nil {
			return err
		}
		if err := idx.checkSpeaker(r.Speaker); err != nil {
			return err
		}
		_, newly, err := idx.store.Fence(r.Tenant, r.Generation, r.Speaker, time.Now().UTC())
		if err != nil {
			return genErr(codes.Unavailable, ErrGenerationStore, "%v", err)
		}
		st, err := s.generationStatus(genKey{r.Tenant, r.Generation})
		if err != nil {
			return err
		}
		resp = &api.FenceGenerationResponse{Status: st, NewlyFenced: newly}
		return nil
	}, true)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// FenceGenerations fences several generations of a tenant in one management
// operation and one synced write of the tombstone store. For each of them it
// means what FenceGeneration means, and it is atomic: when it fails, none of
// them was fenced by it. A generation listed twice is fenced and reported
// once; the results follow the order of first mention.
func (s *BgpServer) FenceGenerations(ctx context.Context, r *api.FenceGenerationsRequest) (*api.FenceGenerationsResponse, error) {
	if r == nil {
		return nil, genErr(codes.InvalidArgument, ErrGenerationInvalid, "nil request")
	}
	gens, err := distinctGenerations(r.Tenant, r.Generations, r.Speaker)
	if err != nil {
		return nil, err
	}
	var resp *api.FenceGenerationsResponse
	err = s.mgmtOperationWithContext(ctx, func() error {
		idx := s.generations
		if err := idx.enabled(); err != nil {
			return err
		}
		if err := idx.checkSpeaker(r.Speaker); err != nil {
			return err
		}
		_, newly, err := idx.store.FenceMany(r.Tenant, gens, r.Speaker, time.Now().UTC())
		if err != nil {
			return genErr(codes.Unavailable, ErrGenerationStore, "%v", err)
		}
		out := &api.FenceGenerationsResponse{Results: make([]*api.FenceGenerationResponse, len(gens))}
		for i, g := range gens {
			st, err := s.generationStatus(genKey{r.Tenant, g})
			if err != nil {
				return err
			}
			out.Results[i] = &api.FenceGenerationResponse{Status: st, NewlyFenced: newly[i]}
		}
		resp = out
		return nil
	}, true)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// GetGeneration returns a generation's status, receipts and UUIDs. It reads
// the RIB at the route key of each UUID and nowhere else.
func (s *BgpServer) GetGeneration(ctx context.Context, r *api.GetGenerationRequest) (*api.GetGenerationResponse, error) {
	if r == nil {
		return nil, genErr(codes.InvalidArgument, ErrGenerationInvalid, "nil request")
	}
	var resp *api.GetGenerationResponse
	err := s.mgmtOperationWithContext(ctx, func() error {
		if err := s.generations.enabled(); err != nil {
			return err
		}
		if err := validateGeneration(r.Tenant, r.Generation, ""); err != nil {
			return err
		}
		st, err := s.generationStatus(genKey{r.Tenant, r.Generation})
		if err != nil {
			return err
		}
		resp = &api.GetGenerationResponse{Status: st}
		return nil
	}, true)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// LookupDestinations reads the given destinations of the global RIB, each
// by its key, and says which generation owns each local path. It works for
// every family, and it runs in the management loop, so it sees every
// mutation acknowledged before it.
func (s *BgpServer) LookupDestinations(ctx context.Context, r *api.LookupDestinationsRequest) (*api.LookupDestinationsResponse, error) {
	if r == nil {
		return nil, genErr(codes.InvalidArgument, ErrGenerationInvalid, "nil request")
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
	resp := &api.LookupDestinationsResponse{}
	err := s.mgmtOperationWithContext(ctx, func() error {
		idx := s.generations
		resp.Incarnation = uuidBytes(idx.incarnation)
		resp.SpeakerId = s.generationSpeakerID()
		for _, k := range keys {
			d := &api.DestinationLookup{Key: k.key}
			resp.Destinations = append(resp.Destinations, d)
			tbl, ok := s.globalRib.GetTable(k.family)
			if !ok {
				continue
			}
			dest := tbl.GetDestination(k.nlri)
			if dest == nil {
				continue
			}
			prefix := k.nlri.String()
			for _, p := range dest.GetAllKnownPathList() {
				op := &api.OwnedPath{Path: toPathApi(toPathApiUtil(p), false, false, false)}
				if p.IsLocal() {
					if gp, ok := idx.byRoute[routeKey{family: k.family, prefix: prefix, id: p.RemoteID()}]; ok {
						op.Uuid = uuidBytes(gp.id)
						op.Owner = &api.GenerationTag{
							Tenant:     gp.rec.key.tenant,
							Generation: gp.rec.key.generation,
							CommandId:  gp.commandID,
							Speaker:    gp.speaker,
						}
					}
				}
				d.Paths = append(d.Paths, op)
			}
		}
		return nil
	}, true)
	if err != nil {
		return nil, err
	}
	return resp, nil
}
