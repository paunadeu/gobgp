# Owned paths: design of a set of generic API primitives

Status: revision 2, for review before any code of B, C, D and F. A and E are
implemented on their own branches (see below). Base: osrg/gobgp master
`42c7736d` ("server: do not apply a path request whose gRPC context has
ended", #3912).

Every change here must be worth merging on its own, for any GoBGP client.
The last part maps a client that steers routes in generations onto them, and
lists what that client does itself.

Changes from revision 1 are listed at the end.

## Problems, as any client sees them

1. **DeletePath by UUID walks the table.** For each UUID it walks
   `GetPathList` of every family. On a table of 1M local paths one delete
   took 881 ms (gates01, AMD Ryzen 9 5950X, master `42c7736d`), holding the
   management loop that long.
2. **The UUID map is keyed by path identifier and prefix string, without
   the family.** When a labeled-unicast AddPath writes the prefix of an
   existing unicast path, the unicast UUID is overwritten and no longer
   deletes anything, while the surviving labeled UUID deletes the local
   paths of every family with that prefix and path identifier. A delete of
   all local paths of one family clears the whole map, so the UUIDs of other
   families stop working. A path replaced by AddPathStream keeps its old UUID,
   and a delete by that stale UUID removes the new path.
3. **A client cannot retry a write safely.** #3912 makes "timed out before
   the loop ran it" mean "not applied". A request the loop has started runs
   to completion, and the server's copy of the deadline starts when the
   request arrives, so it ends a little after the client's. A client that
   sees DEADLINE_EXCEEDED, or loses the connection, cannot tell whether its
   AddPath was applied; a retry may add the path a second time under a
   second UUID.
4. **A controller cannot stop writing for good.** A controller that is
   replaced (failover, upgrade, a new job) can still have AddPath requests in
   flight on other connections, queued behind a long operation. Nothing lets
   its successor say "from now on, nothing from the old one", and know which
   of the old one's paths are in the table at that point.
5. **Cleaning up after a controller needs its paths.** To withdraw what a
   controller wrote, a client must have kept every UUID, or list whole
   families and guess.
6. **ListPath can filter only some families by prefix.** `Table.Select`
   handles IPv4/IPv6 unicast, VPN, EVPN (route type) and MUP; any other
   family, labeled unicast among them, fails with "route filtering is not
   supported for this family", so reading one destination means streaming
   the whole family.

## The series

Each line is one pull request, small enough to review on its own.

| # | Branch | Title | Stacks on | State |
| --- | --- | --- | --- | --- |
| A | `upstream/uuid-index` | server: index local paths for DeletePath by UUID | master | implemented |
| E | `upstream/listpath-exact` | server, table: exact ListPath lookups by NLRI, and prefix lookups for labeled unicast | master | implemented |
| X | `upstream/expected-incarnation` | server: report a server incarnation and accept it as a precondition | master | design |
| B | `upstream/request-id` | server: an optional request ID makes path writes safe to retry | master | design |
| C1 | `upstream/owned-paths` | server: optional path owners, listed through an indexed ListPath filter | A | design |
| C2 | `upstream/owner-cleanup` | server: delete the paths of owners in bounded steps | C1 | design |
| D | `upstream/close-owner` | server: close owners to refuse their later writes | C1 (C2 for `only_closed`) | design |
| F | (later) | server: refuse unowned writes to chosen families | C1, D | after ownership settles |

A, E, X and B are independent of each other and of the rest. A and B both
touch `addPath` and `deletePath`; whichever lands second rebases without a
semantic conflict.

Conventions for every PR: native `apiutil` request and result types with a
gRPC adapter over them, a CLI equivalent, a `ServerOption` beside every
gobgpd flag, enums starting at `*_UNSPECIFIED`, and the gRPC status code of
every refusal documented in the proto comment.

---

### A. Index local paths for DeletePath by UUID (implemented)

**Problem.** 1 and 2.

**Slot identity.** A local path's slot is the family, the destination
identity the table itself keys by (`table.DestinationIdentity`, the bytes
`AddrPrefixOnlyCompare` compares: address and length for unicast, RD and
prefix for VPN, the full serialized NLRI for BGP-LS, the string form
otherwise, so a label is not part of a labeled-unicast key), the source (AS,
ID, local ID, address, as `PeerInfo.Equal` compares them) and the path
identifier. Two paths with the same slot replace each other, as
`destination.implicitWithdraw` does, except that a path added with
`no_implicit_withdraw` is never replaced and coexists with later ones.

**Lifecycle.** The index is touched only inside management operations.

| Event | Effect on the index |
| --- | --- |
| AddPath | the entry of the slot's replaceable path ends; a new entry is added (none for a withdrawal) |
| AddPathStream, zebra routes | the entry of the slot's replaceable path ends |
| a withdrawal through AddPath, AddPathStream or DeletePath by key | the slot's entries end when no local path is left in it |
| DeletePath of all local paths of a family | the entries of that family end (not the whole map) |
| DeleteVrf | the entries of paths written to that VRF end |
| StartBgp | the index is new, with the new table |
| a withdrawal inside the table: import-policy rejection, EVPN MAC mobility | the entry stays; a delete by its UUID finds the slot empty, deletes nothing and ends it |

A delete by UUID reads the one destination of the entry and withdraws every
local path of its slot (several only with `no_implicit_withdraw`, as before).
ListPath of the global table fills the existing `Path.uuid` field (16,
documented and never filled until now) for a local path whose slot has one
entry, taken in the same management step as the selection.

**Behavior change, released as a bug fix, no flag.** A UUID deletes only the
path of its own slot. A UUID replaced by any later write, a stale UUID, and
a delete-all of another family no longer reach other paths.

**API.** No new message. `apiutil.Path.UUID`; CLI `gobgp global rib del uuid
<uuid>`.

**Measured** on gates01, 1M local paths: delete by UUID 10.8 to 11.3 us
against 881 ms on master; 100 deletes by UUID took 0.8 ms where one walk of
the table took 269 ms. Regression tests cover each bug above and each lifecycle
row.

---

### E. Exact ListPath lookups by NLRI (implemented)

**Problem.** 6.

**API.** `TableLookupPrefix.nlri` (field 4), `apiutil.LookupPrefix.NLRI`.

**Semantics.**

- The NLRI is decoded with `apiutil.UnmarshalNLRI` for the request family and
  read through `Table.SelectDestination`: one destination, by the identity
  the table keys it by, in any family.
- It is always exact, even with `TYPE_UNSPECIFIED`: it never falls back to a
  longest-prefix match, which an unspecified-type address string still does,
  unchanged.
- Refused with INVALID_ARGUMENT, before the table is read: an NLRI together
  with a prefix string or an RD, an NLRI with `TYPE_LONGER` or
  `TYPE_SHORTER`, an NLRI that is not of the request family (prefix types by
  address family, any other type by a serialize-and-parse round trip), an
  NLRI that does not decode, and an NLRI on a VRF table. A VRF table ignores
  prefix strings, as it always has, and is left as it is.
- Labeled unicast also takes prefix strings, with the unicast lookup types;
  the lookup builds labeled NLRIs with an empty label stack.
- Request order is not kept and a key with no destination produces nothing:
  ListPath streams the selection in table order. A client matches answers to
  keys by NLRI identity. Older servers ignore field 4 and read the entry as
  an empty prefix string.

**CLI.** `gobgp global rib nlri <nlri>` and `gobgp neighbor <addr> <table>
nlri <nlri>`, the NLRI in the syntax of `rib add`.

**Measured** on gates01, table of 200k paths: 1,000 NLRI keys in one ListPath
about 3 ms, against 469 to 513 ms to list the table.

---

### X. Server incarnation and `expected_incarnation`

**Problem.** A client that writes to one of several speakers cannot tell
from an answer which server instance applied it, and a router ID is not a
unique server identity (two speakers may share it, and a restarted speaker
keeps it while losing its local paths).

**Semantics.**

- The server takes a random 16-byte `incarnation` at StartBgp; it changes on
  every start. `GetBgp` returns it, and so do the answers of AddPath,
  DeletePath and the owner calls below.
- An optional `expected_incarnation` on AddPathRequest, each
  AddPathStreamRequest message, DeletePathRequest and the owner calls.
  When set and different from the server's, the request is refused with
  FAILED_PRECONDITION before anything changes, inside the same management
  step as the mutation.

**Tests.** A restart changes it; a mismatched precondition changes nothing on
every call; a matching one applies.

**Stacks on:** master.

---

### B. Safe retry: `request_id`

**Problem.** 3. It finishes what #3912 started: #3912 makes "timed out before
it ran" mean "not applied"; this makes a retry safe when the client cannot
tell which case it is in.

**Guarantee.** Within the retention horizon, a request ID is applied at most
once. The server never forgets a receipt younger than the horizon: when the
receipt store is full of receipts younger than the horizon, a new identified
mutation is refused with RESOURCE_EXHAUSTED, while replays (an ID that has a
receipt) are still answered. The client therefore never gets a silent second
application inside the horizon.

**Semantics.**

- Optional `bytes request_id` (1 to 64 bytes) on AddPathRequest,
  DeletePathRequest and each AddPathStreamRequest message, and on the native
  `apiutil` requests.
- The receipt holds a SHA-256 of the complete logical request and its result.
  The hash covers the request kind, table type, VRF, every path (family,
  NLRI, path identifier, attributes, source, flags) in order, the
  delete selector (UUID, key, all and family), the owner when C1 is in, and
  `expected_incarnation` when X is in.
- The receipt is written in the management step of the mutation, so a
  request that ran and whose answer was lost is found by the retry.
- A request whose ID has a receipt with the same hash is not applied again:
  the first result is returned with `replayed` set. The same ID with another
  hash is refused with ALREADY_EXISTS. A request refused before any mutation
  (validation, precondition, RESOURCE_EXHAUSTED) leaves no receipt.
- **Partial native batches.** A native `AddPath` with several paths applies
  them one by one and can mutate earlier paths before a later one fails. Its
  receipt records the result of every path (UUID or error) and the overall
  error, and a replay returns exactly those.
- **AddPathStream.** The stream has one final response. Each message is its
  own request: a message with a receipt is skipped, one with a conflicting
  ID ends the stream with ALREADY_EXISTS, and the messages before it stay
  applied with their receipts. `AddPathStreamResponse` gains `applied`
  (messages applied) and `replayed` (messages skipped as replays), so a
  client that retries a whole stream after a lost answer learns how much of
  it was new.
- Receipts are kept in memory, at least `--request-id-retention` (default 10
  minutes) and at most `--request-id-max` (default 100,000) of them; a
  receipt older than the horizon expires. They do not survive a restart, and
  neither do local paths; with X a client sees the restart.
- Without `request_id` nothing changes.

**Sizing.** A receipt is about 200 bytes plus the per-path results of a
native batch (an estimate to be measured in the PR), so the default cap is
about 20 MB. Size the cap above the identified write rate times the horizon:
100,000 receipts cover 166 identified writes a second for 10 minutes.

**Metrics.** `gobgp_request_id_receipts` (gauge),
`gobgp_request_id_replays_total`, `gobgp_request_id_conflicts_total`,
`gobgp_request_id_rejected_full_total` (RESOURCE_EXHAUSTED),
`gobgp_request_id_expired_total`.

**API.** `request_id` and `replayed` fields; `applied`, `replayed` on
AddPathStreamResponse; `ServerOption` `RequestIDRetention(d, max)` beside the
flags.

**Tests.** A lost answer (an interceptor drops it) and a retry return the same
UUID and add the path once; a native batch that fails half way replays the
same per-path results; streams retried whole; a conflicting ID; a refused
request leaves no receipt; expiry by age; RESOURCE_EXHAUSTED when full of
young receipts, with replays still answered; metrics.

**Stacks on:** master.

---

### C1. Owned paths and an indexed owner filter

**Problem.** 5, and the base for C2 and D.

**Semantics.**

- Optional `string owner` (1 to 256 bytes, opaque, chosen by the client: a
  controller, a session, a job) on AddPathRequest, each AddPathStreamRequest
  message and DeletePathRequest. It is not a security boundary: any client can
  name any owner.
- A path written with an owner is owned by it until another write takes its
  slot (it then belongs to that write's owner, or to none) or it is deleted.
  The index of A records the owner of each entry. Owned stream paths get
  index entries with server-generated UUIDs, so they can be listed and
  deleted by owner; unowned stream paths still get none.
- **Owner checks on destructive requests.**
  - DeletePath by UUID with an owner refuses a UUID owned by another owner
    (FAILED_PRECONDITION).
  - DeletePath by key with an owner deletes only when the slot's current path
    is owned by that owner (otherwise FAILED_PRECONDITION).
  - DeletePath of all local paths with an owner is refused
    (INVALID_ARGUMENT); C2 is the call for that.
  - Destructive requests without an owner behave as before; F lets a
    deployment refuse them for chosen families.
- **Listing.** `ListPathRequest.owner`: the global table restricted to the
  paths that owner currently owns, selected through the owner index, never a
  table walk. It combines with the family and with prefix and NLRI filters
  (intersection). Selection runs in management-loop steps of at most 10,000
  paths, streamed as it goes, so a large owner does not hold the loop; each
  step is consistent with the mutation order, the whole stream is not a
  single snapshot. For a closed owner (D) the set only shrinks, so a listing
  after the close is complete except for paths deleted while it streams.
- **Owner state.** `GetOwners(owners)` (1 to 1,024 owners) returns, per owner,
  the number of current paths, `closed` and `closed_at` once D is in, and
  `incarnation`. It is bounded by the number of owners, not paths, and
  answers for empty and unknown owners too.
- ListPath fills `Path.owner` for an owned local path.
- Validation is all or nothing: an empty list, more than 1,024 entries
  (repeats counted) or an owner out of range refuses the whole request with
  INVALID_ARGUMENT before anything changes. A repeated owner is reported once,
  in the order of first mention.

**API.** `owner` on the three write requests and on `Path`, `owner` on
ListPathRequest; `rpc GetOwners(GetOwnersRequest{owners}) returns
(GetOwnersResponse{owners: repeated OwnerState{owner, current_paths,
closed, closed_at}, incarnation})`. CLI: `-o/--owner` on `rib add`, `rib
del` and `rib`; `gobgp global rib owner <owner>...`.

**Tests.** Owners across connections; replacement by another owner and by an
unowned write; owned stream paths listed and deletable; every owner check on
delete by UUID, by key and delete-all; ListPath owner filter with family,
prefix and NLRI filters, across several steps (step size forced small);
GetOwners for unknown, empty and populated owners; limits and repeats.

**Stacks on:** A.

---

### C2. Delete the paths of owners in bounded steps

**Problem.** 5: a controller that shuts down withdraws what it wrote.

**Semantics.**

- `DeleteOwnerPaths(owners, max_paths)`. Each call works on a finite target
  set: at its first step it takes the UUIDs the listed owners own, at most
  `max_paths` (default and maximum 100,000), and only those are candidates;
  paths written afterwards are not.
- Steps of at most 2,048 withdrawals each, with one propagation per step and
  at most 2,048 index entries examined, so other operations run between two
  steps. Before each withdrawal the step rechecks that the UUID's entry still
  exists, that the slot's current entry is that UUID, and that its owner is
  still the listed owner; otherwise the path is reported `REPLACED` and left
  alone.
- The answer gives, per path, `DELETED`, `ALREADY_ABSENT` (the index named it
  but the table no longer held it) or `REPLACED`; per owner, the number of
  paths it still owns after the call; and `remaining`, the candidates not
  reached when `max_paths` or the context cut the call short. The client
  repeats the call until `remaining` is 0 and the owners own nothing.
- A call that fails half way leaves what it deleted deleted. It is
  idempotent.
- With D, `only_closed` reports an owner that is not closed as `refused`,
  leaves its paths alone, and lets the other owners proceed.

**Measured on the existing fork implementation** (same index and step
logic, gates01, 1M paths): about 4 to 7 us per deleted path; a step of 2,048
paths holds the loop about 10 to 14 ms, where one step of 10,000 paths held
it 60 to 70 ms.

**Tests.** Replacement between two steps (step size forced to 1); owners whose
paths exceed `max_paths`, finished by repeated calls; a restart in the middle;
idempotence; limits.

**Stacks on:** C1.

---

### D. Close owners

**Problem.** 4.

**Semantics.**

- `CloseOwners(owners)` (1 to 1,024). The closed set is checked in the
  management-loop closure of every owned write, and the loop runs one
  operation at a time with `shared.mu` held, so a close is one point in the
  order of mutations, on every connection.
- **The answer** gives, per owner, `newly_closed`, `closed_at` and the number
  of paths it owns at the close point, taken in the same step. Those are the
  owner's current paths at that point, not every earlier write: a path that
  was written and then replaced or deleted before the close is not among
  them. The paths themselves are read with the ListPath owner filter; after
  a close the owner's set only shrinks.
- After the close, every new mutation naming the owner is refused with
  FAILED_PRECONDITION. A request whose `request_id` has a receipt (B) is a
  replay, not a new mutation: it returns the receipt. Deletes of a closed
  owner's paths (by UUID with the owner, C2) are allowed; that is the cleanup
  the close is for.
- **Failure semantics.** A validation failure changes nothing. An answer lost
  in transport, or an error while committing the close to disk, means the
  outcome is unknown: the close may or may not be in effect. Closing is
  idempotent (`newly_closed` false, the first `closed_at` kept), so the client
  retries until it gets an answer.
- Closed owners are never reopened.
- An operation sent before StartBgp is refused ("bgp server hasn't started
  yet", `server.go` `active`). StartBgp loads the closed set before it marks
  the server active, so no owned write is evaluated against a partial set.

**Persistence.** In memory by default: the closed set is lost on restart
together with the local paths, and a late write naming a closed owner after a
restart is accepted. With `--owner-close-dir` (and the `ServerOption`
`OwnerCloseDir`), closes are durable:

- **File.** `closed-owners.log`: a header (magic, format version, CRC32C),
  then one frame per CloseOwners call. A frame is length, record count, the
  records (owner length, owner, close time) and a CRC32C of all of it. The
  frame of one call is written as one unit, so a batch is never half on
  disk after recovery.
- **Creation.** A new file is written to a temporary name, its header synced,
  renamed into place, and the directory synced.
- **Append.** The frame is written in full (short writes are continued) and
  then `fsync`ed; the close is put in the in-memory set and acknowledged only
  after `fsync` returns success.
- **Write or sync failure.** The file is truncated back to its size before the
  frame and synced; when that succeeds, the call fails with UNAVAILABLE and
  nothing is closed. When the truncation or its sync fails, or after any
  `fsync` failure (the kernel may have dropped dirty pages), the store stops
  accepting closes and owned writes until restart, failing them with
  UNAVAILABLE, rather than continue on a file whose state is unknown.
- **Startup.** The file is read frame by frame. An incomplete last frame, or
  a last frame whose CRC fails, was never acknowledged: it is truncated and
  the file synced before any append. Any other damage (a bad header, a bad
  CRC on a frame that is followed by another) fails StartBgp: an acknowledged
  close is never silently dropped.
- **Lifecycle of the registry.** Closes are permanent for the life of the
  directory. The registry is capped by `--owner-close-max` (default
  10,000,000); a close beyond it is refused with RESOURCE_EXHAUSTED. An
  operator may start over with an empty directory only when no client can
  still send a write naming an old owner. Metrics: `gobgp_owner_closes`
  (gauge), `gobgp_owner_close_sync_seconds` (histogram),
  `gobgp_owner_close_failures_total`.
- **Why a log and not bbolt.** No new dependency, and the format is small
  enough to review. The closed set is held in memory either way for the
  check in the write path; an on-disk B-tree would not by itself bound memory.

**Measured** on gates01 (root disk `/dev/sda1`, measured while another test job
ran):

| | p50 | p90 | p99 | p99.9 | max |
| --- | --- | --- | --- | --- | --- |
| append and fsync, 1 owner per frame | 0.26 ms | 0.38 ms | 0.67 ms | 1.42 ms | 1.48 ms |
| 20 owners per frame | 0.28 ms | 0.45 ms | 0.70 ms | 1.54 ms | 6.69 ms |
| 1,000 owners per frame | 0.49 ms | 0.58 ms | 1.18 ms | 1.77 ms | 1.92 ms |

| In-memory closed set (Go map) | 16-byte owners | 32-byte owners | 64-byte owners |
| --- | --- | --- | --- |
| 100,000 owners | 5.1 MB | 6.7 MB | 9.9 MB |
| 1,000,000 owners | 72 MB | 88 MB | 120 MB |

**API.** `rpc CloseOwners(CloseOwnersRequest{owners, expected_incarnation})
returns (CloseOwnersResponse{results: repeated {owner, newly_closed,
closed_at, current_paths}, incarnation})`; `only_closed` on
DeleteOwnerPathsRequest; `closed`, `closed_at` in OwnerState. CLI: `gobgp
global rib owner close <owner>...`.

**Tests.** Owned writes queued behind a busy loop on one connection race a
close on another; for each owner the writes that succeed are exactly those
counted by the close, and ListPath with the owner filter returns exactly
them (several rounds, `-race -count=N`). Later writes refused, streams
included; replays answered from the receipt; the store: creation, restart
with the set loaded before the first write, a torn last frame truncated, a
corrupt middle frame failing StartBgp, a short write and an `fsync` error
injected through a file interface, the cap, a second server on the same
directory (`flock`).

**Stacks on:** C1; `only_closed` needs C2.

---

### F. Refuse unowned writes to chosen families (later)

`--require-owner <family>` (repeatable) and a `ServerOption`: an AddPath,
AddPathStream or DeletePath without an owner that would write a listed family
is refused with FAILED_PRECONDITION; a deletion must name the owner that owns
the slot. Routes from zebra are not API writes and are not affected. It is
proposed only once the ownership semantics of C1 and D are settled.

---

## Mapping a generation-based client onto the primitives

A client that steers routes in generations and today uses a fork of GoBGP
with a generation fence (generation-tagged writes, fences, receipts) maps as
follows.

| Fork | Generic |
| --- | --- |
| `GenerationTag{namespace, generation}` | `owner` = `"<namespace>/<generation>"`, built by the client |
| command ID, receipts, `replayed` | `request_id` (B) |
| `FenceGeneration`, `FenceGenerations` | `CloseOwners` (D) |
| fence ack listing the generation's paths | CloseOwners counts, then ListPath with the owner filter (C1) |
| fences on disk (bbolt) | `--owner-close-dir` (framed log, D) |
| `GetGeneration`, `GetGenerations` | `GetOwners` and ListPath with the owner filter (C1) |
| tagged DeletePath by UUID | DeletePath by UUID with `owner` (A, C1) |
| `DeleteGenerations` (only fenced, per-generation refusal) | `DeleteOwnerPaths{only_closed: true}`, repeated until `remaining` is 0 (C2, D) |
| replacement protection | C2 rechecks UUID and owner before each withdrawal |
| `LookupDestinations` | ListPath with NLRI keys (E), `Path.uuid` (A), `Path.owner` (C1) |
| incarnation, instance ID guard | `incarnation` and `expected_incarnation` (X) |
| `--generation-fence-require-tag` | `--require-owner` (F) |

### What the generic version loses, and how a client gets it back

1. **Instance ID on every answer.** Replaced by X: the client reads the
   incarnation once with GetBgp and sends it as `expected_incarnation`; a
   write that reaches another server, or the same server after a restart, is
   refused before it changes anything.
2. **History of a generation's paths** (REPLACED and DELETED entries, who
   replaced a path). Generic listings show current paths. The client records
   the slots it wrote; ListPath with NLRI keys (E) shows who owns each slot
   now, through `Path.owner`.
3. **Receipts listed by generation.** Receipts answer retries and are not
   listed. Each owned path carries its `request_id` in the owner listing, so
   a lost answer is recovered by ListPath with the owner filter.
4. **Fence answer listing every path.** The close answer gives counts; the
   paths come from the owner listing right after, which is complete because a
   closed owner's set only shrinks.
5. **Generation numbers as a type.** Encoded in the owner string.
6. **Receipt lifetime.** A window (10 minutes, 100,000 by default) with
   RESOURCE_EXHAUSTED instead of silent forgetting. After the window the
   client reads the owner listing instead of retrying.
7. **ListPath answer order and empty entries.** Matched by NLRI; a missing
   key is an absent destination.
8. **Existing fences.** The fork's bbolt file does not carry over: the client
   re-closes, from its own records, every generation it has stopped, before
   moving its speakers to the upstream build. CloseOwners is idempotent, so
   this can run at every start.
9. **Closed-set memory.** About 70 to 120 bytes per closed owner (measured
   above). At 1,000 closes a day that is about 30 MB a year with 32-byte
   owners.

### Semantics the client relies on, and where they live

- One point in the mutation order on every connection: D.
- Idempotence: B (writes), C2 (cleanup), D (closes).
- All-or-nothing validation: C1, C2, D.
- Per-owner refusal inside a batch: C2 with D (`only_closed`).
- Replacement protection: C2.
- Restart survival of closes: D with `--owner-close-dir`.
- Server identity on every write: X.

## Pre-empting maintainer objections

- **Size.** Eight small PRs instead of three large ones; A, E, X and B are
  independent and useful on their own; C1 adds a field and an indexed filter
  before any new deletion or fencing call.
- **Memory.** Measured above for the closed set; receipts capped and
  measured in B; the path index costs one entry per API-written path.
- **Disk.** One `fsync` per CloseOwners call, measured above; nothing on disk
  unless `--owner-close-dir` is set.
- **Lifecycle.** The closed registry is permanent, capped, and its failure
  modes are spelled out; nothing is dropped silently.

## Open questions for review

1. B's defaults (10 minutes, 100,000) and the receipt size estimate.
2. C1's step size for the owner filter (10,000) and C2's `max_paths`
   (100,000) and step (2,048).
3. D's cap (10,000,000) and whether the store should also stop owned writes
   after a sync failure, or only closes.
4. Whether X's incarnation belongs in GetBgpResponse or in a new call.

## Changes from revision 1

- A's bug description corrected: after the collision the old UUID fails and
  the surviving one deletes across families. Added the delete-all bug, the
  slot identity (table destination identity, source, path identifier,
  `no_implicit_withdraw`) and the lifecycle table. No compatibility flag.
- E specified in full: decoding, `SelectDestination`, refusals, labeled
  NLRIs for labeled lookups, VRF, the unspecified type, older servers.
- X added: incarnation and `expected_incarnation`.
- B: guaranteed horizon with RESOURCE_EXHAUSTED, full-request hash, partial
  native batches, AddPathStream aggregate, sizing and metrics.
- C split into C1 (owners, indexed ListPath filter, GetOwners, owner checks
  on every destructive request, owned stream entries) and C2 (bounded
  cleanup with a finite target set, examined-entry bound, `remaining`, and
  rechecks before each withdrawal).
- D: answer is the current paths at the close point; failure semantics;
  replays return the receipt; framed log with short-write, `fsync`-failure,
  truncation, creation and corruption rules; measured memory and `fsync`
  tails; registry lifecycle and cap; corrected the StartBgp wording; no bbolt
  memory claim.
- F deferred until ownership settles; deletions must name the owning owner.
