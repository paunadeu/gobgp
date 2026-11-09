# Owned paths: design of a set of generic API primitives

Status: revision 4, for review before any code of B, C, D and F. A and E are
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

**Lifecycle.** An entry records the path the table installed for its
AddPath (the path written, or the copy the import policy made of it). The
index is touched only inside management operations. Every API or zebra
write to a slot that has an entry is applied one path at a time and the
slot is read after each, so every transition is seen, also between two
paths of one stream message.

| Event | Effect on the index |
| --- | --- |
| AddPath | entries whose installed path left the slot end; an entry is added for the installed path, none when nothing was installed (a withdrawal, or a path the import policy rejected) |
| any write list | paths of slots without an entry go to the table in runs, as one list; a path of a slot with an entry, or whose installed path AddPath captures, is propagated on its own and its slot read after it |
| AddPathStream, zebra routes, DeletePath by key | entries whose installed path left the slot end, after each path |
| DeletePath of all local paths of a family | the entries of that family end (not the whole map) |
| DeleteVrf | the entries whose installed path DeleteVrf withdrew end; VPN FlowSpec paths outlive their VRF and keep their UUIDs |
| StartBgp | the index is new, with the new table |
| a withdrawal inside the table caused by a received route (EVPN MAC mobility) | in A, the entry stays until its slot is next written or read; a delete by its UUID finds its path gone, deletes nothing, and ends it. C1 makes this synchronous (see below) |

A delete by UUID reads the one destination of the entry and withdraws every
local path of its slot (several only with `no_implicit_withdraw`, as before).
ListPath of the global table fills the existing `Path.uuid` field (16,
documented and never filled until now) for each installed path that has an
entry, taken in the same management step as the selection.

**Behavior change, released as a bug fix, no flag.** A UUID deletes only the
path of its own slot. A UUID replaced by any later write, a stale UUID, and
a delete-all of another family no longer reach other paths.

**API.** No new message. `apiutil.Path.UUID`; CLI `gobgp global rib del uuid
<uuid>`.

**Measured** on gates01, 1M local paths: delete by UUID 13.3 to 16.0 us
against 881 ms on master; 100 deletes by UUID took 0.77 ms where one walk of
the table took 308 ms. The entries of a slot and the paths read back are
kept in sets, so reconciling a slot is linear in its size, and a delete by
UUID withdraws the slot as one list and reconciles once. Paths of slots
without an entry still go to the table in runs, as one list.

| Delete by one UUID, slot of k coexisting paths | k = 1 | k = 10 | k = 100 | k = 1,000 |
| --- | --- | --- | --- | --- |
| time | 9.1 to 10.0 us | 25 to 32 us | 0.20 to 0.23 ms | 7.7 to 7.9 ms |

Past 100 the table's own withdrawal, which scans the slot for each withdrawn
path, dominates.

| AddPathStream, 10,000 new paths, table of 1M local paths | no index | 1 entry | 1M entries |
| --- | --- | --- | --- |
| per path | 1.78 to 1.89 us | 1.83 to 1.93 us | 1.64 to 2.07 us | Regression tests: each bug above, a stream message
that withdraws a `no_implicit_withdraw` path and adds its replacement, the
path identifier, a label change, two BGP-LS nodes that print alike, and VPN
FlowSpec in both families outliving its VRF.

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
  `TYPE_SHORTER`, an NLRI that is not of the request family (checked by its
  concrete type and its own address family, never by a byte round trip,
  which can read an encapsulation endpoint as a host prefix; an unknown type
  is refused), an
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
2.7 to 5.1 ms, against 469 to 513 ms to list the table.

---

### X. Server incarnation and `expected_incarnation`

**Problem.** A client that writes to one of several speakers cannot tell
from an answer which server instance applied it, and a router ID is not a
unique server identity (two speakers may share it, and a restarted speaker
keeps it while losing its local paths).

**Semantics.**

- The server takes a random 16-byte `incarnation` at StartBgp; it changes on
  every start. `GetBgpResponse` carries it. Write answers do not repeat it: a
  client reads it once and sends it back as a precondition.
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
once. The server never forgets a receipt younger than the horizon. The bytes
a receipt needs are reserved before the mutation: when the store cannot
reserve them without dropping a receipt younger than the horizon, by count or
by bytes, the new identified mutation is refused with RESOURCE_EXHAUSTED and
nothing changes. Replays (an ID that has a receipt) are still answered. A
client therefore never gets a silent second application inside the horizon.

**Semantics.**

- Optional `bytes request_id` (1 to 64 bytes) on AddPathRequest,
  DeletePathRequest and each AddPathStreamRequest message, and on the native
  `apiutil` requests.
- The receipt holds a SHA-256 of the complete logical request and its
  compact result. The hash covers the request kind, table type, VRF, every
  path (family, NLRI, path identifier, attributes, source, flags) in order,
  the delete selector (UUID, key, all and family), the owner when C1 is in,
  and `expected_incarnation` when X is in.
- **Compact result.** Per path, the UUID (16 bytes) or an error, stored as
  its gRPC code and a message truncated to 128 bytes; for a delete, the
  outcome. The reservation is the exact size of the request ID and hash plus
  this upper bound per path, computed before the mutation; the unused part is
  released after it.
- The receipt is written in the management step of the mutation, so a
  request that ran and whose answer was lost is found by the retry.
- A request whose ID has a receipt with the same hash is not applied again:
  the first result is returned with `replayed` set. The same ID with another
  hash is refused with ALREADY_EXISTS. A request refused before any mutation
  (validation, precondition, RESOURCE_EXHAUSTED) leaves no receipt.
- **Partial native batches.** A native `AddPath` with several paths applies
  them one by one and can mutate earlier paths before a later one fails
  (`server.go`, the per-path loop of `addPath`). Its receipt records the
  result of every path and the overall error, and a replay returns exactly
  those.
- **AddPathStream.** The stream has one final response. Each message is its
  own request: a message with a receipt is skipped, one with a conflicting ID
  ends the stream with ALREADY_EXISTS, one that cannot reserve its receipt
  ends it with RESOURCE_EXHAUSTED, and the messages before it stay applied
  with their receipts. `AddPathStreamResponse` gains `applied` (messages
  applied) and `replayed` (messages skipped as replays), so a client that
  retries a whole stream after a lost answer learns how much of it was new.
- Receipts are kept in memory for at least `--request-id-retention` (default
  10 minutes), within `--request-id-max` (default 100,000) and
  `--request-id-max-bytes` (default 64 MiB); a receipt older than the horizon
  expires. They do not survive a restart, and neither do local paths; with X a
  client sees the restart.
- Without `request_id` nothing changes.

**Sizing, measured.** A receipt store of 100,000 receipts (map plus expiry
list, 32-byte hash) measured on gates01: see the table below. The default
byte cap is above the default count cap at one path per receipt, so the count
binds for single-path writes and the bytes bind for large native batches.
Size the caps above the identified write rate times the horizon: 100,000
receipts cover 166 identified writes a second for 10 minutes.

| 100,000 receipts | 16-byte IDs | 36-byte IDs (UUID text) | 64-byte IDs |
| --- | --- | --- | --- |
| 1 path per receipt | 24.4 MB (244 B each) | 30.7 MB (306 B) | 33.9 MB (338 B) |
| 10 paths per receipt | 38.7 MB (387 B) | 45.1 MB (450 B) | 48.3 MB (482 B) |

The revision 2 estimate of 200 bytes per receipt was low; these are the
measured figures.

**Metrics.** `gobgp_request_id_receipts` and `gobgp_request_id_bytes`
(gauges), `gobgp_request_id_replays_total`, `gobgp_request_id_conflicts_total`,
`gobgp_request_id_rejected_full_total` (RESOURCE_EXHAUSTED),
`gobgp_request_id_expired_total`.

**API.** `request_id` and `replayed` fields; `applied`, `replayed` on
AddPathStreamResponse; `ServerOption` `RequestIDRetention(age, count,
bytes)` beside the flags.

**Tests.** A lost answer (an interceptor drops it) and a retry return the same
UUID and add the path once; a native batch that fails half way replays the
same per-path results; streams retried whole; a conflicting ID; a refused
request leaves no receipt; expiry by age; RESOURCE_EXHAUSTED by count and by
bytes while every receipt is young, with replays still answered and nothing
mutated; metrics.

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
- **No owned coexistence.** An owned path never shares a slot with another
  path. An owned write with `no_implicit_withdraw` is refused with
  INVALID_ARGUMENT; an owned write to a slot that holds a
  `no_implicit_withdraw` path is refused with FAILED_PRECONDITION; and a
  `no_implicit_withdraw` write to a slot that holds an owned path is refused
  with FAILED_PRECONDITION. Every owned slot therefore holds one path, so a
  withdrawal of the slot removes exactly the path the UUID and owner checks
  were made for. The alternative, per-path identity inside a slot, needs the
  table to withdraw one path among several of the same source and path
  identifier, which `explicitWithdraw` cannot do today (it removes the last
  match): a change to the table's withdrawal path, a new withdrawal key, and
  tests of every caller of `explicitWithdraw`, for a combination
  (`no_implicit_withdraw` with owners) no known client needs. It can be
  added later without changing this API.
- **Synchronous membership.** C1 makes the index exact: it also observes
  transitions the table makes on its own. `TableManager.Update` returns, per
  destination, the old and new known path lists; when an installed path of
  an entry leaves the list, the entry ends in the same step. Received routes
  are processed in FSM goroutines under the read lock of `shared.mu`, so the
  index gets its own mutex; the check is made only when a removed path is
  local, which for received routes happens only with EVPN MAC mobility.
- **Owner checks on destructive requests.**
  - DeletePath by UUID with an owner refuses a UUID owned by another owner
    (FAILED_PRECONDITION).
  - DeletePath by key with an owner deletes only when the slot's path is
    owned by that owner (otherwise FAILED_PRECONDITION).
  - DeletePath of all local paths with an owner is refused
    (INVALID_ARGUMENT); C2 is the call for that.
  - Destructive requests without an owner behave as before; F lets a
    deployment refuse them for chosen families.
- **Listing.** `ListPathRequest.owner`: the global table restricted to the
  paths that owner currently owns, enumerated through a per-owner list of
  entries ordered by a creation sequence number, never a table walk. It
  combines with the family and with prefix and NLRI filters (intersection).
  Each management-loop step examines at most 2,048 entries of the owner,
  stale ones included, sends what it found, and continues from its cursor in
  the next step, so a large owner does not hold the loop. Each step is
  consistent with the mutation order; the whole stream is not a single
  snapshot. For a closed owner (D) the set only shrinks, so a listing after
  the close is complete except for paths deleted while it streams.
- **Owner state.** `GetOwners(owners)` (1 to 1,024 owners) returns, per owner,
  the number of current paths (a counter kept with the index, exact with
  synchronous membership), `closed` and `closed_at` once D is in. It is
  bounded by the number of owners, not paths, and answers for empty and
  unknown owners too. The incarnation is read from GetBgp (X).
- ListPath fills `Path.owner` for an owned local path.
- Validation is all or nothing: an empty list, more than 1,024 entries
  (repeats counted) or an owner out of range refuses the whole request with
  INVALID_ARGUMENT before anything changes. A repeated owner is reported once,
  in the order of first mention.

**API.** `owner` on the three write requests and on `Path`, `owner` on
ListPathRequest; `rpc GetOwners(GetOwnersRequest{owners}) returns
(GetOwnersResponse{owners: repeated OwnerState{owner, current_paths,
closed, closed_at}})`. CLI: `-o/--owner` on `rib add`, `rib del` and `rib`;
`gobgp global rib owner <owner>...`.

**Tests.** Owners across connections; replacement by another owner and by an
unowned write; every refusal of owned coexistence; an EVPN MAC move ending an
owned entry at once; owned stream paths listed and deletable; every owner
check on delete by UUID, by key and delete-all; ListPath owner filter with
family, prefix and NLRI filters across several steps (step size forced
small), with stale entries counted in the step bound; GetOwners counts for
unknown, empty and populated owners; limits and repeats.

**Stacks on:** A.

---

### C2. Delete the paths of owners in bounded steps

**Problem.** 5: a controller that shuts down withdraws what it wrote.

**Semantics.**

- `DeleteOwnerPaths(owners, max_paths)`. The first step records a cutoff,
  the current value of the index's creation sequence number: constant size,
  whatever the number of paths. Only entries created at or before the cutoff
  are candidates; paths written afterwards are not.
- Each step enumerates the owners' entry lists from a cursor and examines at
  most 2,048 entries, stale ones included, withdrawing the candidates it
  finds with one propagation. Before each withdrawal the step rechecks that
  the UUID's entry still exists, that its installed path is still in the
  slot, and that its owner is still the listed owner; otherwise the path is
  reported `REPLACED` and left alone. With no owned coexistence (C1) the
  withdrawal removes exactly that path.
- A call withdraws at most `max_paths` paths (default and maximum 100,000).
  Each step withdraws at most the smaller of 2,048 and the allowance left,
  so the last step is clipped and the call never exceeds `max_paths`.
- The answer is compact and bounded as a whole: per owner, the counts of
  `DELETED`, `ALREADY_ABSENT` (the index named it but the table no longer
  held it) and `REPLACED`, and the number of paths it still owns;
  `remaining`, the candidates not reached; and sample UUIDs, at most 1,000
  for the whole call across owners and outcomes, filled in the order the
  paths were handled, with `samples_truncated` set when more were left out.
  The answer never exceeds 1 MiB: with 1,024 owners of 256 bytes, the
  per-owner counts take at most about 300 KiB and the samples 16 KiB. The
  client repeats the call until `remaining` is 0 and the owners own nothing,
  and reads UUIDs it needs from the owner listing.
- A call that fails half way leaves what it deleted deleted. It is
  idempotent.
- With D, `only_closed` reports an owner that is not closed as `refused`,
  leaves its paths alone, and lets the other owners proceed.

**Measured on the existing fork implementation** (same index and step
logic, gates01, 1M paths): about 4 to 7 us per deleted path; a step of 2,048
paths holds the loop about 10 to 14 ms, where one step of 10,000 paths held
it 60 to 70 ms.

**Tests.** Replacement between two steps (step size forced to 1); a path
written after the cutoff is left alone; owners whose paths exceed
`max_paths`, finished by repeated calls; a step bound reached on stale
entries alone; a restart in the middle; idempotence; the bounded answer;
limits.

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
  of paths it owns at the close point, taken in the same step from the
  synchronous index of C1. Those are the owner's current paths at that
  point, not every earlier write: a path that was written and then replaced
  or deleted before the close is not among them. The paths themselves are
  read with the ListPath owner filter; after a close the owner's set only
  shrinks. The answer is bounded by the number of owners.
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

- **File.** `closed-owners.log`: a file header (magic, format version,
  CRC32C), then one frame per CloseOwners call. A frame is a fixed 16-byte
  frame header, the body, and a body CRC:
  - frame header: body length (4 bytes), sequence number (8 bytes, one more
    than the previous frame's, starting at 1), and a CRC32C of those 12
    bytes (4 bytes). The length is protected on its own, so a damaged length
    is detected without trusting it to find the body.
  - body: record count, then the records (owner length, owner, close time).
  - body CRC: a CRC32C of the frame header and the body.
  The frame of one call is written as one unit, so a batch is never half on
  disk after recovery.
- **Creation.** A new file is written to a temporary name, its header synced,
  renamed into place, and the directory synced.
- **Append.** The frame is written in full (short writes are continued) and
  then `fsync`ed; the close is put in the in-memory set and acknowledged only
  after `fsync` returns success.
- **Write failure before any byte reached the file.** The call fails with
  UNAVAILABLE and nothing is closed; the store stays usable.
- **Any other failure** (a short write that cannot be completed, an error
  from `fsync`, a truncation that fails): the outcome is uncertain, since the
  kernel may have dropped dirty pages or kept part of the frame. The store
  then stops accepting both closes and owned writes until restart, failing
  them with UNAVAILABLE, rather than continue on a file whose state is
  unknown. Unowned writes are not affected. The restart runs the recovery
  below, which never drops an acknowledged close.
- **Startup.** The file is read frame by frame. Recovery is conservative and
  decides only on bytes that are themselves protected:
  - A torn tail is recoverable in exactly two shapes, both at the end of
    the file: fewer than 16 bytes after the last good frame (the frame
    header itself is incomplete), or a frame header whose CRC is valid,
    whose sequence number follows the previous one, whose length is within
    the maximum frame size, and whose body ends beyond the end of the file.
    Such a frame was never fully written, so never synced and never
    acknowledged. It is truncated and the file synced before any append.
  - Everything else fails StartBgp: a complete frame header whose CRC fails
    (a damaged length or sequence number is caught here, before the length
    is used), a sequence number out of order, a length beyond the maximum, a
    complete frame whose body CRC fails (the last one included), a bad file
    header, and trailing bytes that are not a frame. None of these can be
    told apart from damage to an acknowledged close, so none is repaired
    silently; the operator decides.
  - The recovery relies on the file system keeping bytes that `fsync`
    reported synced. A file system that truncates a synced file could
    present an acknowledged last frame as a torn tail; that is outside what
    a log can detect without a second copy.
- **Lifecycle of the registry.** Closes are permanent for the life of the
  directory. The registry has a count cap, `--owner-close-max` (default
  1,000,000), and a byte budget, `--owner-close-max-bytes` (default 128 MiB,
  counting owner bytes plus a fixed per-owner overhead of 64 bytes); a close
  beyond either is refused with RESOURCE_EXHAUSTED before anything is
  written. An operator may start over with an empty directory only when no
  client can still send a write naming an old owner. Metrics:
  `gobgp_owner_closes` and `gobgp_owner_close_bytes` (gauges),
  `gobgp_owner_close_sync_seconds` (histogram),
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
closed_at, current_paths}})`; `only_closed` on
DeleteOwnerPathsRequest; `closed`, `closed_at` in OwnerState. CLI: `gobgp
global rib owner close <owner>...`.

**Tests.** Owned writes queued behind a busy loop on one connection race a
close on another; for each owner the writes that succeed are exactly those
counted by the close, and ListPath with the owner filter returns exactly
them (several rounds, `-race -count=N`). Later writes refused, streams
included; replays answered from the receipt; the store: creation, restart
with the set loaded before the first write, a torn tail truncated (cut inside
the frame header and inside the body), a frame corrupted after a successful
acknowledgment failing StartBgp whether it is the last frame or not, the
length field of an acknowledged frame changed to another permitted value
beyond the end of the file failing StartBgp (its frame header CRC no longer
matches), a sequence number gap, a bad file header and an oversized length
failing StartBgp, a short write and an
`fsync` error injected through a file interface stopping closes and owned
writes until restart, the count cap and the byte budget, a second server on
the same directory (`flock`).

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
- **Memory.** Measured above for the closed set and for receipts, both with
  count and byte caps; the path index costs one entry per API-written path.
- **Disk.** One `fsync` per CloseOwners call, measured above; nothing on disk
  unless `--owner-close-dir` is set.
- **Lifecycle.** The closed registry is permanent, capped, and its failure
  modes are spelled out; nothing is dropped silently.

## Settled in review

1. B: 10 minutes and 100,000 receipts as a starting point, plus a byte cap
   (64 MiB); the receipt size is measured above.
2. C1 and C2 examine at most 2,048 entries per step; 100,000 is only C2's
   total per call, with bounded enumeration and a bounded answer.
3. D: 1,000,000 closed owners and a 128 MiB byte budget by default; after an
   uncertain persistence failure the store stops owned writes as well as
   closes.
4. X's incarnation is in GetBgpResponse.
5. Owned coexistence in one slot is refused (C1); per-path identity inside a
   slot is left for later, with its cost written down.

## Open questions for review

1. Whether `--owner-close-max-bytes` should count the measured map overhead
   (70 to 120 bytes per owner) instead of the fixed 64 bytes.

## Changes from revision 3

- A: sets for a slot's entries and paths read back; a whole-slot delete
  reconciles once; runs of unwatched paths go as one list; measured with
  coexisting slots of 1 to 1,000 paths and with indexes of 0, 1 and 1M
  entries.
- D: every frame has its own header with the body length, a sequence number
  and a CRC over both; recovery treats a frame as torn only when that header
  is valid and in sequence, or incomplete, and fails startup on anything
  else, including a length changed to another permitted value. A test
  changes the length of an acknowledged frame.
- C2: the last step is clipped to the `max_paths` allowance left; samples
  are capped at 1,000 per call across owners and outcomes, and the answer at
  1 MiB.

## Changes from revision 2

- A: entries track the path the table installed, and every API or zebra
  write to a watched slot is applied one path at a time with the slot read
  after each, so a stream message that withdraws a `no_implicit_withdraw`
  path and adds its replacement no longer leaves a stale UUID. DeleteVrf
  ends only the entries whose path it withdrew (VPN FlowSpec outlives the
  VRF). Rejected paths get no entry. New regression tests listed in A.
- E: the family of an NLRI key is checked by its concrete type and address
  family; the byte round trip is gone.
- X: the incarnation is in GetBgpResponse only.
- B: receipt bytes reserved before the mutation, a byte cap, compact
  results, measured receipt size.
- C1: owned coexistence refused, with the cost of the alternative; the index
  observes table transitions so membership and counts are exact; the owner
  filter is bounded by 2,048 examined entries per step.
- C2: a constant-size cutoff and incremental enumeration bounded by examined
  entries; a compact, bounded answer.
- D: a complete frame with a bad CRC fails startup, the last one included;
  only a torn tail is recovered, conservatively; uncertain persistence
  failures stop owned writes too; lower defaults with a byte budget; tests
  that corrupt an acknowledged frame.

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
