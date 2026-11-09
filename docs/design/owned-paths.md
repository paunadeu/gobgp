# Owned paths: design of a set of generic API primitives

Status: design, for review before any code. Base: osrg/gobgp master
`42c7736d` ("server: do not apply a path request whose gRPC context has
ended", #3912).

This document proposes six small pull requests. Each one must be worth
merging on its own, for any GoBGP client, and none depends on a particular
deployment. The last part maps an existing client that steers routes in
generations onto them, and lists what that client has to do itself.

## Problems, as any client sees them

1. **DeletePath by UUID walks the table.** `deletePath` loops over the UUID
   map and, for each match, over `GetPathList` of every family. On a table
   of 1M local paths a single delete by UUID took about 850 ms (measured on
   gates01, Ryzen 9 5950X); it holds the management loop for that long.
2. **The UUID map is keyed without the family.** `pathTokey` is
   `"<path id>:<prefix>"`. A labeled-unicast AddPath for a prefix that has a
   unicast path overwrites the unicast path's UUID, so that path can no longer
   be deleted by UUID; and a delete by either UUID removes the local paths of
   every family with that prefix. A path replaced by `AddPathStream` keeps its
   old UUID in the map, and deleting by that stale UUID removes the new path.
3. **A client cannot retry a write safely.** Since #3912 a request whose
   context ends before the management loop runs it is not applied. A request
   the loop has started runs to completion, and the server's copy of the
   deadline starts when the request arrives, so it ends a little after the
   client's. A client that sees DEADLINE_EXCEEDED, or loses the connection,
   does not know whether its AddPath was applied, and a retry may add the path
   twice or under a second UUID.
4. **A controller cannot stop writing for good.** A controller that is
   replaced (failover, upgrade, a new job) may still have AddPath requests in
   flight on other connections, queued behind a long operation. Nothing lets
   the new controller say "from now on, nothing from the old one", and know
   which of its paths landed before that point.
5. **Cleaning up after a controller needs its paths.** To withdraw what a
   controller wrote, a client must have kept every UUID, or must list whole
   families and guess.
6. **ListPath can filter by prefix only some families.** `Table.Select`
   handles IPv4/IPv6 unicast (exact, longer, shorter), VPN, EVPN (route type)
   and MUP; every other family, labeled unicast among them, returns "route
   filtering is not supported for this family", so reading one destination
   means streaming the whole family.

## The pull requests

| # | Branch (proposed) | Title | Stacks on |
| --- | --- | --- | --- |
| A | `upstream/uuid-index` | server: index local paths by route key for DeletePath by UUID | master |
| B | `upstream/request-id` | server: an optional request ID makes AddPath and DeletePath safe to retry | master |
| C | `upstream/owned-paths` | server: optional path owners, with listing and bounded deletion by owner | A |
| D | `upstream/close-owner` | server: close an owner to refuse its later writes | C |
| E | `upstream/listpath-exact` | table: exact ListPath lookups for every family | master |
| F | (follow-up) | server: refuse unowned writes to chosen families | C |

The order differs from the first sketch in one place: deleting all paths of
an owner moves into C, before closing exists. It is useful without closing
(a controller that shuts down cleanly withdraws its own paths), and C would
otherwise ship an owner field with no operation on it. D then adds closing
and an option to delete only closed owners.

A, B and E are independent of each other. A and B both touch `addPath` and
`deletePath`; whichever lands second rebases, without semantic conflict.

---

### A. Index local paths by route key

**Problem.** Problems 1 and 2.

**Semantics.**

- The server keeps an index of the local paths written through the API:
  route key -> UUID and UUID -> entry. The route key is the family, the NLRI
  as the table keys it (`nlri.String()`, so a label is not part of a
  labeled-unicast key), the path identifier, and for a VRF write the global
  key after `ToGlobalPath`.
- AddPath sets the entry of its key; a previous UUID at that key ends.
  AddPathStream, a withdrawal, a delete by key and a delete of all local
  paths end the entries of the keys they touch. The index is updated in the
  same management-loop step as the table.
- DeletePath by UUID looks the entry up, reads that one destination, and
  withdraws the local path with that path identifier. Cost: one map lookup
  and one destination read, about 11 us at 1M paths instead of about 850 ms.
- The UUID of a path that another write replaced no longer deletes anything:
  it returns the existing error "can't find a specified path(s) with the
  given UUID(s)".
- `Path.uuid` (field 16, documented as "only paths installed by AddPath API
  have this", never filled today) is filled by ListPath for local paths that
  have an entry.

**Behavior change** (the PR must say it plainly): a UUID deletes only the
path of its own family. Today it deletes the local paths of every family with
the same prefix and path identifier, and a labeled-unicast write can make a
unicast UUID undeletable. Both are treated as bugs.

**API.** No new message. `Path.uuid` starts being filled.

**Tests.** Unicast and labeled unicast for the same prefix get distinct
UUIDs, each deletes only its own; a UUID replaced by AddPath, AddPathStream,
a withdrawal or a delete by key no longer deletes; delete all paths of a
family; VRF writes; ListPath reports `uuid`; the existing DeletePath tests
pass. Benchmark: delete by UUID at 1M paths, before and after.

**Stacks on:** master.

---

### B. Safe retry: `request_id`

**Problem.** Problem 3. It finishes what #3912 started: #3912 makes "timed
out before it ran" mean "not applied"; this makes a retry safe when the
client cannot tell which case it is in.

**Semantics.**

- `AddPathRequest`, `DeletePathRequest` and each `AddPathStreamRequest`
  message take an optional `request_id` (bytes, 1 to 64; a UUID is the
  expected use).
- The server keeps a receipt per applied request ID: a hash of the request
  and its result (the UUID of AddPath, the outcome of DeletePath). A request
  whose ID has a receipt is not applied again; the first result is returned
  with `replayed` set. The same ID with another payload is refused with
  ALREADY_EXISTS. A request that was refused leaves no receipt, so a retry of
  it is evaluated again.
- The receipt is written in the same management-loop step as the mutation,
  so a request that ran and whose answer was lost is found by the retry.
- Receipts are kept in memory for at least `--request-id-retention`
  (default 10 minutes) and at most `--request-id-max` (default 100000) of
  them, oldest dropped first. A retry after its receipt is gone is applied
  again; the retention must cover the client's retry horizon. Receipts do not
  survive a restart, and neither do local paths.
- Without `request_id` nothing changes.

**API.** `bytes request_id` on the three requests; `bool replayed` on
`AddPathResponse` and on a new `DeletePathResponse` field. Field numbers
follow upstream (next free numbers).

**Tests.** A lost answer (a server interceptor drops it) and a retry return
the same UUID and add the path once; the same ID with another path is
refused; a deadline that passes after the loop started, then a retry; a
refused request leaves no receipt; retention by count and age; streams.

**Stacks on:** master.

---

### C. Owned paths

**Problem.** Problem 5, and the base for D.

**Semantics.**

- `AddPathRequest`, `AddPathStreamRequest` and `DeletePathRequest` take an
  optional `owner`: an opaque string chosen by the client (a controller, a
  session, a job), 1 to 256 bytes. It is not a security boundary; any client
  can name any owner.
- A path written with an owner is owned by it until another write takes its
  route key (it then belongs to that write's owner, or to none) or it is
  deleted. The index of A records the owner of each entry.
- `ListOwnerPaths(owners)` (1 to 1024 owners) returns, per owner, its current
  paths: UUID, route key, the path, the `request_id` that wrote it (when B is
  in), and whether the table holds it now. It reads only those destinations,
  in one management-loop step.
- `DeleteOwnerPaths(owners)` withdraws every current path of the listed
  owners. Replacement protection: a path is withdrawn only while one of the
  listed owners owns it, so a path another write took over is kept. It runs
  in steps of at most 2048 withdrawals with one propagation each, so other
  operations run between two steps; each step rechecks ownership. It returns
  per path DELETED or ALREADY_ABSENT (the index named it but the table no
  longer held it), the number of steps, and per owner the paths left (none).
  A call that fails halfway leaves what it deleted deleted; repeating it
  finishes the rest. It is idempotent.
- A DeletePath by UUID with an owner refuses a UUID owned by another owner.
- ListPath fills a new `Path.owner` for local paths that have one.
- Validation is all or nothing: an empty list, more than 1024 entries
  (repeats counted), or an owner out of range refuses the whole request with
  INVALID_ARGUMENT before anything changes. A repeated owner is handled and
  reported once, in the order of first mention.
- Answers carry the server `incarnation`, a random ID taken at StartBgp, so a
  client sees that the server it lists is not the one it wrote to.

**API.**

- `string owner` on the three requests; `string owner` on `Path`.
- `rpc ListOwnerPaths(ListOwnerPathsRequest{owners}) returns
  (ListOwnerPathsResponse{owners: repeated OwnerPaths{owner, paths:
  repeated OwnedPath{uuid, family, nlri, identifier, vrf_id, request_id,
  present, path}}, incarnation})`.
- `rpc DeleteOwnerPaths(DeleteOwnerPathsRequest{owners}) returns
  (DeleteOwnerPathsResponse{results: repeated OwnerDeleteResult{owner,
  outcomes: repeated {uuid, outcome}}, steps, incarnation})`.

**Measured on the existing implementation** (same index and step logic,
gates01, table of 1M paths): a deleted path costs about 4 to 7 us; 1,000
owners of one path each take about 5.7 ms; a step of 2048 paths holds the
loop about 10 to 14 ms, where a single step of 10,000 paths held it 60 to
70 ms.

**Tests.** Owners across connections; replacement by another owner and by
an unowned write, also between two steps of a delete (step size forced to 1);
a restart in the middle of a delete; limits and repeats; listing after a lost
AddPath answer finds the path; ListPath shows owners; scale test with 1,024
owners.

**Stacks on:** A.

---

### D. Close an owner

**Problem.** Problem 4.

**Semantics.**

- `CloseOwners(owners)` (1 to 1024) closes them. The closed set is checked
  in the management-loop closure that does each owned write, and the loop
  runs one operation at a time with `shared.mu` held, so a close is one point
  in the order of mutations: every write of those owners the loop ran before
  it is applied and listed in the answer (the answer is `ListOwnerPaths` of
  the owners, taken in the same step), and every later write naming any of
  them is refused with FAILED_PRECONDITION, on every connection.
- It is idempotent and atomic: closing a closed owner is not an error
  (`newly_closed` false, the first close time kept), and when the call fails
  it has closed none.
- A request naming a closed owner is refused even when its `request_id` has
  a receipt: after a close, every write naming the owner fails, with no
  exception. (A replay applies nothing, so returning the receipt would also
  be safe; refusing keeps the guarantee one sentence long.)
- A delete by UUID or by owner of a closed owner's paths is allowed: that is
  the cleanup the close is for.
- `DeleteOwnerPaths` gains `only_closed`: with it, an owner that is not
  closed is reported `refused`, its paths are left alone, and the other
  owners of the call proceed.
- `ListOwnerPaths` reports per owner `closed` and `closed_at`.
- Closed owners are never reopened.

**Persistence.** In memory by default: the closed set is lost on restart,
together with the local paths, and a late write naming a closed owner after a
restart is accepted. With `--owner-close-dir`, closes are written to a file
there and synced before the answer, and StartBgp loads them before it accepts
any write (every write waits for StartBgp). A second server cannot open the
same file (`flock`).

Choice: a **dependency-free append-only file**, not bbolt.

- Format: a header (magic, version), then one record per close: length,
  owner, close time, CRC32C. A `CloseOwners` call appends all its records and
  syncs once. A torn last record (crash during the append) fails its CRC and
  is dropped at load; that close was never acknowledged.
- Cost: the closed set is kept in memory, since closes are never removed. At
  about 100 bytes per owner in a Go map (an estimate, not measured), 1M closed
  owners take about 100 MB, and loading the file takes one sequential read.
  The file grows by the record size per close (about 50 bytes with a
  32-byte owner) and needs no compaction.
- bbolt would keep memory flat (B+tree on disk, bounded cache) at the price
  of a new dependency, or of a build tag and a second code path to test.
  Upstream is more likely to accept no dependency; a client that closes
  millions of owners can namespace them and keep the count down.

**API.** `rpc CloseOwners(CloseOwnersRequest{owners}) returns
(CloseOwnersResponse{results: repeated {owner, newly_closed, closed_at,
paths}, incarnation})`; `only_closed` on `DeleteOwnerPathsRequest`; `closed`,
`closed_at` on `OwnerPaths`; flags `--owner-close-dir`.

**Tests.** Writes queued behind a busy loop on one connection race a close on
another; for each owner the writes that succeed are exactly the ones the
close lists, and the table holds only those (several rounds, with
`-race -count=N`); later writes, streams and replays are refused; closes
survive a restart and are loaded before the first write; a torn last record;
a second server on the same directory; `only_closed` with a mix of closed and
open owners; idempotence; a close equals a batch of one.

**Stacks on:** C.

---

### E. Exact ListPath lookups for every family

**Problem.** Problem 6.

**Can ListPath do it without a new RPC?** Yes, with one addition.

- `TableLookupPrefix` is a string plus an RD. A string is not a key for every
  family (flowspec, BGP-LS, SR policy). Add `NLRI nlri` to
  `TableLookupPrefix`: with `TYPE_EXACT` and an NLRI, `Table.Select` reads
  that destination by its table key (`GetDestination`), for any family.
- For labeled unicast, also accept the prefix string with exact, longer and
  shorter, as for unicast; the table key of a labeled-unicast NLRI is its
  prefix.
- Batch: `ListPathRequest.prefixes` is already repeated, so one request reads
  many keys.

**What ListPath cannot keep.**

- Request order: the selected destinations are collected in a table and
  streamed in its iteration order. A client matches answers to keys by the
  destination's NLRI.
- An entry for a key with no destination: ListPath streams nothing for it.
  Absence is a key that is not in the answer.

Both are acceptable for a lookup; the request does not need to change shape.
The lookup runs inside `getRib`'s management operation, so it sees every
mutation acknowledged before it, as today.

**API.** `NLRI nlri = 4` on `TableLookupPrefix` (next free number).

**Tests.** Exact lookups by NLRI for unicast, labeled unicast (label ignored),
flowspec and EVPN; labeled unicast by prefix string with each lookup type;
an exact lookup does not match a covering prefix; missing keys; 1,000 keys on
a table of 200k paths cost less than one walk (measured on the existing
implementation: 1.7 ms against 45 ms).

**Stacks on:** master.

---

### F. Follow-up: refuse unowned writes to chosen families

`--require-owner <family>` (repeatable): an AddPath, AddPathStream or
DeletePath without an owner that would write a listed family is refused with
FAILED_PRECONDITION; routes from zebra are not API writes and are not
affected. It lets a deployment guarantee that every path of a family belongs
to some owner, so closing owners covers all of them. Small, after C.

---

## Mapping a generation-based client onto the primitives

A client that steers routes in generations, and today uses the
`octo/generation-fence` fork API, maps as follows.

| Fork | Generic |
| --- | --- |
| `GenerationTag{tenant/namespace, generation}` | `owner` = `"<namespace>/<generation>"`, built by the client |
| `GenerationTag.command_id`, receipts, `replayed` | `request_id` (B) |
| `FenceGeneration`, `FenceGenerations` | `CloseOwners` (D) |
| tombstones on disk (bbolt) | `--owner-close-dir` (append-only file) |
| `GetGeneration`, `GetGenerations` | `ListOwnerPaths` (C, D) |
| tagged `DeletePath` by UUID | `DeletePath` by UUID with `owner` (A, C) |
| `DeleteGenerations` (only fenced, per-generation refusal) | `DeleteOwnerPaths{only_closed: true}` (C, D) |
| replacement protection | built into C |
| `LookupDestinations` | ListPath with exact NLRI keys (E), `Path.owner` and `Path.uuid` (A, C) |
| incarnation | `incarnation` in C and D answers |
| `--generation-fence-require-tag` | `--require-owner` (F) |
| bounded delete steps, `steps` | same, in C |

### What the generic version loses, and how a client gets it back

1. **Instance ID check on every answer.** The fork returns the configured
   speaker ID on every answer and refuses commands naming another one.
   Generic: dropped; a server identity check is a deployment concern. Client
   side: one gRPC connection per server address, checked once with `GetBgp`
   (router ID) and on every reconnect, plus `incarnation` on owner answers.
   Possible small follow-up: an optional `expected_router_id` precondition on
   writes and owner calls, refused when it differs. Generic enough to propose
   later if needed.
2. **History of a generation's paths** (REPLACED and DELETED entries, who
   replaced a path, `ended_at`). Generic `ListOwnerPaths` lists current paths
   only; a path that another write took over simply stops being listed.
   Client side: the client records which keys it wrote; ListPath with exact
   keys (E) shows who owns each key now, through `Path.owner`.
3. **Per-command receipts listed by generation** (`GetGeneration.receipts`).
   Generic: receipts answer retries; they are not listed. Client side: each
   owned path carries the `request_id` that wrote it, so a lost answer is
   recovered by `ListOwnerPaths`; the client's own log keeps the command
   history.
4. **Refusal of replays after a fence** is kept (D refuses every write naming
   a closed owner).
5. **Generation numbers as a type** (uint64, not 0). Generic owners are
   strings; the client encodes the generation in the owner string.
6. **Receipt lifetime.** The fork keeps receipts while a generation owns a
   path, plus a bounded number of settled generations. Generic: a time and
   count window. The client must retry within `--request-id-retention`; after
   that it reads `ListOwnerPaths` instead of retrying blindly.
7. **ListPath answer order and empty entries.** `LookupDestinations` answered
   in request order with an entry per key; ListPath does neither. Client side:
   match by NLRI; a missing key is an absent destination.
8. **Existing tombstones.** The fork's bbolt file does not carry over. Client
   side: the client re-closes, from its own records, every generation it has
   withdrawn, before it moves its speakers to the upstream build. `CloseOwners`
   is idempotent, so this can run at every start.
9. **Closed-set memory.** A client that closes one owner per generation keeps
   every closed owner in the server's memory (about 100 bytes each, an
   estimate). At 1,000 closes a day that is about 36 MB a year. If that
   matters, a follow-up can add owner prefixes ("close every owner
   `<namespace>/` up to N"), which a generation-based client could use; it is
   not proposed now because it is not generic.

### Semantics the client relies on, and where they live

- One point in the mutation order on every connection: D (closed-set check
  in the mutation closure).
- Idempotence: B (requests), C (`DeleteOwnerPaths`), D (`CloseOwners`).
- All-or-nothing validation: C and D batch calls.
- Per-owner refusal inside a batch: D (`only_closed`).
- Replacement protection: C.
- Restart survival of closes: D with `--owner-close-dir`.

## Open questions for review

1. A's behavior change (a UUID deletes only its own family) could be gated
   behind a flag for a release if upstream prefers. The design treats the old
   behavior as a bug.
2. B's retention defaults (10 minutes, 100000 receipts) are guesses.
3. D refuses replays of a closed owner; returning the receipt instead is also
   safe. Which one upstream prefers.
4. D's file format versus bbolt (memory against a dependency).
5. Whether C should also accept a single `owner` field on `ListPath` as a
   filter, instead of a separate `ListOwnerPaths`. A separate call keeps the
   per-owner answer shape and the batch limit; a ListPath filter reuses an
   existing call but would walk the table, unless it uses the index.
