# Generation fence

A route writer that steers traffic in generations can make the speaker
enforce that a generation it has stopped never writes again. This page is the
contract a client relies on.

## Why

`AddPath` queues its work on the management loop, which runs one operation at
a time. A request can wait there behind a long operation, can come from any
client connection, and can still be applied after its client gave up on it.
A writer that withdraws a generation cannot tell from the client side whether
such a request is still in flight. The fence moves that question to the one
place that orders every mutation: the management loop.

## Enabling it

```shell
gobgpd --generation-fence-dir /var/lib/gobgp/fence \
  --generation-fence-require-tag ipv4-unicast \
  --generation-fence-require-tag ipv4-labelled-unicast \
  --generation-fence-speaker-id speaker-1
```

| Flag | Default | Meaning |
| --- | --- | --- |
| `--generation-fence-dir` | none | Directory of the tombstone store. Without it the fence is off. |
| `--generation-fence-require-tag` | none | Family that untagged `AddPath`, `AddPathStream` and `DeletePath` may not write. Repeatable. |
| `--generation-fence-speaker-id` | none | When set, a command naming another speaker is refused. |
| `--generation-fence-cache-size` | 65536 | Tombstone lookups kept in memory. |
| `--generation-fence-retention` | 65536 | Generations without a current path kept in memory with their receipts. |

As a library, pass `server.GenerationFenceOption(server.GenerationFenceConfig{...})`
to `server.NewBgpServer`. `StartBgp` opens the store and fails when it cannot,
and every write waits for `StartBgp`, so no write runs before the tombstones
are restored.

## Commands

A write is a command of a generation when it carries a `GenerationTag`:
tenant, generation (not 0), command ID and speaker. The tag is an optional
field of `AddPathRequest`, `AddPathStreamRequest` and `DeletePathRequest`, so
existing clients are not affected.

- A tagged `AddPath` or a tagged `AddPathStream` message is refused with
  `FAILED_PRECONDITION` when the generation is fenced. The check runs in the
  management loop, in the same step as the mutation.
- A command ID that already has a receipt is not applied again: `AddPath`
  returns the first UUID with `replayed` set. The same command ID with
  another payload is refused with `ALREADY_EXISTS`.
- All paths of a tagged command are applied, or none is. A tagged write may
  not be a withdrawal.
- A tagged `DeletePath` names one UUID of the same generation. It is accepted
  after the fence. Its `outcome` is `DELETED`, `ALREADY_DELETED`, or `REPLACED`
  when another write took the route key, in which case the path there is left
  alone. A UUID of another generation is refused with `FAILED_PRECONDITION`,
  an unknown one with `NOT_FOUND`.

## FenceGeneration

`FenceGeneration(tenant, generation)` writes a tombstone and syncs it to disk
before it answers. Its answer means that every command of the generation the
server ran before it is applied and listed in the returned status, and every
later one is refused, on any connection. It is idempotent: `newly_fenced` is
false and the first fence time is kept when the generation was fenced already.

## Reading back

- `GetGeneration(tenant, generation)` returns the state (`UNKNOWN`, `LIVE`,
  `FENCED`), the receipts, and every UUID the generation was given, each with
  its family, NLRI, path identifier, state (`CURRENT`, `REPLACED`, `DELETED`),
  who replaced it, and `present`, read from the RIB at the route key.
- `LookupDestinations(keys)` returns every path the global RIB holds at each
  exact destination, local and learned, with the UUID and generation of each
  local path a tagged `AddPath` owns. It works for every family, including the
  ones `ListPath` cannot filter by prefix.

Both read only the destinations they name, and both run in the management
loop, so they see every mutation acknowledged before them.

Every answer of `GetGeneration`, `FenceGeneration` (in its status),
`LookupDestinations` and `AddPath` carries `speaker_id`, the value of
`--generation-fence-speaker-id`, empty when it is not set. A client compares
it with the speaker it meant to ask, so it learns which speaker answered
instead of trusting its own configuration.

Receipts and UUIDs live in memory and belong to one incarnation of the
speaker, a random ID taken at `StartBgp` and returned by these calls and by a
tagged `AddPath`. Local paths do not survive a restart either, so a
generation unknown to a new incarnation owns nothing in its RIB.

## Untagged writes

Untagged writes behave as before, with these additions:

- an untagged write that takes or deletes the route key of a tagged path marks
  that path `REPLACED` or `DELETED`;
- a legacy delete by UUID, which matches every family with the same prefix,
  does not take a path that a tagged `AddPath` wrote;
- with `--generation-fence-require-tag`, untagged writes to the listed
  families are refused with `FAILED_PRECONDITION`.

## Tombstone store

`generation-fence.db` under the configured directory is a bbolt file, format 1.

| Bucket | Key | Value |
| --- | --- | --- |
| `meta` | `format` | `0x01` |
| `tombstones` | uint16 BE tenant length, tenant, uint64 BE generation | `0x01`, int64 BE fence time in Unix nanoseconds (UTC), uint16 BE speaker length, speaker |

Tombstones are never removed. A lookup is a B+tree read; a bounded LRU cache
answers repeated lookups. The file is locked while a speaker holds it, so two
speakers cannot share a directory.
