# Generation fence

A client that replaces its routes in generations can have gobgpd guarantee
that a generation it has stopped never writes again, from any connection
and however late a request arrives. This page describes the contract.

## Why

`AddPath` queues its work on the management loop, which runs one operation
at a time. A request can wait there behind a long operation, can come from
any client connection, and can be applied after its client gave up on it:
the server's copy of a deadline starts when the request arrives, so it ends
a little after the client's. A client that withdraws a generation cannot
tell from its side whether a request of that generation is still on its
way. The fence moves the question to the one place that orders every
mutation, the management loop.

## Enabling it

```shell
gobgpd --generation-fence-dir /var/lib/gobgp/fence \
  --generation-fence-require-tag ipv4-unicast \
  --generation-fence-instance-id gobgp-1
```

| Flag | Default | Meaning |
| --- | --- | --- |
| `--generation-fence-dir` | none | Directory of the fence store. Without it the fence is off. |
| `--generation-fence-require-tag` | none | Family that untagged `AddPath`, `AddPathStream` and `DeletePath` may not write. Repeatable. |
| `--generation-fence-instance-id` | none | Instance ID of this server. A command naming another one is refused. |
| `--generation-fence-cache-size` | 65536 | Fence lookups kept in memory. |
| `--generation-fence-retention` | 65536 | Generations without a current path kept in memory with their receipts. |

As a library, pass `server.GenerationFenceOption(server.GenerationFenceConfig{...})`
to `server.NewBgpServer`. `StartBgp` opens the store and fails when it
cannot, and every write waits for `StartBgp`, so no write runs before the
fences are restored.

## Commands

A write is a command of a generation when it carries a `GenerationTag`: an
optional namespace, the generation (not 0), a command ID, and an optional
instance ID. The tag is an optional field of `AddPathRequest`,
`AddPathStreamRequest` and `DeletePathRequest`, so existing clients are not
affected.

- A tagged `AddPath`, or a tagged `AddPathStream` message, is refused with
  `FAILED_PRECONDITION` when its generation is fenced. The check runs in the
  management loop, in the same step as the mutation.
- A command ID that already has a receipt is not applied again: `AddPath`
  returns the first uuid with `replayed` set. The same command ID with
  another payload is refused with `ALREADY_EXISTS`.
- All paths of a tagged command are applied, or none is. A tagged path
  cannot be a withdrawal.
- A tagged `DeletePath` names one uuid of the same generation and is
  accepted after the fence. Its `outcome` is `DELETED`, `ALREADY_DELETED`,
  or `REPLACED` when another write took the route key, in which case the path
  there is left alone. A uuid of another generation is refused with
  `FAILED_PRECONDITION`, an unknown one with `NOT_FOUND`.
- When the server has an instance ID, a command that names another one is
  refused with `FAILED_PRECONDITION`. A command that names none is accepted.

## FenceGeneration

`FenceGeneration(namespace, generation, instance_id)` writes the fence and
syncs it to disk before it answers. The answer means that every command of
the generation the server ran before it is applied and listed in the
returned status, and that every later one is refused, on any connection. It
is idempotent: fencing a fenced generation returns `newly_fenced` false and
keeps the first fence time.

## Reading back

`GetGeneration(namespace, generation)` returns the state (`UNKNOWN`, `LIVE`,
`FENCED`), the receipts, and every uuid the generation was given, each with
its route key (family, NLRI, path identifier, VRF), its state (`CURRENT`,
`REPLACED`, `DELETED`), the write that replaced it, and `present`, read from
the global table at the route key. It reads only those destinations, and it
runs in the management loop, so it sees every mutation acknowledged before
it.

Receipts and uuids are kept in memory and belong to one incarnation of the
server, a random ID taken at `StartBgp` and returned by `GetGeneration`,
`FenceGeneration` and a tagged `AddPath`. Local paths do not survive a
restart either, so a generation a new incarnation does not know owns nothing
in its table. Fences survive the restart.

Every answer of `GetGeneration`, `FenceGeneration` and `AddPath` carries the
configured `instance_id`, empty when there is none, so a client can check
which server answered.

## Untagged writes

Untagged writes behave as before, with these additions:

- an untagged write that takes or deletes the route key of a tagged path
  marks that path `REPLACED` or `DELETED`;
- a delete by uuid of an untagged path, which matches every family with the
  same prefix and path identifier, does not take a path that a tagged
  `AddPath` wrote;
- with `--generation-fence-require-tag`, untagged writes to the listed
  families are refused with `FAILED_PRECONDITION`. Routes from zebra are not
  API writes and are not affected.

## Fence store

`generation-fence.db` under the configured directory is a bbolt file, format
1.

| Bucket | Key | Value |
| --- | --- | --- |
| `meta` | `format` | `0x01` |
| `tombstones` | uint16 BE namespace length, namespace, uint64 BE generation | `0x01`, int64 BE fence time in Unix nanoseconds (UTC), uint16 BE instance ID length, instance ID |

Fences are never removed. A lookup is a B+tree read; a bounded LRU cache
answers repeated lookups. The file is locked while a server holds it, so two
servers cannot share a directory.
