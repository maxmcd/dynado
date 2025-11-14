# Maelstrom Integration

The repository now ships a tiny Maelstrom-facing binary so we can reuse the
Dynado storage stack under the [`lin-kv` workload](../../jepsen-io/maelstrom/doc/workloads.md#workload-lin-kv).
It is intentionally simple—reads, writes, and compare-and-set commands go
straight through Dynado's `ShardedSQLiteStorage`, giving us confidence that the
core storage path behaves under Jepsen's workload generator.

## Requirements

1. Install Maelstrom (download the `maelstrom.tar.bz2` release and follow
   `../../jepsen-io/maelstrom/doc/01-getting-ready/index.md`).
2. Install Bun and project dependencies: `bun install`.

## Running the lin-kv workload

```bash
# from the repo root
maelstrom test \
  -w lin-kv \
  --bin ./test/maelstrom/run-lin-kv \
  --time-limit 10 \
  --node-count 1 \
  --rate 10 \
  --concurrency 2n
```

Flags can be tuned just like the Maelstrom tutorials (e.g., add nemeses or
increase `--node-count`). The adapter automatically:

- Creates its own storage directory rooted at `${MAELSTROM_DATA_DIR:-.maelstrom-data}/node-${PID}` so parallel Maelstrom nodes don't fight over SQLite files.
- Provisions a Dynamo-style table (`lin_kv` with `pk` hash key) the first time it starts.
- Maps Maelstrom `write`, `read`, and `cas` RPCs onto Dynado's storage layer,
  including Dynamo-style compare-and-set semantics for CAS.

## Running the txn-rw-register workload

```bash
maelstrom test \
  -w txn-rw-register \
  --bin ./test/maelstrom/run-txn-rw-register \
  --time-limit 10 \
  --node-count 1 \
  --rate 10 \
  --concurrency 2n
```

Each `txn` request is executed atomically inside the adapter: it runs all reads,
stages writes, then commits them to Dynado once the transaction completes, so
Maelstrom observes the same semantics as a serial single-node state machine.

## Helpful environment variables

| Variable | Default | Purpose |
| --- | --- | --- |
| `MAELSTROM_DATA_DIR` | `.maelstrom-data` | Base directory for per-node shard databases. Set this to a tmp dir if you don't want files in the repo. |
| `MAELSTROM_SHARD_COUNT` | `1` | Number of SQLite shards to instantiate behind the adapter. |

After a run you can inspect `MAELSTROM_DATA_DIR` to see the SQLite files that
backed the workload, or delete the directory to start fresh.
