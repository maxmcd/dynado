# dynado

To install dependencies:

```bash
bun install
```

To run:

```bash
bun run index.ts
```

This project was created using `bun init` in bun v1.3.1. [Bun](https://bun.com) is a fast all-in-one JavaScript runtime.

## Maelstrom testing

To exercise Dynado with Jepsen's Maelstrom workloads, start with the lin-kv
adapter or try the transactional register workload:

```bash
maelstrom test -w lin-kv --bin ./test/maelstrom/run-lin-kv --time-limit 10 --node-count 1 --concurrency 2n

maelstrom test -w txn-rw-register --bin ./test/maelstrom/run-txn-rw-register --time-limit 10 --node-count 1 --concurrency 2n
```

See `docs/maelstrom.md` for details, environment variables, and more options.
