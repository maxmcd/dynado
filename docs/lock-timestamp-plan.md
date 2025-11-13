# Durable Locks & Deterministic Timestamp Plan

## 1. Catalog Current Flow
- Map how `TransactionCoordinator.generateTransactionTimestamp()` feeds `Shard.prepare()` and where `ongoing_transaction_id`, `last_update_timestamp`, `lsn` are written (`src/transaction-protocol.ts`, `src/coordinator.ts`, `src/shard.ts`).
- Confirm locks are persisted to SQLite during prepare and only cleared in `commit`/`release`.

## 2. Deterministic Timestamp Redesign
- Assign a stable `coordinatorId` (e.g., hash of Durable Object name) per coordinator instance.
- Persist `{lastTimestamp, sequence}` per coordinator in durable storage (new `coordinator_state` table or DO storage key).
- Replace `generateTransactionTimestamp()` with Snowflake-style logic:
  - Read persisted state, compute `now = Date.now()`.
  - If `now > lastTimestamp`, reset `sequence = 0`; else reuse `lastTimestamp` and increment `sequence`.
  - Embed `coordinatorId` bits + sequence into a 64-bit logical timestamp.
  - Persist updated `{lastTimestamp, sequence}` before returning the value.
- Update `TransactionCoordinator.transactWrite()` to call the new generator and propagate the deterministic timestamp to shards.

## 3. Shard Locking Changes
- Remove the `req.timestamp <= last_update_timestamp` rejection in `Shard.prepare()`; rely on `ongoing_transaction_id` for exclusivity.
- Keep `lsn` increments on commit; optionally return prior `lsn` for read validation.
- Ensure `prepare`, `commit`, and `release` execute their updates in single SQLite transactions so locks survive restarts.
- Decide whether to keep `last_update_timestamp` as informational or drop the column (requires migration if removed).

## 4. Schema / Migration Work
- Add a migration (or auto-migrate on startup) to create `coordinator_state` with columns `(coordinator_id TEXT PRIMARY KEY, last_timestamp INTEGER, sequence INTEGER)`.
- If removing `last_update_timestamp`, supply default values or migration SQL to drop/ignore the column without corrupting existing data files.

## 5. Testing Strategy
- Unit-test the timestamp generator for monotonicity under concurrent calls and after simulated restarts (persisted state reload).
- Integration test with two coordinators (distinct IDs) interleaving transactions to verify shards never emit `TimestampConflict`.
- Regression test that a coordinator restart (clearing in-memory state, keeping persisted clock) yields strictly increasing timestamps.
- Transaction tests confirming locks survive failure: prepare, simulate crash before commit, ensure placeholder rows still blocked until release.
