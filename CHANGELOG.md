# 0.1.0

- Added `acquireQueueLock`: transaction-scoped advisory queue lock
  (`pgmq.acquire_queue_lock`), matching the official Rust client and
  `pgmq-go`. Serializes queue-level DDL such as FIFO index creation.
- `createPartitionedQueue` is now idempotent by default
  (`ifNotExists: true`): it takes the queue lock and checks `pgmq.meta`
  inside `runTx` before creating, like the official Rust client. Pass
  `ifNotExists: false` for the previous direct call.
- Added `queueMetadata` / `queueExists`: single-queue metadata lookups from
  `pgmq.meta`.
- Added `extensionVersion`: the installed PGMQ extension version.
- Added `listenNotifyInsert`: broadcast `Stream<String>` of insert
  notifications for `Connection` sessions (`Pool`/`TxSession` throw).
- `QueueMetrics.defaultPartitionLength` parses the
  `default_partition_length` column added in PGMQ v1.13.0 (`null` on older
  servers).
- Targets PGMQ v1.13.0 (still compatible with v1.12.0).

# 0.0.1

- Initial release.
- Full PGMQ SQL API (`v1.12.0`) over any `package:postgres` `Session`
  (`Connection`, `Pool`, `TxSession`): extension setup, queue lifecycle
  (including unlogged and partitioned queues), send / send batch with headers
  and delays, read / long-poll / pop, grouped (FIFO) reads, delete / archive
  (single + batch), visibility timeouts, metrics, topic routing and bindings,
  insert notifications, FIFO indexes.
- Strongly typed `PgmqMessage<T>` with `toJson` / `fromJson` / `mapPayload`
  support; `Map<String, dynamic>` payloads work out of the box.
- Parameterized queries only — no SQL string interpolation of user input.
- Unit tests, Docker-based integration tests, and examples for `Connection`,
  `Pool`, transactions, FIFO and topics.
