# 0.1.0

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
