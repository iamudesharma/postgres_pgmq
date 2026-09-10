# postgres_pgmq

[![CI](https://github.com/iamudesharma/postgres_pgmq/actions/workflows/ci.yml/badge.svg)](https://github.com/iamudesharma/postgres_pgmq/actions/workflows/ci.yml)

Idiomatic Dart client for [Postgres Message Queue (PGMQ)](https://github.com/pgmq/pgmq),
built on top of [`package:postgres`](https://pub.dev/packages/postgres).

Bring your own connection: wrap any existing `Session` — a `Connection`,
a `Pool`, or a `TxSession` inside `runTx` — and get the full PGMQ API with
strongly typed models. No extra connection, pooling, or configuration layer.

```yaml
dependencies:
  postgres: ^3.5.0
  postgres_pgmq: ^0.1.0
```

```dart
import 'package:postgres/postgres.dart';
import 'package:postgres_pgmq/postgres_pgmq.dart';

final pgmq = Pgmq(connection); // or Pgmq(pool)
await pgmq.ensureExtension();
await pgmq.createQueue('emails');

final id = await pgmq.send('emails', {'to': 'ada@example.com'});
final msg = await pgmq.pop('emails'); // read + delete

await connection.runTx((tx) async {
  final txPgmq = Pgmq(tx); // joins your transaction
  await txPgmq.send('emails', {'to': 'grace@example.com'});
});
```

Targets PGMQ `v1.13.0` (Postgres 14–18; compatible with v1.12.0). The official
PGMQ SQL API is the source of truth; [`pgmq-go`](https://github.com/craigpastro/pgmq-go)
informed the thin-wrapper architecture and the official Python and Rust
clients informed feature coverage.

## Start a local PGMQ database

```sh
docker run -d --name pgmq -e POSTGRES_PASSWORD=postgres -p 5432:5432 \
  ghcr.io/pgmq/pg18-pgmq:latest
# or: docker compose up -d
```

> The official PGMQ image does not enable SSL, so local connections need
> `ConnectionSettings(sslMode: SslMode.disable)` (as in the examples) or
> `?sslmode=disable` on connection-string URLs.

## Usage

### Queues

```dart
await pgmq.ensureExtension(); // CREATE EXTENSION IF NOT EXISTS pgmq
await pgmq.extensionVersion(); // '1.13.0' (null when not installed)
await pgmq.createQueue('jobs');
await pgmq.createQueue('fast', unlogged: true);
await pgmq.createPartitionedQueue('big', partitionInterval: '10000'); // idempotent

await pgmq.listQueues();          // List<QueueRecord>
await pgmq.queueMetadata('jobs'); // QueueRecord? (single pgmq.meta lookup)
await pgmq.queueExists('jobs');   // bool
await pgmq.metrics('jobs');       // QueueMetrics
await pgmq.metricsAll();
await pgmq.purgeQueue('jobs');    // int purged
await pgmq.dropQueue('jobs');     // bool existed

// Serialize queue-level DDL with a transaction-scoped advisory lock:
await connection.runTx((tx) async {
  final txPgmq = Pgmq(tx);
  await txPgmq.acquireQueueLock('jobs');
  await txPgmq.createFifoIndex('jobs');
});
```

`createPartitionedQueue` takes the queue lock and checks `pgmq.meta` inside
`runTx`, so it is race-free and idempotent (`ifNotExists: false` opts out).
`acquireQueueLock` uses `pg_advisory_xact_lock`, so it must run inside a
transaction and is released on commit/rollback.

### Send

```dart
final id = await pgmq.send('jobs', {'job': 'email', 'id': 1});

// headers (used for FIFO groups, filtering, app metadata)
await pgmq.send('jobs', {'n': 2}, headers: {'tenant': 'acme'});

// delayed / scheduled
await pgmq.send('jobs', {'n': 3}, delay: Duration(minutes: 5));
await pgmq.send('jobs', {'n': 4}, visibleAt: DateTime.utc(2030, 1, 1));

// batch (headers list must match messages; null = no headers)
final ids = await pgmq.sendBatch('jobs', [
  {'n': 1},
  {'n': 2},
]);
```

### Read / pop

```dart
// read up to qty, invisible to others for vt
final batch = await pgmq.read<Map<String, dynamic>>('jobs', qty: 10);
final one = await pgmq.readOne('jobs');

// server-side long poll (give timeout headroom over maxPollSeconds)
final waited = await pgmq.readWithPoll('jobs',
  maxPollSeconds: 5, timeout: Duration(seconds: 10));

// read + delete (at-most-once)
final popped = await pgmq.pop('jobs');
final many = await pgmq.popMany('jobs', 10);

// experimental server-side JSON filter
final filtered = await pgmq.read('jobs', conditional: {'job': 'email'});
```

### FIFO groups

```dart
await pgmq.createFifoIndex('shipments');
await pgmq.send('shipments', {...}, headers: {'x-pgmq-group': 'order-1'});

await pgmq.readGrouped('shipments', qty: 5);          // earliest group first
await pgmq.readGroupedRr('shipments', qty: 5);        // round-robin, fair
await pgmq.readGroupedHead('shipments', qty: 10);     // one head per group
// ... each with a `*WithPoll` long-poll variant
```

### Delete / archive / visibility timeout

```dart
await pgmq.delete('jobs', msgId);            // bool
await pgmq.deleteBatch('jobs', [1, 2, 3]);   // List<int> deleted
await pgmq.archive('jobs', msgId);           // bool
await pgmq.archiveBatch('jobs', [1, 2]);     // List<int> archived

// heartbeat pattern: extend the lease while processing
await pgmq.setVt('jobs', msgId, delay: Duration(minutes: 2));
await pgmq.setVtBatch('jobs', ids, delay: Duration(minutes: 2));
```

### Typed payloads

Reads default to `Map<String, dynamic>`. Use `fromJson` / `toJson` (or
`PgmqMessage.mapPayload`) for domain models:

```dart
final orders = await pgmq.read<Order>('orders',
  fromJson: (json) => Order.fromJson((json as Map).cast()));
await pgmq.send('orders', Order(...), toJson: (o) => o.toJson());
```

### Topics

```dart
await pgmq.bindTopic('orders.#', 'billing');
await pgmq.sendTopic('orders.created', {'id': 7}); // int fan-out count
await pgmq.sendBatchTopic('orders.created', [{'id': 7}, {'id': 8}]);
await pgmq.testRouting('orders.created'); // dry-run
await pgmq.listTopicBindings(queue: 'billing');
await pgmq.unbindTopic('orders.#', 'billing');
```

Patterns: `*` matches exactly one dot-segment, `#` matches zero or more.

### Insert notifications

```dart
await pgmq.enableNotify('jobs', throttleIntervalMs: 250);

// In a long-lived connection (LISTEN needs a Connection, not a Pool):
final sub = pgmq.listenNotifyInsert('jobs').listen((_) {
  // A message was inserted; read it with the usual API.
});
// ... later:
await sub.cancel(); // UNLISTENs

await pgmq.updateNotify('jobs', 100);
await pgmq.disableNotify('jobs');
```

The raw channel remains available as `Pgmq.notifyChannelName('jobs')`
(`pgmq.q_jobs.INSERT`) for `connection.channels[...]` subscriptions.
Notifications are transient — keep a poll (`readWithPoll`) as a fallback.

## Design notes

- **Your session, your rules.** `Pgmq` accepts a `Session` and never closes
  it, never reads timeouts, and never spawns connections. Pooling,
  `runTx`, and `LISTEN` subscriptions stay exactly as `package:postgres`
  documents them.
- **Parameterized SQL only.** Queue names, payloads, and ids are bound via
  `Sql.named`; nothing user-supplied is interpolated into SQL text.
- **Errors propagate.** Server errors surface as `package:postgres`
  exceptions (inspect SQLSTATE via `PgException`); client misuse throws
  `PgmqException`. Nothing is swallowed to `null`: empty reads are `[]`
  (lists) or `null` (single-message helpers), missing `delete`/`archive`
  targets are `false`/`[]`.
- **Delays** are `Duration` (relative seconds) or `DateTime` (absolute
  `timestamptz`); the two are mutually exclusive. Note the server's single
  `send_topic` supports only relative delays.
- **Small core.** The only runtime dependency is `package:postgres`.

## Testing

```sh
dart test --exclude-tags integration  # unit tests (no database)
docker compose up -d                  # official PGMQ image
PGMQ_TEST_DSN='postgresql://postgres:postgres@localhost:5432/postgres' \
  dart test --tags integration
```

If host port 5432 is already taken (e.g. a local Postgres), start the
container on another port and point the DSN at it:

```sh
PGMQ_HOST_PORT=5434 docker compose up -d
PGMQ_TEST_DSN='postgresql://postgres:postgres@localhost:5434/postgres' \
  dart test --tags integration
```

## Examples

- `example/connection_example.dart` — single connection basics
- `example/pool_example.dart` — pool + batch + long-poll worker
- `example/transaction_example.dart` — `runTx` with `Pgmq(tx)`
- `example/topics_fifo_example.dart` — FIFO groups and topic routing
- `example/pgmq_flutter_demo/` — Flutter app (mobile/desktop) with a UI and
  a full-API automated sweep (`flutter test`), proving the package from a
  real consumer's dependency graph

## Reference

- PGMQ: https://github.com/pgmq/pgmq
- Dart postgres driver: https://pub.dev/packages/postgres
- PGMQ clients: linked from the official PGMQ repository

## License

MIT — see [LICENSE](https://github.com/iamudesharma/postgres_pgmq/blob/main/LICENSE). PGMQ itself is Apache-2.0; reference SDKs were
used for behavior and API research only, not copied.
