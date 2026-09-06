# pgmq_flutter_demo

Flutter demo and consumer-verification app for
[`postgres_pgmq`](../..).

- `lib/pgmq_sweep.dart` — pure-Dart sweep exercising the **full**
  `postgres_pgmq` API (extension setup, queues, send/read/pop, batches,
  long-polling, FIFO groups, topics, metrics, notifications, transactions,
  typed payloads). Shared by the app and the test.
- `lib/main.dart` — Material UI: connect to a database, run the full sweep,
  or poke individual operations (send / read / pop / metrics) with a live log.
- `test/sweep_test.dart` — runs the sweep via `flutter test` against
  `PGMQ_TEST_DSN` (skipped without it).

> `package:postgres` uses TCP sockets, so this demo targets
> mobile/desktop — **not Flutter web**.

## Run the verification

```sh
# from the repo root
PGMQ_HOST_PORT=5434 docker compose up -d
cd example/pgmq_flutter_demo
flutter pub get
PGMQ_TEST_DSN='postgresql://postgres:postgres@localhost:5434/postgres' \
  flutter test
```

## Run the app (macOS)

```sh
flutter run -d macos
```

Defaults point at `localhost:5434`; adjust host/port/user in the UI.
The official PGMQ image has no SSL, so the app connects with
`SslMode.disable`.
