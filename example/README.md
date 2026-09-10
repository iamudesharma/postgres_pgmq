# postgres_pgmq examples

Start a local PGMQ database:

```sh
docker run -d --name pgmq -e POSTGRES_PASSWORD=postgres -p 5432:5432 \
  ghcr.io/pgmq/pg18-pgmq:latest
```

## Dart scripts

| File | What it shows |
| --- | --- |
| [`connection_example.dart`](connection_example.dart) | Single `Connection`, send / read / pop |
| [`pool_example.dart`](pool_example.dart) | `Pool`, batch send, long-poll worker |
| [`transaction_example.dart`](transaction_example.dart) | `runTx` with `Pgmq(tx)` |
| [`topics_fifo_example.dart`](topics_fifo_example.dart) | FIFO groups and topic routing |

Run any script from the repo root:

```sh
dart run example/connection_example.dart
```

## Flutter demo

The full Flutter consumer app lives in [`pgmq_flutter_demo/`](pgmq_flutter_demo/).
It exercises the entire client API from a real app's dependency graph. See that
folder's README for setup and `flutter test` instructions.
