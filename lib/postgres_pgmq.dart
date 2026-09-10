/// Idiomatic Dart client for Postgres Message Queue (PGMQ).
///
/// Start with [Pgmq], which wraps any `package:postgres` session
/// ([Connection], [Pool] or [TxSession]):
///
/// ```dart
/// import 'package:postgres/postgres.dart';
/// import 'package:postgres_pgmq/postgres_pgmq.dart';
///
/// final pgmq = Pgmq(connection);
///
/// final emails = pgmq.queue('emails'); // queue-scoped handle
/// await emails.create();
/// final id = await emails.send({'to': 'a@example.com'});
/// final msg = await emails.pop();
/// ```
library;

export 'src/exception.dart';
export 'src/models.dart';
export 'src/pgmq.dart';
export 'src/queue.dart';
