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
/// await pgmq.createQueue('emails');
/// final id = await pgmq.send('emails', {'to': 'a@example.com'});
/// final msg = await pgmq.pop('emails');
/// ```dart
/// import 'package:postgres/postgres.dart';
/// import 'package:postgres_pgmq/postgres_pgmq.dart';
///
/// final pgmq = Pgmq(connection);
/// await pgmq.createQueue('emails');
/// final id = await pgmq.send('emails', {'to': 'a@example.com'});
/// final msg = await pgmq.pop('emails');
/// ```
library;

export 'src/exception.dart';
export 'src/models.dart';
export 'src/pgmq.dart';
