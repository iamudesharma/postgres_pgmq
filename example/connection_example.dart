import 'package:postgres/postgres.dart';
import 'package:postgres_pgmq/postgres_pgmq.dart';

/// Basic usage with a single [Connection].
///
/// Start a local PGMQ database first:
///
/// ```sh
/// docker run -d --name pgmq -e POSTGRES_PASSWORD=postgres -p 5432:5432 \
///   ghcr.io/pgmq/pg18-pgmq:latest
/// ```
Future<void> main() async {
  final connection = await Connection.open(
    Endpoint(
      host: 'localhost',
      database: 'postgres',
      username: 'postgres',
      password: 'postgres',
    ),
    // The official PGMQ image does not enable SSL.
    settings: const ConnectionSettings(sslMode: SslMode.disable),
  );

  try {
    // Wrap the existing connection — no new pooling or config needed.
    final pgmq = Pgmq(connection);
    await pgmq.ensureExtension();

    const queue = 'emails';
    await pgmq.createQueue(queue);

    final msgId = await pgmq.send(queue, {
      'to': 'ada@example.com',
      'subject': 'Hello from PGMQ',
    });
    print('sent $msgId');

    // Read (invisible to others for 30s) then delete.
    final message = await pgmq.readOne<Map<String, dynamic>>(queue);
    print('read ${message?.message}');
    if (message != null) {
      await pgmq.delete(queue, message.msgId);
    }

    // Or do both atomically:
    await pgmq.send(queue, {'to': 'grace@example.com'});
    final popped = await pgmq.pop<Map<String, dynamic>>(queue);
    print('popped ${popped?.message}');

    final metrics = await pgmq.metrics(queue);
    print('queue length: ${metrics.queueLength}');

    await pgmq.dropQueue(queue);
  } finally {
    await connection.close();
  }
}
