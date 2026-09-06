import 'package:postgres/postgres.dart';
import 'package:postgres_pgmq/postgres_pgmq.dart';

/// Usage with a connection [Pool]. The pool is created and owned by the
/// application; [Pgmq] only borrows sessions from it and never closes it.
Future<void> main() async {
  final pool = Pool.withEndpoints(
    [
      Endpoint(
        host: 'localhost',
        database: 'postgres',
        username: 'postgres',
        password: 'postgres',
      ),
    ],
    settings: const PoolSettings(maxConnectionCount: 5),
  );

  try {
    final pgmq = Pgmq(pool);
    await pgmq.ensureExtension();

    const queue = 'jobs';
    await pgmq.createQueue(queue);

    // Batch send.
    final ids = await pgmq.sendBatch(queue, [
      {'job': 'resize-image', 'id': 1},
      {'job': 'send-email', 'id': 2},
      {'job': 'generate-pdf', 'id': 3},
    ]);
    print('sent $ids');

    // Long-poll for work (server-side wait, up to 5s here).
    final batch = await pgmq.readWithPoll<Map<String, dynamic>>(
      queue,
      qty: 10,
      maxPollSeconds: 5,
      timeout: const Duration(seconds: 10),
    );
    for (final message in batch) {
      print('processing ${message.message}');
      await pgmq.archive(queue, message.msgId);
    }

    for (final m in await pgmq.metricsAll()) {
      print('${m.queueName}: ${m.queueLength} visible=${m.queueVisibleLength}');
    }

    await pgmq.dropQueue(queue);
  } finally {
    await pool.close();
  }
}
