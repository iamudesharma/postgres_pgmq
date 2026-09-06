import 'package:postgres/postgres.dart';
import 'package:postgres_pgmq/postgres_pgmq.dart';

/// FIFO message groups and topic routing.
Future<void> main() async {
  final connection = await Connection.open(
    Endpoint(
      host: 'localhost',
      database: 'postgres',
      username: 'postgres',
      password: 'postgres',
    ),
  );

  try {
    final pgmq = Pgmq(connection);
    await pgmq.ensureExtension();

    // --- FIFO groups -------------------------------------------------------
    const fifo = 'shipments';
    await pgmq.createQueue(fifo);
    await pgmq.createFifoIndex(fifo);

    await pgmq.send(
      fifo,
      {'step': 'label'},
      headers: {'x-pgmq-group': 'order-1'},
    );
    await pgmq.send(
      fifo,
      {'step': 'label'},
      headers: {'x-pgmq-group': 'order-2'},
    );
    await pgmq.send(
      fifo,
      {'step': 'pick'},
      headers: {'x-pgmq-group': 'order-1'},
    );

    // Fills from the earliest group first.
    final batch = await pgmq.readGrouped<Map<String, dynamic>>(fifo, qty: 2);
    print('grouped: ${batch.map((m) => m.message).toList()}');

    // One head message per group — good for parallel workers.
    final heads = await pgmq.readGroupedHead<Map<String, dynamic>>(
      fifo,
      qty: 10,
    );
    print('heads: ${heads.length} groups');

    // --- Topics ------------------------------------------------------------
    const billing = 'billing_events';
    await pgmq.createQueue(billing);
    await pgmq.bindTopic('orders.#', billing);

    final fanout = await pgmq.sendTopic('orders.created', {'id': 7});
    print('routed to $fanout queue(s)');

    final routed = await pgmq.pop<Map<String, dynamic>>(billing);
    print('billing got ${routed?.message}');

    await pgmq.unbindTopic('orders.#', billing);

    await pgmq.dropQueue(fifo);
    await pgmq.dropQueue(billing);
  } finally {
    await connection.close();
  }
}
