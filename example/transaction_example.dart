import 'package:postgres/postgres.dart';
import 'package:postgres_pgmq/postgres_pgmq.dart';

/// Transactions: pass the [TxSession] to [Pgmq] to run queue operations
/// atomically with the rest of your database work.
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
    await pgmq.createQueue('orders');
    await pgmq.createQueue('order_events');

    await connection.runTx((tx) async {
      // Same PGMQ API, now transactional.
      final txPgmq = Pgmq(tx);

      final orderId = await txPgmq.send('orders', {'total': 42});
      await txPgmq.send('order_events', {'order_msg_id': orderId});

      // Regular SQL works alongside queue operations.
      await tx.execute(
        Sql.named('SELECT @id:int8'),
        parameters: {'id': orderId},
      );
      // Commit happens automatically; an exception would roll everything back.
    });

    final orders = await pgmq.read<Map<String, dynamic>>('orders');
    print('orders: ${orders.map((m) => m.message).toList()}');

    await pgmq.dropQueue('orders');
    await pgmq.dropQueue('order_events');
  } finally {
    await connection.close();
  }
}
