import 'package:postgres/postgres.dart';
import 'package:postgres_pgmq/postgres_pgmq.dart';

/// Queue handles: a bound queue name, typed payloads and streamed consumption.
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
    final pgmq = Pgmq(connection);
    await pgmq.ensureExtension();

    // A typed handle binds the queue name and the payload conversion once.
    final orders = pgmq.queueOf<Order>(
      'orders',
      fromJson: (json) => Order.fromJson((json! as Map).cast()),
      toJson: (order) => order.toJson(),
    );
    await orders.create();

    await orders.send(const Order(1, 'espresso'));
    await orders.send(const Order(2, 'filter'));
    await orders.send(const Order(3, 'cortado'));
    print('queued 3 orders');

    // One-shot read: acknowledge by deleting.
    final first = await orders.readOne();
    if (first != null) {
      print('read order ${first.message.id} (${first.message.item})');
      await orders.delete(first.msgId);
    }

    // Stream: emits each message as it becomes visible. `take(2)` cancels the
    // subscription once two messages have been consumed.
    final streamed =
        await orders.watch(qty: 1, maxPollSeconds: 2).take(2).toList();
    for (final message in streamed) {
      print('streamed order ${message.message.id}');
      await orders.delete(message.msgId);
    }

    print('remaining: ${(await orders.metrics()).queueLength}');
    await orders.drop();
  } finally {
    await connection.close();
  }
}

/// Domain model used with [Pgmq.queueOf].
class Order {
  /// Creates an order.
  const Order(this.id, this.item);

  /// Order id.
  final int id;

  /// Item name.
  final String item;

  /// Decodes an order from a `jsonb` payload.
  factory Order.fromJson(Map<String, dynamic> json) =>
      Order(json['id'] as int, json['item'] as String);

  /// Encodes this order as a `jsonb` payload.
  Map<String, dynamic> toJson() => {'id': id, 'item': item};
}
