@Tags(['integration'])
library;

import 'dart:io';

import 'package:postgres/postgres.dart';
import 'package:postgres_pgmq/postgres_pgmq.dart';
import 'package:test/test.dart';

/// Integration tests against the official PGMQ Docker image.
///
/// Run with a live database:
///
/// ```sh
/// docker run -d --name pgmq -e POSTGRES_PASSWORD=postgres -p 5432:5432 \
///   ghcr.io/pgmq/pg18-pgmq:latest
/// PGMQ_TEST_DSN='postgresql://postgres:postgres@localhost:5432/postgres' \
///   dart test --tags integration
/// ```
///
/// Without `PGMQ_TEST_DSN` the whole suite is skipped.
///
/// The official PGMQ image does not enable SSL, so a plain DSN connects with
/// SSL disabled. To override, add `?sslmode=require` (or `verify-ca` /
/// `verify-full`) to the DSN, which is honored as-is.
final String _dsn = Platform.environment['PGMQ_TEST_DSN'] ?? '';

Future<Connection> _open() {
  final uri = Uri.parse(_dsn);
  if (uri.queryParameters.containsKey('sslmode')) {
    return Connection.openFromUrl(_dsn);
  }
  final userInfo = uri.userInfo.split(':');
  final username = userInfo.firstWhere(
    (s) => s.isNotEmpty,
    orElse: () => 'postgres',
  );
  return Connection.open(
    Endpoint(
      host: uri.host.isEmpty ? 'localhost' : uri.host,
      port: uri.hasPort ? uri.port : 5432,
      database: uri.pathSegments.isEmpty ? 'postgres' : uri.pathSegments.last,
      username: Uri.decodeComponent(username),
      password: userInfo.length > 1
          ? Uri.decodeComponent(userInfo.sublist(1).join(':'))
          : null,
    ),
    settings: const ConnectionSettings(sslMode: SslMode.disable),
  );
}

String _queue(String suffix) =>
    'dart_it_${suffix}_${DateTime.now().microsecondsSinceEpoch % 100000}';

void main() {
  group(
    'pgmq integration',
    () {
      Connection? connection;
      late Pgmq pgmq;

      setUpAll(() async {
        connection = await _open();
        pgmq = Pgmq(connection!);
        await pgmq.ensureExtension();
        expect(await pgmq.extensionExists(), isTrue);
      });

      tearDownAll(() async {
        // Nullable: setUpAll may have failed before connecting.
        await connection?.close();
      });

      group('queue lifecycle', () {
        test('create / list / purge / drop', () async {
          final q = _queue('life');
          await pgmq.createQueue(q);
          // idempotent re-create
          await pgmq.createQueue(q);
          final names =
              (await pgmq.listQueues()).map((e) => e.queueName).toSet();
          expect(names, contains(q));

          await pgmq.send(q, {'n': 1});
          await pgmq.send(q, {'n': 2});
          expect(await pgmq.purgeQueue(q), 2);
          expect(await pgmq.dropQueue(q), isTrue);
          expect(await pgmq.dropQueue(q), isFalse);
        });

        test('unlogged queue', () async {
          final q = _queue('unlogged');
          await pgmq.createUnloggedQueue(q);
          final record =
              (await pgmq.listQueues()).firstWhere((e) => e.queueName == q);
          expect(record.isUnlogged, isTrue);
          expect(await pgmq.dropQueue(q), isTrue);
        });
      });

      group('send / read / delete', () {
        test('round trip with headers', () async {
          final q = _queue('roundtrip');
          await pgmq.createQueue(q);
          try {
            final id = await pgmq.send(
              q,
              {'hello': 'world'},
              headers: {'source': 'dart-it'},
            );
            expect(id, greaterThan(0));

            final rows = await pgmq.read<Map<String, dynamic>>(q);
            expect(rows, hasLength(1));
            expect(rows.first.message, {'hello': 'world'});
            expect(rows.first.headers, {'source': 'dart-it'});

            expect(await pgmq.delete(q, id), isTrue);
            expect(await pgmq.delete(q, id), isFalse);
            expect(await pgmq.read(q), isEmpty);
          } finally {
            await pgmq.dropQueue(q);
          }
        });

        test('batch send / batch delete', () async {
          final q = _queue('batch');
          await pgmq.createQueue(q);
          try {
            final ids = await pgmq.sendBatch(q, [
              {'n': 1},
              {'n': 2},
              {'n': 3},
            ]);
            expect(ids, hasLength(3));
            final rows = await pgmq.read(q, qty: 10);
            expect(rows, hasLength(3));
            final deleted = await pgmq.deleteBatch(
              q,
              rows.map((m) => m.msgId).toList(),
            );
            expect(deleted, hasLength(3));
          } finally {
            await pgmq.dropQueue(q);
          }
        });

        test('pop removes the message', () async {
          final q = _queue('pop');
          await pgmq.createQueue(q);
          try {
            await pgmq.send(q, {'a': 1});
            final popped = await pgmq.pop<Map<String, dynamic>>(q);
            expect(popped?.message, {'a': 1});
            expect(await pgmq.pop(q), isNull);
          } finally {
            await pgmq.dropQueue(q);
          }
        });

        test('delayed message becomes visible later', () async {
          final q = _queue('delay');
          await pgmq.createQueue(q);
          try {
            await pgmq.send(
              q,
              {'late': true},
              delay: const Duration(seconds: 2),
            );
            expect(await pgmq.read(q), isEmpty);
            final rows = await pgmq.readWithPoll(
              q,
              maxPollSeconds: 10,
              pollIntervalMs: 200,
              timeout: const Duration(seconds: 20),
            );
            expect(rows, hasLength(1));
            await pgmq.delete(q, rows.first.msgId);
          } finally {
            await pgmq.dropQueue(q);
          }
        });

        test('archive moves to the archive table', () async {
          final q = _queue('archive');
          await pgmq.createQueue(q);
          try {
            final id = await pgmq.send(q, {'a': 1});
            expect(await pgmq.archive(q, id), isTrue);
            expect(await pgmq.read(q), isEmpty);
          } finally {
            await pgmq.dropQueue(q);
          }
        });

        test('setVt extends visibility', () async {
          final q = _queue('vt');
          await pgmq.createQueue(q);
          try {
            await pgmq.send(q, {'a': 1});
            final first = (await pgmq.read(q)).single;
            // consumed lease; extend it so a second immediate read stays empty.
            await pgmq.setVt(q, first.msgId,
                delay: const Duration(seconds: 30));
            expect(await pgmq.read(q), isEmpty);
            await pgmq.delete(q, first.msgId);
          } finally {
            await pgmq.dropQueue(q);
          }
        });

        test('metrics reflect activity', () async {
          final q = _queue('metrics');
          await pgmq.createQueue(q);
          try {
            await pgmq.sendBatch(q, [
              {'n': 1},
              {'n': 2},
            ]);
            final m = await pgmq.metrics(q);
            expect(m.queueName, q);
            expect(m.queueLength, 2);
            expect(m.totalMessages, greaterThanOrEqualTo(2));
            expect(
              (await pgmq.metricsAll()).map((e) => e.queueName),
              contains(q),
            );
          } finally {
            await pgmq.dropQueue(q);
          }
        });
      });

      group('fifo', () {
        test('grouped reads respect groups', () async {
          // Two queues: the batch read leases group 'a' for its VT, so the
          // heads assertion runs on an independent queue.
          final q = _queue('fifo');
          final qh = _queue('fifo_heads');
          await pgmq.createQueue(q);
          await pgmq.createQueue(qh);
          try {
            await pgmq.createFifoIndex(q);
            for (final target in [q, qh]) {
              await pgmq.send(
                target,
                {'n': 1},
                headers: {'x-pgmq-group': 'a'},
              );
              await pgmq.send(
                target,
                {'n': 2},
                headers: {'x-pgmq-group': 'b'},
              );
              await pgmq.send(
                target,
                {'n': 3},
                headers: {'x-pgmq-group': 'a'},
              );
            }

            final batch = await pgmq.readGrouped(q, qty: 2);
            expect(batch, hasLength(2));
            expect(
              batch.map((m) => m.headers?['x-pgmq-group']).toSet(),
              {'a'},
            );

            final heads = await pgmq.readGroupedHead(qh, qty: 10);
            expect(
              heads.map((m) => m.headers?['x-pgmq-group']).toSet(),
              containsAll({'a', 'b'}),
            );
          } finally {
            await pgmq.dropQueue(q);
            await pgmq.dropQueue(qh);
          }
        });
      });

      group('topics', () {
        test('bind / send_topic / unbind', () async {
          final q = _queue('topic');
          await pgmq.createQueue(q);
          try {
            await pgmq.bindTopic('orders.*', q);
            final bindings = await pgmq.listTopicBindings(queue: q);
            expect(bindings.map((b) => b.pattern), contains('orders.*'));

            final routes = await pgmq.testRouting('orders.created');
            expect(routes.map((r) => r.queueName), contains(q));

            final fanout = await pgmq.sendTopic(
              'orders.created',
              {'id': 1},
            );
            expect(fanout, greaterThanOrEqualTo(1));
            final rows = await pgmq.read(q);
            // NB: decoded jsonb maps need deep-equality; Map.== is identity.
            expect(
              rows.map((m) => m.message),
              contains(equals({'id': 1})),
            );

            expect(await pgmq.unbindTopic('orders.*', q), isTrue);
          } finally {
            await pgmq.dropQueue(q);
          }
        });
      });

      group('transactions', () {
        test('Pgmq works on TxSession and rolls back', () async {
          final q = _queue('tx');
          await pgmq.createQueue(q);
          try {
            await connection!.runTx((tx) async {
              final txPgmq = Pgmq(tx);
              await txPgmq.send(q, {'n': 1});
              // Visible inside the transaction.
              expect(await txPgmq.read(q), hasLength(1));
              // Force a rollback.
              throw const TestRollback();
            });
          } on TestRollback {
            // expected
          }
          expect(await pgmq.read(q), isEmpty);
          await pgmq.dropQueue(q);
        });
      });

      group('notifications', () {
        test('enable / list / disable', () async {
          final q = _queue('notify');
          await pgmq.createQueue(q);
          try {
            await pgmq.enableNotify(q, throttleIntervalMs: 0);
            final throttles = await pgmq.listNotifyThrottles();
            expect(throttles.map((t) => t.queueName), contains(q));
            await pgmq.updateNotify(q, 100);
            await pgmq.disableNotify(q);
          } finally {
            await pgmq.dropQueue(q);
          }
        });
      });
    },
    skip: _dsn.isEmpty ? 'Set PGMQ_TEST_DSN to run integration tests.' : false,
  );
}

class TestRollback implements Exception {
  const TestRollback();
}
