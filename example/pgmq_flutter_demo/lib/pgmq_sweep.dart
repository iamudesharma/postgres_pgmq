import 'package:postgres/postgres.dart';
import 'package:postgres_pgmq/postgres_pgmq.dart';

/// One verification step of [runFullSweep].
class SweepStep {
  /// Short name of the verified area (e.g. `'topics'`).
  final String name;

  /// Whether the step passed.
  final bool ok;

  /// Human-readable detail (counts, ids, or the failure).
  final String detail;

  /// Creates a step result.
  const SweepStep(this.name, this.ok, this.detail);
}

/// Opens a [Connection] from a DSN string.
///
/// The official PGMQ image does not enable SSL, so a plain DSN connects with
/// [SslMode.disable]. Add `?sslmode=require` (or `verify-ca`/`verify-full`)
/// to honor SSL instead.
Future<Connection> openConnectionFromDsn(String dsn) {
  final uri = Uri.parse(dsn);
  if (uri.queryParameters.containsKey('sslmode')) {
    return Connection.openFromUrl(dsn);
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

/// Exercises the full `postgres_pgmq` API against a live PGMQ database.
///
/// Every area gets isolated queues (named `<prefix>_<area>[_2]`), so
/// visibility-timeout leases from one step can never leak into another.
/// All queues are dropped at the end (best effort). Steps never throw:
/// failures are collected as [SweepStep.ok] == `false`.
///
/// When [connection] is provided, a transactional rollback step runs too.
Future<List<SweepStep>> runFullSweep(
  Pgmq pgmq, {
  Connection? connection,
}) async {
  final steps = <SweepStep>[];

  Future<void> step(String name, Future<String> Function() body) async {
    try {
      steps.add(SweepStep(name, true, await body()));
    } catch (e) {
      steps.add(SweepStep(name, false, '$e'));
    }
  }

  final stamp = DateTime.now().microsecondsSinceEpoch % 1000000;
  String q(String area) => 'fl_${stamp}_$area';

  Future<void> dropAll(List<String> queues) async {
    for (final queue in queues) {
      try {
        await pgmq.dropQueue(queue);
      } catch (_) {
        // best effort cleanup
      }
    }
  }

  final created = <String>[];
  String track(String area) {
    final name = q(area);
    created.add(name);
    return name;
  }

  try {
    await step('extension', () async {
      await pgmq.ensureExtension();
      final exists = await pgmq.extensionExists();
      if (!exists) throw StateError('extension missing after ensure');
      final version = await pgmq.extensionVersion();
      if (version == null || version.isEmpty) {
        throw StateError('extensionVersion missing');
      }
      return 'installed v$version';
    });

    await step('create/list queues', () async {
      final a = track('create');
      final b = track('unlogged');
      await pgmq.createQueue(a);
      await pgmq.createQueue(a); // idempotent
      await pgmq.createUnloggedQueue(b);
      await pgmq.validateQueueName(a);
      final names = (await pgmq.listQueues()).map((e) => e.queueName).toSet();
      if (!names.contains(a) || !names.contains(b)) {
        throw StateError('queues missing from list_queues');
      }
      final unlogged =
          (await pgmq.listQueues()).firstWhere((e) => e.queueName == b);
      if (!unlogged.isUnlogged) throw StateError('unlogged flag not set');
      final meta = await pgmq.queueMetadata(a);
      if (meta == null || meta.queueName != a) {
        throw StateError('queueMetadata: $meta');
      }
      if (meta.isUnlogged) {
        throw StateError('standard queue reported as unlogged');
      }
      if (!await pgmq.queueExists(b)) {
        throw StateError('queueExists returned false');
      }
      return 'queues=$a,$b';
    });

    await step('send variants', () async {
      final queue = track('send');
      await pgmq.createQueue(queue);
      final id1 = await pgmq.send(queue, {'n': 1});
      final id2 = await pgmq.send(
        queue,
        {'n': 2},
        headers: {'tenant': 'acme'},
      );
      final id3 = await pgmq.send(
        queue,
        {'n': 3},
        delay: const Duration(seconds: 120),
      );
      final id4 = await pgmq.send(
        queue,
        {'n': 4},
        visibleAt: DateTime.now().toUtc().add(const Duration(minutes: 2)),
      );
      final id5 = await pgmq.send(
        queue,
        _Order(7),
        toJson: (o) => {'id': o.id},
      );
      for (final id in [id1, id2, id3, id4, id5]) {
        if (id <= 0) throw StateError('bad msg id $id');
      }
      return 'ids=$id1..$id5';
    });

    await step('sendBatch', () async {
      final queue = track('sendbatch');
      await pgmq.createQueue(queue);
      final ids = await pgmq.sendBatch(queue, [
        {'n': 1},
        {'n': 2},
        {'n': 3},
      ], headers: [
        {'g': 'x'},
        null,
        {'g': 'y'},
      ]);
      if (ids.length != 3) throw StateError('expected 3 ids, got $ids');
      final delayed = await pgmq.sendBatch(
        queue,
        [
          {'n': 4},
        ],
        delay: const Duration(seconds: 120),
      );
      if (delayed.length != 1) throw StateError('delayed batch failed');
      return 'ids=$ids';
    });

    await step('read variants', () async {
      final queue = track('read');
      await pgmq.createQueue(queue);
      await pgmq.send(queue, {'job': 'email'});
      await pgmq.send(
        queue,
        {'job': 'sms'},
        headers: {'tenant': 'acme'},
      );
      final batch = await pgmq.read<Map<String, dynamic>>(queue, qty: 10);
      if (batch.length != 2) {
        throw StateError('expected 2, got ${batch.length}');
      }
      if (batch.first.headers?['tenant'] != null &&
          batch.first.message['job'] == null) {
        throw StateError('bad decode: ${batch.first}');
      }
      final one = await pgmq.readOne<Map<String, dynamic>>(queue);
      // Leased by the batch read above, so nothing visible.
      if (one != null) {
        throw StateError('expected leased messages invisible');
      }

      final queue2 = track('readcond');
      await pgmq.createQueue(queue2);
      await pgmq.send(queue2, {'job': 'email'});
      await pgmq.send(queue2, {'job': 'sms'});
      final filtered = await pgmq.read<Map<String, dynamic>>(
        queue2,
        qty: 10,
        conditional: {'job': 'email'},
      );
      if (filtered.length != 1 || filtered.single.message['job'] != 'email') {
        throw StateError('conditional read failed: $filtered');
      }
      return 'batch=${batch.length} conditional=1';
    });

    await step('readWithPoll', () async {
      final queue = track('poll');
      await pgmq.createQueue(queue);
      await pgmq.send(
        queue,
        {'late': true},
        delay: const Duration(seconds: 2),
      );
      if ((await pgmq.read(queue)).isNotEmpty) {
        throw StateError('delayed message visible too early');
      }
      final rows = await pgmq.readWithPoll<Map<String, dynamic>>(
        queue,
        maxPollSeconds: 10,
        pollIntervalMs: 200,
        timeout: const Duration(seconds: 20),
      );
      if (rows.length != 1 || rows.single.message['late'] != true) {
        throw StateError('poll did not return delayed message');
      }
      return 'polled=1';
    });

    await step('pop', () async {
      final queue = track('pop');
      await pgmq.createQueue(queue);
      await pgmq.send(queue, {'a': 1});
      await pgmq.send(queue, {'a': 2});
      final first = await pgmq.pop<Map<String, dynamic>>(queue);
      if (first?.message['a'] != 1) throw StateError('bad pop: $first');
      final rest = await pgmq.popMany<Map<String, dynamic>>(queue, 10);
      if (rest.length != 1 || rest.single.message['a'] != 2) {
        throw StateError('bad popMany: $rest');
      }
      if (await pgmq.pop(queue) != null) {
        throw StateError('expected empty queue');
      }
      return 'popped=2';
    });

    await step('delete/archive', () async {
      final queue = track('del');
      await pgmq.createQueue(queue);
      final ids = await pgmq.sendBatch(queue, [
        {'n': 1},
        {'n': 2},
        {'n': 3},
      ]);
      if (!await pgmq.delete(queue, ids[0])) {
        throw StateError('delete single failed');
      }
      if (await pgmq.delete(queue, ids[0])) {
        throw StateError('double delete should be false');
      }
      final deleted = await pgmq.deleteBatch(queue, ids.sublist(1));
      if (deleted.length != 2) throw StateError('deleteBatch: $deleted');

      final queue2 = track('arch');
      await pgmq.createQueue(queue2);
      final aids = await pgmq.sendBatch(queue2, [
        {'n': 1},
        {'n': 2},
      ]);
      if (!await pgmq.archive(queue2, aids[0])) {
        throw StateError('archive single failed');
      }
      final archived = await pgmq.archiveBatch(queue2, aids.sublist(1));
      if (archived.length != 1) throw StateError('archiveBatch: $archived');
      if ((await pgmq.read(queue2)).isNotEmpty) {
        throw StateError('archived messages still visible');
      }
      return 'deleted=3 archived=2';
    });

    await step('setVt', () async {
      final queue = track('vt');
      await pgmq.createQueue(queue);
      await pgmq.send(queue, {'a': 1});
      final first = (await pgmq.read(queue)).single;
      final updated = await pgmq.setVt(
        queue,
        first.msgId,
        delay: const Duration(seconds: 60),
      );
      if (updated == null || updated.msgId != first.msgId) {
        throw StateError('setVt returned $updated');
      }
      if ((await pgmq.read(queue)).isNotEmpty) {
        throw StateError('lease not extended');
      }
      await pgmq.send(queue, {'a': 2});
      final more = await pgmq.read(queue, qty: 10);
      final extended = await pgmq.setVtBatch(
        queue,
        more.map((m) => m.msgId).toList(),
        delay: const Duration(seconds: 60),
      );
      if (extended.length != more.length) {
        throw StateError('setVtBatch: $extended');
      }
      await pgmq.delete(queue, first.msgId);
      return 'extended=${extended.length + 1}';
    });

    await step('metrics', () async {
      final queue = track('metrics');
      await pgmq.createQueue(queue);
      await pgmq.sendBatch(queue, [
        {'n': 1},
        {'n': 2},
      ]);
      final m = await pgmq.metrics(queue);
      if (m.queueLength != 2) throw StateError('metrics: $m');
      final all = await pgmq.metricsAll();
      if (!all.any((e) => e.queueName == queue)) {
        throw StateError('metricsAll missing queue');
      }
      return 'length=${m.queueLength} total=${m.totalMessages}';
    });

    await step('fifo', () async {
      // One queue per read style: each read leases messages for its VT,
      // which would hide group heads from the other reads.
      final queue = track('fifo');
      final queueRr = track('fiforr');
      final queueHeads = track('fifoheads');
      await pgmq.createQueue(queue);
      await pgmq.createQueue(queueRr);
      await pgmq.createQueue(queueHeads);
      await pgmq.createFifoIndex(queue);
      for (final target in [queue, queueRr, queueHeads]) {
        await pgmq.send(target, {'n': 1}, headers: {'x-pgmq-group': 'a'});
        await pgmq.send(target, {'n': 2}, headers: {'x-pgmq-group': 'b'});
        await pgmq.send(target, {'n': 3}, headers: {'x-pgmq-group': 'a'});
      }
      final batch = await pgmq.readGrouped(queue, qty: 2);
      if (batch.length != 2 ||
          batch.any((m) => m.headers?['x-pgmq-group'] != 'a')) {
        throw StateError('readGrouped: $batch');
      }
      final rr = await pgmq.readGroupedRr(queueRr, qty: 2);
      if (rr.length != 2) throw StateError('readGroupedRr: $rr');
      final heads = await pgmq.readGroupedHead(queueHeads, qty: 10);
      final groups = heads.map((m) => m.headers?['x-pgmq-group']).toSet();
      if (!groups.containsAll({'a', 'b'})) {
        throw StateError('heads: $groups');
      }
      await pgmq.createFifoIndexesAll();
      return 'grouped=2 rr=2 heads=${heads.length}';
    });

    await step('topics', () async {
      final queue = track('topic');
      await pgmq.createQueue(queue);
      await pgmq.bindTopic('orders.#', queue);
      final bindings = await pgmq.listTopicBindings(queue: queue);
      if (!bindings.any((b) => b.pattern == 'orders.#')) {
        throw StateError('binding missing: $bindings');
      }
      final routes = await pgmq.testRouting('orders.created');
      if (!routes.any((r) => r.queueName == queue)) {
        throw StateError('routing missing: $routes');
      }
      if (!await pgmq.validateRoutingKey('orders.created')) {
        throw StateError('valid routing key rejected');
      }
      if (await pgmq.validateRoutingKey('***')) {
        throw StateError('invalid routing key accepted');
      }
      if (!await pgmq.validateTopicPattern('orders.*')) {
        throw StateError('valid pattern rejected');
      }
      final fanout = await pgmq.sendTopic('orders.created', {'id': 1});
      if (fanout < 1) throw StateError('fanout=$fanout');
      final batchOut = await pgmq.sendBatchTopic('orders.created', [
        {'id': 2},
        {'id': 3},
      ]);
      if (batchOut.isEmpty) throw StateError('sendBatchTopic empty');
      final rows = await pgmq.read(queue, qty: 10);
      if (rows.length < 3) throw StateError('routed rows: ${rows.length}');
      if (!await pgmq.unbindTopic('orders.#', queue)) {
        throw StateError('unbind failed');
      }
      if (await pgmq.unbindTopic('orders.#', queue)) {
        throw StateError('double unbind should be false');
      }
      return 'fanout=$fanout batch=${batchOut.length} rows=${rows.length}';
    });

    await step('notifications', () async {
      final queue = track('notify');
      await pgmq.createQueue(queue);
      await pgmq.enableNotify(queue, throttleIntervalMs: 0);
      final throttles = await pgmq.listNotifyThrottles();
      if (!throttles.any((t) => t.queueName == queue)) {
        throw StateError('throttle missing');
      }
      await pgmq.updateNotify(queue, 100);
      await pgmq.disableNotify(queue);
      return 'channel=${Pgmq.notifyChannelName(queue)}';
    });

    await step('typed payloads', () async {
      final queue = track('typed');
      await pgmq.createQueue(queue);
      await pgmq.send(queue, _Order(9), toJson: (o) => {'id': o.id});
      final rows = await pgmq.read<_Order>(
        queue,
        fromJson: (json) => _Order.fromJson((json! as Map).cast()),
      );
      if (rows.single.message.id != 9) {
        throw StateError('typed roundtrip: $rows');
      }
      return 'id=9';
    });

    if (connection != null) {
      await step('transactions', () async {
        final queue = track('tx');
        await pgmq.createQueue(queue);
        try {
          await connection.runTx((tx) async {
            final txPgmq = Pgmq(tx);
            // Transaction-scoped advisory lock: serializes queue-level DDL.
            await txPgmq.acquireQueueLock(queue);
            await txPgmq.createFifoIndex(queue);
            await txPgmq.send(queue, {'n': 1});
            if ((await txPgmq.read(queue)).length != 1) {
              throw StateError('not visible inside tx');
            }
            throw _Rollback();
          });
        } on _Rollback {
          // expected
        }
        if ((await pgmq.read(queue)).isNotEmpty) {
          throw StateError('rollback did not happen');
        }
        return 'rolled back';
      });

      await step('notify listener', () async {
        final queue = track('listen');
        await pgmq.createQueue(queue);
        await pgmq.enableNotify(queue, throttleIntervalMs: 0);
        // Starts the lazy LISTEN when subscribed.
        final firstEvent = pgmq.listenNotifyInsert(queue).first;
        await Future<void>.delayed(const Duration(milliseconds: 250));
        await pgmq.send(queue, {'n': 1});
        await firstEvent.timeout(const Duration(seconds: 10));
        await pgmq.disableNotify(queue);
        return 'notified';
      });
    }

    await step('purge/drop', () async {
      final queue = track('purge');
      await pgmq.createQueue(queue);
      await pgmq.sendBatch(queue, [
        {'n': 1},
        {'n': 2},
      ]);
      final purged = await pgmq.purgeQueue(queue);
      if (purged != 2) throw StateError('purged=$purged');
      if (!await pgmq.dropQueue(queue)) throw StateError('drop failed');
      if (await pgmq.dropQueue(queue)) {
        throw StateError('double drop should be false');
      }
      created.remove(queue);
      return 'purged=2';
    });
  } finally {
    await dropAll(created);
  }

  return steps;
}

class _Order {
  final int id;
  _Order(this.id);
  factory _Order.fromJson(Map<String, dynamic> json) =>
      _Order(json['id'] as int);
}

class _Rollback implements Exception {
  const _Rollback();
}
