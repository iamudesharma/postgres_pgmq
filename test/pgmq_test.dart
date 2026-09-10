import 'dart:async';

import 'package:postgres/postgres.dart';
import 'package:postgres_pgmq/postgres_pgmq.dart';
import 'package:test/test.dart';

/// In-memory fake of [Session] that records the last query and serves canned
/// results. The real SQL is asserted (function name + bound parameters) so
/// unit tests verify the client never interpolates user input into SQL text.
class FakeSession implements Session {
  Object? lastQuery;
  Object? lastParameters;

  int executeCalls = 0;

  /// Handlers tried in order; first match wins.
  final List<FakeHandler> handlers = [];

  static String sqlOf(Object query) {
    try {
      return (query as dynamic).sql as String;
    } catch (_) {
      return '$query';
    }
  }

  String get lastSql => lastQuery == null ? '' : sqlOf(lastQuery!);

  Map<String, Object?> get lastParams =>
      (lastParameters as Map<String, Object?>?) ?? {};

  @override
  Future<Result> execute(
    Object query, {
    Object? parameters,
    bool ignoreRows = false,
    QueryMode? queryMode,
    Duration? timeout,
  }) async {
    executeCalls++;
    lastQuery = query;
    lastParameters = parameters;
    final sql = sqlOf(query);
    for (final handler in handlers) {
      final result = handler(sql);
      if (result != null) return result;
    }
    return emptyResult();
  }

  @override
  Future<void> get closed => Future.value();

  @override
  bool get isOpen => true;

  @override
  Future<Statement> prepare(Object query) => throw UnimplementedError();
}

typedef FakeHandler = Result? Function(String sql);

/// Builds a [Result] from column names + row value lists.
Result tableResult(List<String> columns, List<List<Object?>> rows) {
  final schema = ResultSchema([
    for (final c in columns)
      ResultSchemaColumn(typeOid: 25, type: Type.text, columnName: c),
  ]);
  return Result(
    rows: [
      for (final values in rows) ResultRow(values: values, schema: schema),
    ],
    affectedRows: rows.length,
    schema: schema,
  );
}

Result emptyResult() => tableResult(const ['?'], const []);

Result singleValue(Object? value) => tableResult(const [
      'result'
    ], [
      [value],
    ]);

void main() {
  MessageRow msg({
    int id = 1,
    int readCt = 0,
    Object? payload,
    Map<String, dynamic>? headers,
    bool includeHeaders = true,
    bool includeLastReadAt = true,
  }) {
    return MessageRow(
      id: id,
      readCt: readCt,
      payload: payload ?? {'hello': 'world'},
      headers: headers,
      includeHeaders: includeHeaders,
      includeLastReadAt: includeLastReadAt,
    );
  }

  group('argument validation', () {
    test('empty queue name throws', () async {
      final pgmq = Pgmq(FakeSession());
      expect(() => pgmq.send('', {'a': 1}), throwsA(isA<PgmqException>()));
      expect(() => pgmq.read(''), throwsA(isA<PgmqException>()));
      expect(() => pgmq.dropQueue(''), throwsA(isA<PgmqException>()));
      expect(() => pgmq.acquireQueueLock(''), throwsA(isA<PgmqException>()));
      expect(() => pgmq.queueMetadata(''), throwsA(isA<PgmqException>()));
      expect(() => pgmq.queueExists(''), throwsA(isA<PgmqException>()));
      expect(
        () => pgmq.createPartitionedQueue(''),
        throwsA(isA<PgmqException>()),
      );
    });

    test('listenNotifyInsert requires a Connection', () {
      final pgmq = Pgmq(FakeSession());
      expect(
        () => pgmq.listenNotifyInsert('q'),
        throwsA(isA<PgmqException>()),
      );
    });

    test('queue name longer than 47 chars throws', () async {
      final pgmq = Pgmq(FakeSession());
      final long = 'q' * 48;
      expect(
        () => pgmq.createQueue(long),
        throwsA(isA<PgmqException>()),
      );
    });

    test('delay and visibleAt together throw', () async {
      final pgmq = Pgmq(FakeSession());
      expect(
        () => pgmq.send(
          'q',
          {'a': 1},
          delay: const Duration(seconds: 5),
          visibleAt: DateTime.utc(2030),
        ),
        throwsA(isA<PgmqException>()),
      );
      expect(
        () => pgmq.sendBatch(
          'q',
          [
            {'a': 1},
          ],
          delay: const Duration(seconds: 5),
          visibleAt: DateTime.utc(2030),
        ),
        throwsA(isA<PgmqException>()),
      );
    });

    test('negative delay throws', () async {
      final pgmq = Pgmq(FakeSession());
      expect(
        () => pgmq.send('q', {'a': 1}, delay: const Duration(seconds: -1)),
        throwsA(isA<PgmqException>()),
      );
    });

    test('sendBatch headers length mismatch throws without querying', () async {
      final session = FakeSession();
      final pgmq = Pgmq(session);
      expect(
        () => pgmq.sendBatch(
          'q',
          [
            {'a': 1},
            {'b': 2},
          ],
          headers: [
            {'h': 1},
          ],
        ),
        throwsA(isA<PgmqException>()),
      );
      expect(session.executeCalls, 0);
    });

    test('empty batches short-circuit without querying', () async {
      final session = FakeSession();
      final pgmq = Pgmq(session);
      expect(await pgmq.sendBatch('q', <Map<String, dynamic>>[]), isEmpty);
      expect(await pgmq.deleteBatch('q', []), isEmpty);
      expect(await pgmq.archiveBatch('q', []), isEmpty);
      expect(await pgmq.setVtBatch('q', []), isEmpty);
      expect(
        await pgmq.sendBatchTopic('key', <Map<String, dynamic>>[]),
        isEmpty,
      );
      expect(session.executeCalls, 0);
    });
  });

  group('parameterized SQL (no interpolation)', () {
    test('queue names and payloads are bound parameters', () async {
      final session = FakeSession();
      session.handlers.add((_) => singleValue(7));
      final pgmq = Pgmq(session);
      const evil = "q'; DROP TABLE pgmq.meta; --";
      // 47-char limit still applies; use a shorter evil payload instead.
      const queue = 'my_queue';
      final id = await pgmq.send(queue, {'note': evil});
      expect(id, 7);
      expect(session.lastSql, contains('pgmq.send('));
      expect(session.lastSql, isNot(contains(evil)));
      expect(session.lastSql, contains('@queue'));
      expect(session.lastParams['queue'], queue);
      expect((session.lastParams['msg'] as Map)['note'], evil);
    });

    test('send overload selection', () async {
      final session = FakeSession();
      session.handlers.add((_) => singleValue(1));
      final pgmq = Pgmq(session);

      await pgmq.send('q', {'a': 1});
      expect(session.lastSql, contains('pgmq.send(@queue:text, @msg:jsonb)'));

      await pgmq.send('q', {'a': 1}, headers: {'h': 'v'});
      expect(session.lastSql, contains('@headers:jsonb'));

      await pgmq.send('q', {'a': 1}, delay: const Duration(seconds: 10));
      expect(session.lastSql, contains('@delay:int'));
      expect(session.lastParams['delay'], 10);

      await pgmq.send('q', {'a': 1}, visibleAt: DateTime.utc(2030, 1, 1));
      expect(session.lastSql, contains('@delay:timestamptz'));

      await pgmq.send(
        'q',
        {'a': 1},
        headers: const {'h': 'v'},
        delay: const Duration(seconds: 3),
      );
      expect(session.lastSql, contains('@headers:jsonb, @delay:int'));
    });

    test('sendBatch uses jsonb array bindings', () async {
      final session = FakeSession();
      session.handlers.add(
        (_) => tableResult(const [
          'send_batch'
        ], const [
          [1],
          [2],
        ]),
      );
      final pgmq = Pgmq(session);
      final ids = await pgmq.sendBatch(
        'q',
        [
          {'a': 1},
          {'b': 2},
        ],
        headers: const [
          {'g': 'x'},
          null,
        ],
      );
      expect(ids, [1, 2]);
      expect(session.lastSql, contains('pgmq.send_batch('));
      expect(session.lastSql, contains('@msgs:_jsonb'));
      expect(session.lastSql, contains('@headers:_jsonb'));
      expect(session.lastParams['headers'], isA<TypedValue>());
    });

    test('setVt uses int vs timestamptz overloads', () async {
      final session = FakeSession();
      session.handlers.add((sql) {
        expect(sql, contains('pgmq.set_vt('));
        return emptyResult();
      });
      final pgmq = Pgmq(session);
      await pgmq.setVt('q', 1, delay: const Duration(seconds: 60));
      expect(session.lastSql, contains('@vt:int'));
      expect(session.lastParams['vt'], 60);

      await pgmq.setVt('q', 1, visibleAt: DateTime.utc(2030));
      expect(session.lastSql, contains('@vt:timestamptz'));
    });

    test('grouped reads hit the right functions', () async {
      final session = FakeSession();
      final seen = <String>[];
      session.handlers.add((sql) {
        seen.add(sql);
        return emptyResult();
      });
      final pgmq = Pgmq(session);
      await pgmq.readGrouped('q');
      await pgmq.readGroupedWithPoll('q');
      await pgmq.readGroupedRr('q');
      await pgmq.readGroupedRrWithPoll('q');
      await pgmq.readGroupedHead('q');
      await pgmq.readGroupedHeadWithPoll('q');
      expect(
        seen.map((s) => s.contains('pgmq.read_grouped(')).toString(),
        contains('true'),
      );
      expect(seen.any((s) => s.contains('read_grouped_with_poll(')), isTrue);
      expect(seen.any((s) => s.contains('read_grouped_rr(')), isTrue);
      expect(
        seen.any((s) => s.contains('read_grouped_rr_with_poll(')),
        isTrue,
      );
      expect(seen.any((s) => s.contains('read_grouped_head(')), isTrue);
      expect(
        seen.any((s) => s.contains('read_grouped_head_with_poll(')),
        isTrue,
      );
    });

    test('queue lock and metadata are bound parameters', () async {
      final session = FakeSession();
      session.handlers.add((sql) {
        if (sql.contains('acquire_queue_lock')) return emptyResult();
        return tableResult(
          const [
            'queue_name',
            'is_partitioned',
            'is_unlogged',
            'created_at',
          ],
          const [
            ['jobs', true, false, '2024-01-01T00:00:00Z'],
          ],
        );
      });
      final pgmq = Pgmq(session);

      await pgmq.acquireQueueLock('my_queue');
      expect(
        session.lastSql,
        contains('pgmq.acquire_queue_lock(@queue:text)'),
      );
      expect(session.lastParams['queue'], 'my_queue');

      final record = await pgmq.queueMetadata('my_queue');
      expect(session.lastSql, contains('FROM pgmq.meta'));
      expect(session.lastSql, contains('@queue:text'));
      expect(session.lastParams['queue'], 'my_queue');
      expect(record?.queueName, 'jobs');
      expect(record?.isPartitioned, isTrue);
    });

    test('createPartitionedQueue ifNotExists checks pgmq.meta first', () async {
      final session = FakeSession();
      final seen = <String>[];
      session.handlers.add((sql) {
        seen.add(sql);
        return emptyResult();
      });
      final pgmq = Pgmq(session);
      await pgmq.createPartitionedQueue('q');
      expect(seen.first, contains('FROM pgmq.meta'));
      expect(seen.last, contains('pgmq.create_partitioned('));
      expect(session.lastParams['partition_interval'], '10000');
      expect(session.lastParams['retention_interval'], '100000');
    });

    test('createPartitionedQueue ifNotExists skips existing queues', () async {
      final session = FakeSession();
      session.handlers.add(
        (_) => tableResult(
          const [
            'queue_name',
            'is_partitioned',
            'is_unlogged',
            'created_at',
          ],
          const [
            ['q', true, false, '2024-01-01T00:00:00Z'],
          ],
        ),
      );
      final pgmq = Pgmq(session);
      await pgmq.createPartitionedQueue('q');
      expect(session.executeCalls, 1);
    });

    test('createPartitionedQueue ifNotExists:false calls directly', () async {
      final session = FakeSession();
      final pgmq = Pgmq(session);
      await pgmq.createPartitionedQueue(
        'q',
        ifNotExists: false,
        partitionInterval: '1 day',
        retentionInterval: '30 days',
      );
      expect(session.executeCalls, 1);
      expect(session.lastSql, contains('pgmq.create_partitioned('));
      expect(session.lastParams['partition_interval'], '1 day');
      expect(session.lastParams['retention_interval'], '30 days');
    });
  });

  group('decoding', () {
    test('read decodes full message rows', () async {
      final session = FakeSession();
      session.handlers.add(
        (_) => tableResult(MessageRow.columns, [
          msg(
            id: 42,
            readCt: 2,
            payload: {'order': 7},
            headers: {'x-pgmq-group': 'g1'},
          ).values,
        ]),
      );
      final pgmq = Pgmq(session);
      final rows = await pgmq.read<Map<String, dynamic>>('q');
      expect(rows, hasLength(1));
      final m = rows.first;
      expect(m.msgId, 42);
      expect(m.readCt, 2);
      expect(m.message, {'order': 7});
      expect(m.headers, {'x-pgmq-group': 'g1'});
      expect(m.enqueuedAt, isA<DateTime>());
      expect(m.lastReadAt, isA<DateTime>());
      expect(m.vt, isA<DateTime>());
    });

    test('read tolerates legacy rows without headers/last_read_at', () async {
      final session = FakeSession();
      session.handlers.add(
        (_) => tableResult(MessageRow.legacyColumns, [
          msg(includeHeaders: false, includeLastReadAt: false).valuesLegacy,
        ]),
      );
      final pgmq = Pgmq(session);
      final rows = await pgmq.read<Map<String, dynamic>>('q');
      expect(rows, hasLength(1));
      expect(rows.first.headers, isNull);
      expect(rows.first.lastReadAt, isNull);
    });

    test('empty reads return empty list / null singles', () async {
      final session = FakeSession();
      session.handlers.add((_) => emptyResult());
      final pgmq = Pgmq(session);
      expect(await pgmq.read('q'), isEmpty);
      expect(await pgmq.readOne('q'), isNull);
      expect(await pgmq.pop('q'), isNull);
      expect(await pgmq.popMany('q', 5), isEmpty);
    });

    test('fromJson decodes typed payloads', () async {
      final session = FakeSession();
      session.handlers.add(
        (_) => tableResult(MessageRow.columns, [
          msg(payload: {'to': 'a@example.com'}).values,
        ]),
      );
      final pgmq = Pgmq(session);
      final rows = await pgmq.read<Email>(
        'q',
        fromJson: (json) => Email.fromJson((json as Map).cast()),
      );
      expect(rows.first.message.to, 'a@example.com');
    });

    test('toJson encodes typed sends', () async {
      final session = FakeSession();
      session.handlers.add((_) => singleValue(9));
      final pgmq = Pgmq(session);
      final id = await pgmq.send(
        'q',
        Email('b@example.com'),
        toJson: (e) => {'to': e.to},
      );
      expect(id, 9);
      expect(session.lastParams['msg'], {'to': 'b@example.com'});
    });

    test('mapPayload converts after the fact', () {
      final m = PgmqMessage<Map<String, dynamic>>(
        msgId: 1,
        readCt: 0,
        enqueuedAt: DateTime.utc(2024, 1, 1),
        lastReadAt: null,
        vt: DateTime.utc(2024, 1, 1),
        message: const {'to': 'c@example.com'},
        headers: null,
      );
      final typed = m.mapPayload(Email.fromJson);
      expect(typed.message.to, 'c@example.com');
      expect(typed.msgId, 1);
    });

    test('metrics decodes 8/7/6-column rows (v1.13 to legacy)', () async {
      final session = FakeSession();
      session.handlers.add(
        (_) => tableResult(QueueMetricsRow.columns8, [
          QueueMetricsRow.values8,
        ]),
      );
      final pgmq = Pgmq(session);
      final v13 = await pgmq.metrics('q');
      expect(v13.queueName, 'q');
      expect(v13.queueLength, 5);
      expect(v13.queueVisibleLength, 3);
      expect(v13.totalMessages, 10);
      expect(v13.defaultPartitionLength, 0);

      session.handlers.clear();
      session.handlers.add(
        (_) => tableResult(QueueMetricsRow.columns7, [
          QueueMetricsRow.values7,
        ]),
      );
      final full = await pgmq.metrics('q');
      expect(full.queueName, 'q');
      expect(full.queueLength, 5);
      expect(full.queueVisibleLength, 3);
      expect(full.totalMessages, 10);
      expect(full.defaultPartitionLength, isNull);

      session.handlers.clear();
      session.handlers.add(
        (_) => tableResult(QueueMetricsRow.columns6, [
          QueueMetricsRow.values6,
        ]),
      );
      final legacy = await pgmq.metrics('q');
      expect(legacy.queueVisibleLength, isNull);
      expect(legacy.defaultPartitionLength, isNull);
    });

    test('queueMetadata returns null for missing queues', () async {
      final session = FakeSession();
      session.handlers.add((_) => emptyResult());
      final pgmq = Pgmq(session);
      expect(await pgmq.queueMetadata('missing'), isNull);
      expect(await pgmq.queueExists('missing'), isFalse);
    });

    test('extensionVersion decodes the installed version', () async {
      final session = FakeSession();
      session.handlers.add((_) => singleValue('1.13.0'));
      final pgmq = Pgmq(session);
      expect(await pgmq.extensionVersion(), '1.13.0');
      expect(session.lastSql, contains('pg_extension'));
    });

    test('listQueues decodes queue records', () async {
      final session = FakeSession();
      session.handlers.add(
        (_) => tableResult(const [
          'queue_name',
          'is_partitioned',
          'is_unlogged',
          'created_at',
        ], const [
          ['jobs', false, false, '2024-01-01T00:00:00Z'],
        ]),
      );
      final pgmq = Pgmq(session);
      final queues = await pgmq.listQueues();
      expect(queues.single.queueName, 'jobs');
      expect(queues.single.isPartitioned, isFalse);
    });

    test('single/batch delete+archive map correctly', () async {
      final session = FakeSession();
      session.handlers.add((sql) {
        if (sql.contains('pgmq.delete(') && sql.contains('_int8')) {
          return tableResult(const [
            'delete'
          ], const [
            [3],
            [4],
          ]);
        }
        if (sql.contains('pgmq.archive(') && sql.contains('_int8')) {
          return tableResult(const [
            'archive'
          ], const [
            [5],
          ]);
        }
        return singleValue(true);
      });
      final pgmq = Pgmq(session);
      expect(await pgmq.delete('q', 1), isTrue);
      expect(await pgmq.deleteBatch('q', [3, 4]), [3, 4]);
      expect(await pgmq.archive('q', 2), isTrue);
      expect(await pgmq.archiveBatch('q', [5]), [5]);
    });

    test('drop/purge/sendTopic map scalar results', () async {
      final session = FakeSession();
      session.handlers.add((sql) {
        if (sql.contains('drop_queue')) return singleValue(true);
        if (sql.contains('purge_queue')) return singleValue(12);
        return singleValue(2); // send_topic fan-out count
      });
      final pgmq = Pgmq(session);
      expect(await pgmq.dropQueue('q'), isTrue);
      expect(await pgmq.purgeQueue('q'), 12);
      expect(await pgmq.sendTopic('orders.created', {'id': 1}), 2);
    });

    test('topic listing + routing decode', () async {
      final session = FakeSession();
      session.handlers.add((sql) {
        if (sql.contains('list_topic_bindings')) {
          return tableResult(const [
            'pattern',
            'queue_name',
            'bound_at',
            'compiled_regex',
          ], const [
            [
              'orders.*',
              'orders',
              '2024-01-01T00:00:00Z',
              '^orders\\.[^.]+',
            ],
          ]);
        }
        return tableResult(const [
          'pattern',
          'queue_name',
          'compiled_regex',
        ], const [
          [
            'orders.*',
            'orders',
            '^orders\\.[^.]+',
          ],
        ]);
      });
      final pgmq = Pgmq(session);
      final bindings = await pgmq.listTopicBindings();
      expect(bindings.single.pattern, 'orders.*');
      final routes = await pgmq.testRouting('orders.created');
      expect(routes.single.queueName, 'orders');
    });
  });

  group('error semantics', () {
    test('createQueue swallows already-exists with ifNotExists', () async {
      final session = FakeSession();
      var calls = 0;
      session.handlers.add((_) {
        calls++;
        throw PgException('Queue "q" already exists');
      });
      final pgmq = Pgmq(session);
      await pgmq.createQueue('q'); // does not throw
      expect(calls, 1);
      expect(
        () => pgmq.createQueue('q', ifNotExists: false),
        throwsA(isA<PgException>()),
      );
    });

    test('validators return false instead of throwing', () async {
      final session = FakeSession();
      session.handlers.add((_) => throw PgException('invalid pattern'));
      final pgmq = Pgmq(session);
      expect(await pgmq.validateRoutingKey('***'), isFalse);
      expect(await pgmq.validateTopicPattern('***'), isFalse);
    });

    test('unexpected errors propagate (not swallowed)', () async {
      final session = FakeSession();
      session.handlers.add((_) => throw PgException('connection reset'));
      final pgmq = Pgmq(session);
      expect(() => pgmq.read('q'), throwsA(isA<PgException>()));
    });
  });

  group('PgmqQueue handle', () {
    test('queue() validates the name and binds it on every call', () async {
      final session = FakeSession();
      session.handlers.add((sql) {
        if (sql.contains('pgmq.send(')) return singleValue(42);
        if (sql.contains('pgmq.read(')) {
          return tableResult(MessageRow.columns, [msg(id: 7).values]);
        }
        return emptyResult();
      });
      final pgmq = Pgmq(session);
      final jobs = pgmq.queue('jobs');

      expect(jobs.name, 'jobs');
      expect(() => pgmq.queue(''), throwsA(isA<PgmqException>()));
      expect(() => pgmq.queueOf<int>('', fromJson: (json) => 0),
          throwsA(isA<PgmqException>()));

      expect(await jobs.send({'n': 1}), 42);
      expect(session.lastParams['queue'], 'jobs');
      expect(session.lastParams['msg'], {'n': 1});

      final messages = await jobs.read(qty: 1);
      expect(messages.single.msgId, 7);
      expect(session.lastParams['queue'], 'jobs');
    });

    test('typed handle binds toJson and fromJson once', () async {
      final session = FakeSession();
      session.handlers.add((sql) {
        if (sql.contains('pgmq.send(')) return singleValue(9);
        if (sql.contains('pgmq.read(')) {
          return tableResult(
            MessageRow.columns,
            [
              msg(id: 3, payload: {'value': 7}).values
            ],
          );
        }
        return emptyResult();
      });
      final pgmq = Pgmq(session);
      final counters = pgmq.queueOf<int>(
        'counters',
        fromJson: (json) => ((json! as Map)['value'] as num).toInt(),
        toJson: (value) => {'value': value},
      );

      expect(await counters.send(5), 9);
      expect(session.lastParams['msg'], {'value': 5});
      expect((await counters.readOne())!.message, 7);
    });

    test('watch emits messages from successive long polls', () async {
      final session = FakeSession();
      var reads = 0;
      session.handlers.add((sql) {
        if (sql.contains('read_with_poll')) {
          reads++;
          if (reads > 2) return emptyResult();
          return tableResult(
            MessageRow.columns,
            [
              msg(id: reads, payload: {'n': reads}).values
            ],
          );
        }
        return emptyResult();
      });
      final pgmq = Pgmq(session);
      final messages = await pgmq
          .watch<Map<String, dynamic>>('jobs', qty: 1, maxPollSeconds: 1)
          .take(2)
          .toList();

      expect(messages.map((m) => m.msgId), [1, 2]);
      expect(messages.map((m) => m.message['n']), [1, 2]);
      expect(reads, greaterThanOrEqualTo(2));
    });

    test('watch issues no further reads after cancellation', () async {
      final session = FakeSession();
      var reads = 0;
      final firstRead = Completer<void>();
      session.handlers.add((sql) {
        if (sql.contains('read_with_poll')) {
          reads++;
          if (!firstRead.isCompleted) firstRead.complete();
          return emptyResult();
        }
        return emptyResult();
      });
      final pgmq = Pgmq(session);
      final subscription = pgmq.watch('jobs', maxPollSeconds: 1).listen((_) {});

      await firstRead.future;
      await subscription.cancel().timeout(const Duration(seconds: 5));
      final readsAtCancel = reads;
      await Future<void>.delayed(const Duration(milliseconds: 50));

      expect(reads, readsAtCancel);
      expect(readsAtCancel, greaterThan(0));
    });

    test('handle delegates queue-scoped operations with bound names', () async {
      final session = FakeSession();
      session.handlers.add((sql) {
        if (sql.contains('pgmq.send(')) return singleValue(1);
        if (sql.contains('purge_queue')) return singleValue(3);
        if (sql.contains('drop_queue') ||
            sql.contains('pgmq.delete') ||
            sql.contains('unbind_topic')) {
          return singleValue(true);
        }
        return emptyResult();
      });
      final pgmq = Pgmq(session);
      final jobs = pgmq.queue('jobs');

      expect(await jobs.purge(), 3);
      expect(session.lastParams['queue'], 'jobs');

      expect(await jobs.drop(), isTrue);
      expect(session.lastParams['queue'], 'jobs');

      expect(await jobs.delete(5), isTrue);
      expect(session.lastSql, contains('pgmq.delete('));
      expect(session.lastParams['queue'], 'jobs');

      await jobs.bindTopic('orders.#');
      expect(session.lastSql, contains('bind_topic'));
      expect(session.lastParams['pattern'], 'orders.#');
      expect(session.lastParams['queue'], 'jobs');

      expect(await jobs.unbindTopic('orders.#'), isTrue);
      expect(session.lastParams['queue'], 'jobs');
    });
  });
}

// ---------------------------------------------------------------------------
// Test fixtures
// ---------------------------------------------------------------------------

class Email {
  final String to;
  Email(this.to);
  factory Email.fromJson(Map<String, dynamic> json) =>
      Email(json['to'] as String);
}

class MessageRow {
  static const columns = [
    'msg_id',
    'read_ct',
    'enqueued_at',
    'vt',
    'message',
    'headers',
    'last_read_at',
  ];
  static const legacyColumns = [
    'msg_id',
    'read_ct',
    'enqueued_at',
    'vt',
    'message',
  ];

  final int id;
  final int readCt;
  final Object? payload;
  final Map<String, dynamic>? headers;
  final bool includeHeaders;
  final bool includeLastReadAt;

  MessageRow({
    required this.id,
    required this.readCt,
    required this.payload,
    required this.headers,
    required this.includeHeaders,
    required this.includeLastReadAt,
  });

  List<Object?> get values => [
        id,
        readCt,
        DateTime.utc(2024, 1, 1),
        DateTime.utc(2024, 1, 1, 0, 0, 30),
        payload,
        if (includeHeaders) headers,
        if (includeLastReadAt) DateTime.utc(2024, 1, 1),
      ];

  List<Object?> get valuesLegacy => [
        id,
        readCt,
        DateTime.utc(2024, 1, 1),
        DateTime.utc(2024, 1, 1, 0, 0, 30),
        payload,
      ];
}

class QueueMetricsRow {
  static const columns8 = [
    'queue_name',
    'queue_length',
    'newest_msg_age_sec',
    'oldest_msg_age_sec',
    'total_messages',
    'scrape_time',
    'queue_visible_length',
    'default_partition_length',
  ];
  static const values8 = [
    'q',
    5,
    1,
    60,
    10,
    '2024-01-01T00:00:00Z',
    3,
    0,
  ];
  static const columns7 = [
    'queue_name',
    'queue_length',
    'newest_msg_age_sec',
    'oldest_msg_age_sec',
    'total_messages',
    'scrape_time',
    'queue_visible_length',
  ];
  static const values7 = [
    'q',
    5,
    1,
    60,
    10,
    '2024-01-01T00:00:00Z',
    3,
  ];
  static const columns6 = [
    'queue_name',
    'queue_length',
    'newest_msg_age_sec',
    'oldest_msg_age_sec',
    'total_messages',
    'scrape_time',
  ];
  static const values6 = [
    'q',
    5,
    1,
    60,
    10,
    '2024-01-01T00:00:00Z',
  ];
}
