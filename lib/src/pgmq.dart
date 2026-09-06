import 'package:postgres/postgres.dart';

import 'exception.dart';
import 'models.dart';

/// Default visibility timeout applied when the caller passes `vt: 0` or
/// omits it, mirroring `pgmq-go` (`vtDefault = 30`).
const int defaultVisibilityTimeoutSec = 30;

/// Default long-poll upper bound in seconds (`pgmq.read_with_poll` default).
const int defaultMaxPollSeconds = 5;

/// Default long-poll interval in milliseconds (server default).
const int defaultPollIntervalMs = 100;

/// Idiomatic Dart client for Postgres Message Queue (PGMQ).
///
/// Wraps any `package:postgres` [Session] — a [Connection], [Pool] or
/// [TxSession] — without owning it. The session is never closed by this
/// class; the caller retains full control over connections, pooling and
/// transactions:
///
/// ```dart
/// final pgmq = Pgmq(connection);
/// final pgmq = Pgmq(pool);
/// await pool.runTx((tx) async {
///   final txPgmq = Pgmq(tx);
///   await txPgmq.send('orders', {'id': 1});
/// });
/// ```
///
/// Every method maps to exactly one `pgmq.*` SQL function using parameterized
/// queries (`Sql.named`). Queue names and payloads are always bound
/// parameters, never interpolated into SQL text.
///
/// Payloads are `jsonb`. The default payload type is [Map<String, dynamic>];
/// pass `toJson`/`fromJson` (or use [PgmqMessage.mapPayload]) for domain
/// models. A relative [delay] ([Duration]) maps to the `delay int` (seconds)
/// overloads; an absolute [visibleAt] ([DateTime]) maps to the
/// `delay timestamptz` overloads. The two are mutually exclusive.
class Pgmq {
  /// The underlying postgres session. Never closed by this client.
  final Session session;

  /// Creates a client wrapping [session].
  const Pgmq(this.session);

  // -------------------------------------------------------------------------
  // Extension lifecycle
  // -------------------------------------------------------------------------

  /// Installs the PGMQ extension if it is not already installed.
  ///
  /// Equivalent to `CREATE EXTENSION IF NOT EXISTS pgmq`.
  Future<void> ensureExtension({Duration? timeout}) async {
    await session.execute(
      Sql.named('CREATE EXTENSION IF NOT EXISTS pgmq'),
      timeout: timeout,
    );
  }

  /// Returns `true` when the `pgmq` extension is installed.
  Future<bool> extensionExists({Duration? timeout}) async {
    final result = await session.execute(
      Sql.named(
        "SELECT EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'pgmq')",
      ),
      timeout: timeout,
    );
    return (result.firstOrNull?.firstOrNull as bool?) ?? false;
  }

  // -------------------------------------------------------------------------
  // Queue management
  // -------------------------------------------------------------------------

  /// Creates a queue (standard logged table).
  ///
  /// When [unlogged] is `true`, `pgmq.create_unlogged` is used instead.
  /// When [ifNotExists] is `true` (default), the "already exists" server
  /// error is swallowed.
  Future<void> createQueue(
    String queue, {
    bool unlogged = false,
    bool ifNotExists = true,
    Duration? timeout,
  }) async {
    _requireQueueName(queue);
    final sql = unlogged
        ? Sql.named('SELECT pgmq.create_unlogged(@queue:text)')
        : Sql.named('SELECT pgmq.create(@queue:text)');
    try {
      await session.execute(
        sql,
        parameters: {'queue': queue},
        ignoreRows: true,
        timeout: timeout,
      );
    } on PgException catch (e) {
      if (ifNotExists && e.message.contains('already exists')) return;
      rethrow;
    }
  }

  /// Creates an unlogged queue (faster, not crash-safe; archive stays logged).
  Future<void> createUnloggedQueue(
    String queue, {
    bool ifNotExists = true,
    Duration? timeout,
  }) {
    return createQueue(
      queue,
      unlogged: true,
      ifNotExists: ifNotExists,
      timeout: timeout,
    );
  }

  /// Creates a range-partitioned queue via `pg_partman` (must be installed).
  ///
  /// [partitionInterval] is a row count (`'10000'`) or duration (`'1 day'`);
  /// [retentionInterval] uses the same format.
  Future<void> createPartitionedQueue(
    String queue, {
    String partitionInterval = '10000',
    String retentionInterval = '100000',
    Duration? timeout,
  }) async {
    _requireQueueName(queue);
    await session.execute(
      Sql.named(
        'SELECT pgmq.create_partitioned(@queue:text, @partition_interval:text, @retention_interval:text)',
      ),
      parameters: {
        'queue': queue,
        'partition_interval': partitionInterval,
        'retention_interval': retentionInterval,
      },
      ignoreRows: true,
      timeout: timeout,
    );
  }

  /// Migrates an existing archive table to a partitioned table.
  ///
  /// [table] is the archive table name (e.g. `a_my_queue`).
  Future<void> convertArchivePartitioned(
    String table, {
    String partitionInterval = '10000',
    String retentionInterval = '100000',
    int leadingPartition = 10,
    Duration? timeout,
  }) async {
    await session.execute(
      Sql.named(
        'SELECT pgmq.convert_archive_partitioned(@table:text, @partition_interval:text, @retention_interval:text, @leading_partition:int)',
      ),
      parameters: {
        'table': table,
        'partition_interval': partitionInterval,
        'retention_interval': retentionInterval,
        'leading_partition': leadingPartition,
      },
      ignoreRows: true,
      timeout: timeout,
    );
  }

  /// Drops a queue and its archive. Returns `false` if it did not exist.
  Future<bool> dropQueue(String queue, {Duration? timeout}) async {
    _requireQueueName(queue);
    final result = await session.execute(
      Sql.named('SELECT pgmq.drop_queue(@queue:text)'),
      parameters: {'queue': queue},
      timeout: timeout,
    );
    return (result.firstOrNull?.firstOrNull as bool?) ?? false;
  }

  /// Removes all messages from a queue. Returns the purged message count.
  Future<int> purgeQueue(String queue, {Duration? timeout}) async {
    _requireQueueName(queue);
    final result = await session.execute(
      Sql.named('SELECT pgmq.purge_queue(@queue:text)'),
      parameters: {'queue': queue},
      timeout: timeout,
    );
    return _firstInt(result);
  }

  /// Lists all queues.
  Future<List<QueueRecord>> listQueues({Duration? timeout}) async {
    final result = await session.execute(
      Sql.named('SELECT * FROM pgmq.list_queues()'),
      timeout: timeout,
    );
    return result
        .map((r) => QueueRecord.fromColumnMap(r.toColumnMap()))
        .toList();
  }

  /// Validates a queue name server-side (raises on invalid names).
  Future<void> validateQueueName(String queue, {Duration? timeout}) async {
    await session.execute(
      Sql.named('SELECT pgmq.validate_queue_name(@queue:text)'),
      parameters: {'queue': queue},
      ignoreRows: true,
      timeout: timeout,
    );
  }

  // -------------------------------------------------------------------------
  // Send
  // -------------------------------------------------------------------------

  /// Sends a single message. Returns the assigned message id.
  ///
  /// [headers] sets optional `jsonb` metadata (FIFO grouping uses
  /// `{'x-pgmq-group': '<group>'}`). [delay] hides the message for a relative
  /// duration; [visibleAt] hides it until an absolute timestamp. Pass at most
  /// one of [delay]/[visibleAt]. [toJson] encodes custom payload types;
  /// otherwise [message] must be driver-encodable (`Map`, `List`, primitives).
  Future<int> send<T>(
    String queue,
    T message, {
    Map<String, dynamic>? headers,
    Duration? delay,
    DateTime? visibleAt,
    Object? Function(T value)? toJson,
    Duration? timeout,
  }) async {
    _requireQueueName(queue);
    _requireSingleDelay(delay, visibleAt);
    final Object? payload = toJson != null ? toJson(message) : message;

    final Object query;
    final Map<String, Object?> parameters = {'queue': queue, 'msg': payload};
    if (headers == null && delay == null && visibleAt == null) {
      query = Sql.named('SELECT * FROM pgmq.send(@queue:text, @msg:jsonb)');
    } else if (delay == null && visibleAt == null) {
      query = Sql.named(
        'SELECT * FROM pgmq.send(@queue:text, @msg:jsonb, @headers:jsonb)',
      );
      parameters['headers'] = headers;
    } else if (headers == null && delay != null) {
      query = Sql.named(
        'SELECT * FROM pgmq.send(@queue:text, @msg:jsonb, @delay:int)',
      );
      parameters['delay'] = _delaySeconds(delay);
    } else if (headers == null && visibleAt != null) {
      query = Sql.named(
        'SELECT * FROM pgmq.send(@queue:text, @msg:jsonb, @delay:timestamptz)',
      );
      parameters['delay'] = visibleAt;
    } else if (visibleAt != null) {
      query = Sql.named(
        'SELECT * FROM pgmq.send(@queue:text, @msg:jsonb, @headers:jsonb, @delay:timestamptz)',
      );
      parameters['headers'] = headers;
      parameters['delay'] = visibleAt;
    } else {
      query = Sql.named(
        'SELECT * FROM pgmq.send(@queue:text, @msg:jsonb, @headers:jsonb, @delay:int)',
      );
      parameters['headers'] = headers;
      parameters['delay'] = _delaySeconds(delay);
    }
    final result =
        await session.execute(query, parameters: parameters, timeout: timeout);
    return _firstInt(result);
  }

  /// Sends a batch of messages. Returns the assigned message ids in order.
  ///
  /// When [headers] is provided its length must equal `messages.length`
  /// (use `null` entries for messages without headers).
  Future<List<int>> sendBatch<T>(
    String queue,
    List<T> messages, {
    List<Map<String, dynamic>?>? headers,
    Duration? delay,
    DateTime? visibleAt,
    Object? Function(T value)? toJson,
    Duration? timeout,
  }) async {
    _requireQueueName(queue);
    _requireSingleDelay(delay, visibleAt);
    if (messages.isEmpty) return <int>[];
    if (headers != null && headers.length != messages.length) {
      throw PgmqException(
        'headers length (${headers.length}) must match messages length (${messages.length}).',
      );
    }
    final List<Object?> payloads = [
      for (final m in messages) toJson != null ? toJson(m) : m as Object?,
    ];

    final Object query;
    final Map<String, Object?> parameters = {
      'queue': queue,
      'msgs': TypedValue(Type.jsonbArray, payloads),
    };
    final bool hasHeaders = headers != null;
    if (!hasHeaders && delay == null && visibleAt == null) {
      query =
          Sql.named('SELECT * FROM pgmq.send_batch(@queue:text, @msgs:_jsonb)');
    } else if (!hasHeaders && delay != null) {
      query = Sql.named(
        'SELECT * FROM pgmq.send_batch(@queue:text, @msgs:_jsonb, @delay:int)',
      );
      parameters['delay'] = _delaySeconds(delay);
    } else if (!hasHeaders && visibleAt != null) {
      query = Sql.named(
        'SELECT * FROM pgmq.send_batch(@queue:text, @msgs:_jsonb, @delay:timestamptz)',
      );
      parameters['delay'] = visibleAt;
    } else {
      parameters['headers'] =
          TypedValue(Type.jsonbArray, <Object?>[...headers!]);
      if (delay == null && visibleAt == null) {
        query = Sql.named(
          'SELECT * FROM pgmq.send_batch(@queue:text, @msgs:_jsonb, @headers:_jsonb)',
        );
      } else if (delay != null) {
        query = Sql.named(
          'SELECT * FROM pgmq.send_batch(@queue:text, @msgs:_jsonb, @headers:_jsonb, @delay:int)',
        );
        parameters['delay'] = _delaySeconds(delay);
      } else {
        query = Sql.named(
          'SELECT * FROM pgmq.send_batch(@queue:text, @msgs:_jsonb, @headers:_jsonb, @delay:timestamptz)',
        );
        parameters['delay'] = visibleAt;
      }
    }
    final result =
        await session.execute(query, parameters: parameters, timeout: timeout);
    return [for (final row in result) (row[0] as int)];
  }

  // -------------------------------------------------------------------------
  // Read
  // -------------------------------------------------------------------------

  /// Reads up to [qty] visible messages, making them invisible for [vt].
  ///
  /// [vt] is a [Duration] leash (seconds granularity); values `<= 0` fall
  /// back to [defaultVisibilityTimeoutSec]. [conditional] is an experimental
  /// server-side `message @> conditional` JSONB filter. [fromJson] decodes
  /// custom payload types from the raw decoded `jsonb` value.
  Future<List<PgmqMessage<T>>> read<T>(
    String queue, {
    Duration vt = const Duration(seconds: defaultVisibilityTimeoutSec),
    int qty = 1,
    Map<String, dynamic>? conditional,
    T Function(Object? json)? fromJson,
    Duration? timeout,
  }) async {
    _requireQueueName(queue);
    final result = await session.execute(
      Sql.named(
        'SELECT * FROM pgmq.read(@queue:text, @vt:int, @qty:int, @conditional:jsonb)',
      ),
      parameters: {
        'queue': queue,
        'vt': _vtSeconds(vt),
        'qty': qty,
        'conditional': conditional ?? <String, dynamic>{},
      },
      timeout: timeout,
    );
    return _decodeMessages<T>(result, fromJson);
  }

  /// Reads a single message, or `null` when no message is visible.
  Future<PgmqMessage<T>?> readOne<T>(
    String queue, {
    Duration vt = const Duration(seconds: defaultVisibilityTimeoutSec),
    Map<String, dynamic>? conditional,
    T Function(Object? json)? fromJson,
    Duration? timeout,
  }) async {
    final rows = await read<T>(
      queue,
      vt: vt,
      qty: 1,
      conditional: conditional,
      fromJson: fromJson,
      timeout: timeout,
    );
    return rows.firstOrNull;
  }

  /// Long-polls for up to [qty] messages until one appears or
  /// [maxPollSeconds] elapses.
  ///
  /// The wait happens server-side. [timeout] should exceed [maxPollSeconds].
  Future<List<PgmqMessage<T>>> readWithPoll<T>(
    String queue, {
    Duration vt = const Duration(seconds: defaultVisibilityTimeoutSec),
    int qty = 1,
    int maxPollSeconds = defaultMaxPollSeconds,
    int pollIntervalMs = defaultPollIntervalMs,
    Map<String, dynamic>? conditional,
    T Function(Object? json)? fromJson,
    Duration? timeout,
  }) async {
    _requireQueueName(queue);
    final result = await session.execute(
      Sql.named(
        'SELECT * FROM pgmq.read_with_poll(@queue:text, @vt:int, @qty:int, @max_poll_seconds:int, @poll_interval_ms:int, @conditional:jsonb)',
      ),
      parameters: {
        'queue': queue,
        'vt': _vtSeconds(vt),
        'qty': qty,
        'max_poll_seconds': maxPollSeconds,
        'poll_interval_ms': pollIntervalMs,
        'conditional': conditional ?? <String, dynamic>{},
      },
      timeout: timeout,
    );
    return _decodeMessages<T>(result, fromJson);
  }

  /// FIFO read: fills the batch from the earliest eligible group first.
  ///
  /// Groups are keyed by `headers->>'x-pgmq-group'`. For best performance
  /// create a GIN index via [createFifoIndex].
  Future<List<PgmqMessage<T>>> readGrouped<T>(
    String queue, {
    Duration vt = const Duration(seconds: defaultVisibilityTimeoutSec),
    int qty = 1,
    T Function(Object? json)? fromJson,
    Duration? timeout,
  }) {
    return _readGroupedFn<T>(
      'pgmq.read_grouped(@queue:text, @vt:int, @qty:int)',
      queue,
      vt: vt,
      qty: qty,
      fromJson: fromJson,
      timeout: timeout,
    );
  }

  /// Long-polling variant of [readGrouped].
  Future<List<PgmqMessage<T>>> readGroupedWithPoll<T>(
    String queue, {
    Duration vt = const Duration(seconds: defaultVisibilityTimeoutSec),
    int qty = 1,
    int maxPollSeconds = defaultMaxPollSeconds,
    int pollIntervalMs = defaultPollIntervalMs,
    T Function(Object? json)? fromJson,
    Duration? timeout,
  }) {
    return _readGroupedPollFn<T>(
      'pgmq.read_grouped_with_poll(@queue:text, @vt:int, @qty:int, @max_poll_seconds:int, @poll_interval_ms:int)',
      queue,
      vt: vt,
      qty: qty,
      maxPollSeconds: maxPollSeconds,
      pollIntervalMs: pollIntervalMs,
      fromJson: fromJson,
      timeout: timeout,
    );
  }

  /// FIFO round-robin read: interleaves rank-1 of every eligible group, then
  /// rank-2, and so on (fair, anti-starvation).
  Future<List<PgmqMessage<T>>> readGroupedRr<T>(
    String queue, {
    Duration vt = const Duration(seconds: defaultVisibilityTimeoutSec),
    int qty = 1,
    T Function(Object? json)? fromJson,
    Duration? timeout,
  }) {
    return _readGroupedFn<T>(
      'pgmq.read_grouped_rr(@queue:text, @vt:int, @qty:int)',
      queue,
      vt: vt,
      qty: qty,
      fromJson: fromJson,
      timeout: timeout,
    );
  }

  /// Long-polling variant of [readGroupedRr].
  Future<List<PgmqMessage<T>>> readGroupedRrWithPoll<T>(
    String queue, {
    Duration vt = const Duration(seconds: defaultVisibilityTimeoutSec),
    int qty = 1,
    int maxPollSeconds = defaultMaxPollSeconds,
    int pollIntervalMs = defaultPollIntervalMs,
    T Function(Object? json)? fromJson,
    Duration? timeout,
  }) {
    return _readGroupedPollFn<T>(
      'pgmq.read_grouped_rr_with_poll(@queue:text, @vt:int, @qty:int, @max_poll_seconds:int, @poll_interval_ms:int)',
      queue,
      vt: vt,
      qty: qty,
      maxPollSeconds: maxPollSeconds,
      pollIntervalMs: pollIntervalMs,
      fromJson: fromJson,
      timeout: timeout,
    );
  }

  /// Reads the head (oldest visible) message of up to [qty] groups — one per
  /// group — for horizontal per-group parallelism.
  Future<List<PgmqMessage<T>>> readGroupedHead<T>(
    String queue, {
    Duration vt = const Duration(seconds: defaultVisibilityTimeoutSec),
    int qty = 1,
    T Function(Object? json)? fromJson,
    Duration? timeout,
  }) {
    return _readGroupedFn<T>(
      'pgmq.read_grouped_head(@queue:text, @vt:int, @qty:int)',
      queue,
      vt: vt,
      qty: qty,
      fromJson: fromJson,
      timeout: timeout,
    );
  }

  /// Long-polling variant of [readGroupedHead].
  Future<List<PgmqMessage<T>>> readGroupedHeadWithPoll<T>(
    String queue, {
    Duration vt = const Duration(seconds: defaultVisibilityTimeoutSec),
    int qty = 1,
    int maxPollSeconds = defaultMaxPollSeconds,
    int pollIntervalMs = defaultPollIntervalMs,
    T Function(Object? json)? fromJson,
    Duration? timeout,
  }) {
    return _readGroupedPollFn<T>(
      'pgmq.read_grouped_head_with_poll(@queue:text, @vt:int, @qty:int, @max_poll_seconds:int, @poll_interval_ms:int)',
      queue,
      vt: vt,
      qty: qty,
      maxPollSeconds: maxPollSeconds,
      pollIntervalMs: pollIntervalMs,
      fromJson: fromJson,
      timeout: timeout,
    );
  }

  // -------------------------------------------------------------------------
  // Pop (read + delete)
  // -------------------------------------------------------------------------

  /// Reads and deletes a single message (at-most-once).
  ///
  /// Returns `null` when no message is visible.
  Future<PgmqMessage<T>?> pop<T>(
    String queue, {
    T Function(Object? json)? fromJson,
    Duration? timeout,
  }) async {
    final rows = await popMany<T>(
      queue,
      1,
      fromJson: fromJson,
      timeout: timeout,
    );
    return rows.firstOrNull;
  }

  /// Reads and deletes up to [qty] messages.
  Future<List<PgmqMessage<T>>> popMany<T>(
    String queue,
    int qty, {
    T Function(Object? json)? fromJson,
    Duration? timeout,
  }) async {
    _requireQueueName(queue);
    final result = await session.execute(
      Sql.named('SELECT * FROM pgmq.pop(@queue:text, @qty:int)'),
      parameters: {'queue': queue, 'qty': qty},
      timeout: timeout,
    );
    return _decodeMessages<T>(result, fromJson);
  }

  // -------------------------------------------------------------------------
  // Delete / archive
  // -------------------------------------------------------------------------

  /// Deletes a single message. Returns `true` when it existed.
  Future<bool> delete(String queue, int msgId, {Duration? timeout}) async {
    _requireQueueName(queue);
    final result = await session.execute(
      Sql.named('SELECT pgmq.delete(@queue:text, @msg_id:int8)'),
      parameters: {'queue': queue, 'msg_id': msgId},
      timeout: timeout,
    );
    return (result.firstOrNull?.firstOrNull as bool?) ?? false;
  }

  /// Deletes a batch of messages. Returns the ids that existed.
  Future<List<int>> deleteBatch(
    String queue,
    List<int> msgIds, {
    Duration? timeout,
  }) async {
    _requireQueueName(queue);
    if (msgIds.isEmpty) return <int>[];
    final result = await session.execute(
      Sql.named('SELECT * FROM pgmq.delete(@queue:text, @msg_ids:_int8)'),
      parameters: {
        'queue': queue,
        'msg_ids': TypedValue(Type.bigIntegerArray, msgIds),
      },
      timeout: timeout,
    );
    return [for (final row in result) (row[0] as int)];
  }

  /// Moves a single message to the archive. Returns `true` when it existed.
  Future<bool> archive(String queue, int msgId, {Duration? timeout}) async {
    _requireQueueName(queue);
    final result = await session.execute(
      Sql.named('SELECT pgmq.archive(@queue:text, @msg_id:int8)'),
      parameters: {'queue': queue, 'msg_id': msgId},
      timeout: timeout,
    );
    return (result.firstOrNull?.firstOrNull as bool?) ?? false;
  }

  /// Moves a batch of messages to the archive. Returns archived ids.
  Future<List<int>> archiveBatch(
    String queue,
    List<int> msgIds, {
    Duration? timeout,
  }) async {
    _requireQueueName(queue);
    if (msgIds.isEmpty) return <int>[];
    final result = await session.execute(
      Sql.named('SELECT * FROM pgmq.archive(@queue:text, @msg_ids:_int8)'),
      parameters: {
        'queue': queue,
        'msg_ids': TypedValue(Type.bigIntegerArray, msgIds),
      },
      timeout: timeout,
    );
    return [for (final row in result) (row[0] as int)];
  }

  // -------------------------------------------------------------------------
  // Visibility timeout
  // -------------------------------------------------------------------------

  /// Extends (or shortens) a message's visibility leash.
  ///
  /// Pass [delay] for a relative lease or [visibleAt] for an absolute one
  /// (mutually exclusive; defaults to 30s). Returns the updated message, or
  /// `null` when the id does not exist.
  Future<PgmqMessage<T>?> setVt<T>(
    String queue,
    int msgId, {
    Duration? delay,
    DateTime? visibleAt,
    T Function(Object? json)? fromJson,
    Duration? timeout,
  }) async {
    final rows = await _setVt<T>(
      queue,
      TypedValue(Type.bigInteger, msgId),
      isArray: false,
      delay: delay,
      visibleAt: visibleAt,
      fromJson: fromJson,
      timeout: timeout,
    );
    return rows.firstOrNull;
  }

  /// Batch variant of [setVt]. Returns the updated messages.
  Future<List<PgmqMessage<T>>> setVtBatch<T>(
    String queue,
    List<int> msgIds, {
    Duration? delay,
    DateTime? visibleAt,
    T Function(Object? json)? fromJson,
    Duration? timeout,
  }) async {
    if (msgIds.isEmpty) return <PgmqMessage<T>>[];
    return _setVt<T>(
      queue,
      TypedValue(Type.bigIntegerArray, msgIds),
      isArray: true,
      delay: delay,
      visibleAt: visibleAt,
      fromJson: fromJson,
      timeout: timeout,
    );
  }

  // -------------------------------------------------------------------------
  // Metrics
  // -------------------------------------------------------------------------

  /// Returns metrics for a single queue.
  Future<QueueMetrics> metrics(String queue, {Duration? timeout}) async {
    _requireQueueName(queue);
    final result = await session.execute(
      Sql.named('SELECT * FROM pgmq.metrics(@queue:text)'),
      parameters: {'queue': queue},
      timeout: timeout,
    );
    final row = result.firstOrNull;
    if (row == null) {
      throw PgmqException('No metrics returned for queue "$queue".');
    }
    return QueueMetrics.fromColumnMap(row.toColumnMap());
  }

  /// Returns metrics for all queues.
  Future<List<QueueMetrics>> metricsAll({Duration? timeout}) async {
    final result = await session.execute(
      Sql.named('SELECT * FROM pgmq.metrics_all()'),
      timeout: timeout,
    );
    return result
        .map((r) => QueueMetrics.fromColumnMap(r.toColumnMap()))
        .toList();
  }

  // -------------------------------------------------------------------------
  // FIFO helpers
  // -------------------------------------------------------------------------

  /// Creates the `GIN (headers)` index that speeds up grouped (FIFO) reads.
  Future<void> createFifoIndex(String queue, {Duration? timeout}) async {
    _requireQueueName(queue);
    await session.execute(
      Sql.named('SELECT pgmq.create_fifo_index(@queue:text)'),
      parameters: {'queue': queue},
      ignoreRows: true,
      timeout: timeout,
    );
  }

  /// Creates FIFO indexes for all queues.
  Future<void> createFifoIndexesAll({Duration? timeout}) async {
    await session.execute(
      Sql.named('SELECT pgmq.create_fifo_indexes_all()'),
      ignoreRows: true,
      timeout: timeout,
    );
  }

  /// Deprecated server-side no-op kept for compatibility.
  @Deprecated('detach_archive is a no-op on the server and will be removed.')
  Future<void> detachArchive(String queue, {Duration? timeout}) async {
    await session.execute(
      Sql.named('SELECT pgmq.detach_archive(@queue:text)'),
      parameters: {'queue': queue},
      ignoreRows: true,
      timeout: timeout,
    );
  }

  // -------------------------------------------------------------------------
  // Topics / routing
  // -------------------------------------------------------------------------

  /// Binds a topic [pattern] to [queue] (idempotent).
  ///
  /// `*` matches exactly one dot-segment, `#` matches zero or more segments
  /// (e.g. `orders.*`, `orders.#`).
  Future<void> bindTopic(
    String pattern,
    String queue, {
    Duration? timeout,
  }) async {
    _requireQueueName(queue);
    await session.execute(
      Sql.named('SELECT pgmq.bind_topic(@pattern:text, @queue:text)'),
      parameters: {'pattern': pattern, 'queue': queue},
      ignoreRows: true,
      timeout: timeout,
    );
  }

  /// Removes a topic binding. Returns `true` when a binding was removed.
  Future<bool> unbindTopic(
    String pattern,
    String queue, {
    Duration? timeout,
  }) async {
    final result = await session.execute(
      Sql.named('SELECT pgmq.unbind_topic(@pattern:text, @queue:text)'),
      parameters: {'pattern': pattern, 'queue': queue},
      timeout: timeout,
    );
    return (result.firstOrNull?.firstOrNull as bool?) ?? false;
  }

  /// Lists topic bindings, optionally filtered to one [queue].
  Future<List<TopicBinding>> listTopicBindings({
    String? queue,
    Duration? timeout,
  }) async {
    final Result result;
    if (queue == null) {
      result = await session.execute(
        Sql.named('SELECT * FROM pgmq.list_topic_bindings()'),
        timeout: timeout,
      );
    } else {
      result = await session.execute(
        Sql.named('SELECT * FROM pgmq.list_topic_bindings(@queue:text)'),
        parameters: {'queue': queue},
        timeout: timeout,
      );
    }
    return result
        .map((r) => TopicBinding.fromColumnMap(r.toColumnMap()))
        .toList();
  }

  /// Dry-run: returns the bindings a [routingKey] would fan out to.
  Future<List<RoutingResult>> testRouting(
    String routingKey, {
    Duration? timeout,
  }) async {
    final result = await session.execute(
      Sql.named('SELECT * FROM pgmq.test_routing(@routing_key:text)'),
      parameters: {'routing_key': routingKey},
      timeout: timeout,
    );
    return result
        .map((r) => RoutingResult.fromColumnMap(r.toColumnMap()))
        .toList();
  }

  /// Fans out one message to every queue bound to [routingKey].
  ///
  /// Returns the number of queues the message was routed to (`0` = no match).
  /// The server only supports a relative [delay] for single topic sends.
  Future<int> sendTopic<T>(
    String routingKey,
    T message, {
    Map<String, dynamic>? headers,
    Duration? delay,
    Object? Function(T value)? toJson,
    Duration? timeout,
  }) async {
    final Object? payload = toJson != null ? toJson(message) : message;
    final Object query;
    final Map<String, Object?> parameters = {
      'routing_key': routingKey,
      'msg': payload,
    };
    if (headers == null && delay == null) {
      query = Sql.named(
        'SELECT pgmq.send_topic(@routing_key:text, @msg:jsonb)',
      );
    } else if (delay == null) {
      query = Sql.named(
        'SELECT pgmq.send_topic(@routing_key:text, @msg:jsonb, @headers:jsonb, @delay:int)',
      );
      parameters['headers'] = headers;
      parameters['delay'] = 0;
    } else if (headers == null) {
      query = Sql.named(
        'SELECT pgmq.send_topic(@routing_key:text, @msg:jsonb, @delay:int)',
      );
      parameters['delay'] = _delaySeconds(delay);
    } else {
      query = Sql.named(
        'SELECT pgmq.send_topic(@routing_key:text, @msg:jsonb, @headers:jsonb, @delay:int)',
      );
      parameters['headers'] = headers;
      parameters['delay'] = _delaySeconds(delay);
    }
    final result =
        await session.execute(query, parameters: parameters, timeout: timeout);
    return _firstInt(result);
  }

  /// Fans out a batch of messages to every queue bound to [routingKey].
  Future<List<BatchTopicResult>> sendBatchTopic<T>(
    String routingKey,
    List<T> messages, {
    List<Map<String, dynamic>?>? headers,
    Duration? delay,
    DateTime? visibleAt,
    Object? Function(T value)? toJson,
    Duration? timeout,
  }) async {
    _requireSingleDelay(delay, visibleAt);
    if (messages.isEmpty) return <BatchTopicResult>[];
    if (headers != null && headers.length != messages.length) {
      throw PgmqException(
        'headers length (${headers.length}) must match messages length (${messages.length}).',
      );
    }
    final List<Object?> payloads = [
      for (final m in messages) toJson != null ? toJson(m) : m as Object?,
    ];
    final Object query;
    final Map<String, Object?> parameters = {
      'routing_key': routingKey,
      'msgs': TypedValue(Type.jsonbArray, payloads),
    };
    final bool hasHeaders = headers != null;
    if (!hasHeaders && delay == null && visibleAt == null) {
      query = Sql.named(
        'SELECT * FROM pgmq.send_batch_topic(@routing_key:text, @msgs:_jsonb)',
      );
    } else if (!hasHeaders && delay != null) {
      query = Sql.named(
        'SELECT * FROM pgmq.send_batch_topic(@routing_key:text, @msgs:_jsonb, @delay:int)',
      );
      parameters['delay'] = _delaySeconds(delay);
    } else if (!hasHeaders && visibleAt != null) {
      query = Sql.named(
        'SELECT * FROM pgmq.send_batch_topic(@routing_key:text, @msgs:_jsonb, @delay:timestamptz)',
      );
      parameters['delay'] = visibleAt;
    } else {
      parameters['headers'] =
          TypedValue(Type.jsonbArray, <Object?>[...headers!]);
      if (delay == null && visibleAt == null) {
        query = Sql.named(
          'SELECT * FROM pgmq.send_batch_topic(@routing_key:text, @msgs:_jsonb, @headers:_jsonb)',
        );
      } else if (delay != null) {
        query = Sql.named(
          'SELECT * FROM pgmq.send_batch_topic(@routing_key:text, @msgs:_jsonb, @headers:_jsonb, @delay:int)',
        );
        parameters['delay'] = _delaySeconds(delay);
      } else {
        query = Sql.named(
          'SELECT * FROM pgmq.send_batch_topic(@routing_key:text, @msgs:_jsonb, @headers:_jsonb, @delay:timestamptz)',
        );
        parameters['delay'] = visibleAt;
      }
    }
    final result =
        await session.execute(query, parameters: parameters, timeout: timeout);
    return result
        .map((r) => BatchTopicResult.fromColumnMap(r.toColumnMap()))
        .toList();
  }

  /// Validates a routing key client-side via the server function.
  Future<bool> validateRoutingKey(String routingKey, {Duration? timeout}) {
    return _validateFn(
      'pgmq.validate_routing_key(@value:text)',
      routingKey,
      timeout: timeout,
    );
  }

  /// Validates a topic pattern client-side via the server function.
  Future<bool> validateTopicPattern(String pattern, {Duration? timeout}) {
    return _validateFn(
      'pgmq.validate_topic_pattern(@value:text)',
      pattern,
      timeout: timeout,
    );
  }

  // -------------------------------------------------------------------------
  // Insert notifications (LISTEN/NOTIFY)
  // -------------------------------------------------------------------------

  /// Creates the insert trigger for [queue].
  ///
  /// Subscribers `LISTEN "pgmq.q_<queue>.INSERT"` (see [notifyChannelName]).
  /// [throttleIntervalMs] is the minimum gap between notifications (`0` =
  /// notify on every insert).
  Future<void> enableNotify(
    String queue, {
    int throttleIntervalMs = 250,
    Duration? timeout,
  }) async {
    _requireQueueName(queue);
    await session.execute(
      Sql.named(
        'SELECT pgmq.enable_notify_insert(@queue:text, @throttle_ms:int)',
      ),
      parameters: {'queue': queue, 'throttle_ms': throttleIntervalMs},
      ignoreRows: true,
      timeout: timeout,
    );
  }

  /// Drops the insert trigger for [queue].
  Future<void> disableNotify(String queue, {Duration? timeout}) async {
    _requireQueueName(queue);
    await session.execute(
      Sql.named('SELECT pgmq.disable_notify_insert(@queue:text)'),
      parameters: {'queue': queue},
      ignoreRows: true,
      timeout: timeout,
    );
  }

  /// Changes the throttle interval of an enabled notify trigger.
  Future<void> updateNotify(
    String queue,
    int throttleIntervalMs, {
    Duration? timeout,
  }) async {
    _requireQueueName(queue);
    await session.execute(
      Sql.named(
        'SELECT pgmq.update_notify_insert(@queue:text, @throttle_ms:int)',
      ),
      parameters: {'queue': queue, 'throttle_ms': throttleIntervalMs},
      ignoreRows: true,
      timeout: timeout,
    );
  }

  /// Lists all notify-insert throttle configurations.
  Future<List<NotificationThrottle>> listNotifyThrottles({
    Duration? timeout,
  }) async {
    final result = await session.execute(
      Sql.named('SELECT * FROM pgmq.list_notify_insert_throttles()'),
      timeout: timeout,
    );
    return result
        .map((r) => NotificationThrottle.fromColumnMap(r.toColumnMap()))
        .toList();
  }

  /// Returns the `LISTEN` channel for a queue: `pgmq.q_<queue>.INSERT`.
  static String notifyChannelName(String queue) => 'pgmq.q_$queue.INSERT';

  // -------------------------------------------------------------------------
  // Internals
  // -------------------------------------------------------------------------

  Future<List<PgmqMessage<T>>> _readGroupedFn<T>(
    String fn,
    String queue, {
    required Duration vt,
    required int qty,
    required T Function(Object? json)? fromJson,
    required Duration? timeout,
  }) async {
    _requireQueueName(queue);
    final result = await session.execute(
      Sql.named('SELECT * FROM $fn'),
      parameters: {'queue': queue, 'vt': _vtSeconds(vt), 'qty': qty},
      timeout: timeout,
    );
    return _decodeMessages<T>(result, fromJson);
  }

  Future<List<PgmqMessage<T>>> _readGroupedPollFn<T>(
    String fn,
    String queue, {
    required Duration vt,
    required int qty,
    required int maxPollSeconds,
    required int pollIntervalMs,
    required T Function(Object? json)? fromJson,
    required Duration? timeout,
  }) async {
    _requireQueueName(queue);
    final result = await session.execute(
      Sql.named('SELECT * FROM $fn'),
      parameters: {
        'queue': queue,
        'vt': _vtSeconds(vt),
        'qty': qty,
        'max_poll_seconds': maxPollSeconds,
        'poll_interval_ms': pollIntervalMs,
      },
      timeout: timeout,
    );
    return _decodeMessages<T>(result, fromJson);
  }

  Future<List<PgmqMessage<T>>> _setVt<T>(
    String queue,
    TypedValue<Object> msgIds, {
    required bool isArray,
    required Duration? delay,
    required DateTime? visibleAt,
    required T Function(Object? json)? fromJson,
    required Duration? timeout,
  }) async {
    _requireQueueName(queue);
    _requireSingleDelay(delay, visibleAt);
    final Object query;
    final Map<String, Object?> parameters = {
      'queue': queue,
      'msg_ids': msgIds,
    };
    final String idType = isArray ? '_int8' : 'int8';
    if (visibleAt != null) {
      query = Sql.named(
        'SELECT * FROM pgmq.set_vt(@queue:text, @msg_ids:$idType, @vt:timestamptz)',
      );
      parameters['vt'] = visibleAt;
    } else {
      query = Sql.named(
        'SELECT * FROM pgmq.set_vt(@queue:text, @msg_ids:$idType, @vt:int)',
      );
      parameters['vt'] = _delaySeconds(delay ?? const Duration(seconds: 30));
    }
    final result =
        await session.execute(query, parameters: parameters, timeout: timeout);
    return _decodeMessages<T>(result, fromJson);
  }

  Future<bool> _validateFn(String fn, String value, {Duration? timeout}) async {
    try {
      final result = await session.execute(
        Sql.named('SELECT $fn'),
        parameters: {'value': value},
        timeout: timeout,
      );
      return (result.firstOrNull?.firstOrNull as bool?) ?? false;
    } on PgException {
      // Server validators raise on violation; surface as `false`.
      return false;
    }
  }

  List<PgmqMessage<T>> _decodeMessages<T>(
    Result result,
    T Function(Object? json)? fromJson,
  ) {
    return result
        .map(
          (r) =>
              PgmqMessage<T>.fromColumnMap(r.toColumnMap(), decode: fromJson),
        )
        .toList();
  }
}

// ---------------------------------------------------------------------------
// Argument helpers
// ---------------------------------------------------------------------------

void _requireQueueName(String queue) {
  if (queue.isEmpty) {
    throw const PgmqException('Queue name must not be empty.');
  }
  if (queue.length > 47) {
    throw PgmqException(
      'Queue name "$queue" is ${queue.length} characters; '
      'the server limit is 47.',
    );
  }
}

void _requireSingleDelay(Duration? delay, DateTime? visibleAt) {
  if (delay != null && visibleAt != null) {
    throw const PgmqException(
      'Pass either delay or visibleAt, not both.',
    );
  }
  if (delay != null && delay.isNegative) {
    throw const PgmqException('delay must not be negative.');
  }
}

int _delaySeconds(Duration? delay) => delay == null ? 0 : delay.inSeconds;

/// Visibility timeout in whole seconds; `<= 0` falls back to the default.
int _vtSeconds(Duration vt) {
  if (vt.inSeconds <= 0) return defaultVisibilityTimeoutSec;
  return vt.inSeconds;
}

int _firstInt(Result result) {
  final row = result.firstOrNull;
  if (row == null || row.isEmpty) {
    throw const PgmqException('Expected a single-row integer result.');
  }
  final value = row[0];
  if (value is int) return value;
  if (value is num) return value.toInt();
  throw PgmqException('Expected integer result, got: $value');
}
