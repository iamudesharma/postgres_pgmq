import 'exception.dart';

/// A single PGMQ message with a strongly typed payload.
///
/// The default payload type is [Map<String, dynamic>] (decoded `jsonb`).
/// Use [mapPayload] to convert to a domain model, or pass `fromJson` to
/// the read methods.
class PgmqMessage<T> {
  /// Server-assigned message id (unique per queue).
  final int msgId;

  /// Number of times the message has been read.
  final int readCt;

  /// When the message was enqueued.
  final DateTime enqueuedAt;

  /// When the message was last read, or `null` if never read.
  ///
  /// Absent on older PGMQ servers; `null` in that case.
  final DateTime? lastReadAt;

  /// Timestamp at which the message becomes visible again (the `vt` column
  /// of the PGMQ message record).
  final DateTime visibleAt;

  /// Decoded message payload (`jsonb`).
  final T message;

  /// Optional message headers (`jsonb`, may be `null`).
  ///
  /// FIFO grouping uses `headers['x-pgmq-group']`.
  final Map<String, dynamic>? headers;

  /// Creates a new message.
  const PgmqMessage({
    required this.msgId,
    required this.readCt,
    required this.enqueuedAt,
    required this.lastReadAt,
    required this.visibleAt,
    required this.message,
    required this.headers,
  });

  /// Converts the payload with [convert], preserving metadata.
  PgmqMessage<U> mapPayload<U>(U Function(T payload) convert) {
    return PgmqMessage<U>(
      msgId: msgId,
      readCt: readCt,
      enqueuedAt: enqueuedAt,
      lastReadAt: lastReadAt,
      visibleAt: visibleAt,
      message: convert(message),
      headers: headers,
    );
  }

  /// Builds a message from a row map (column name -> value).
  ///
  /// Uses name-based lookup so column order and the presence of `headers` /
  /// `last_read_at` (which vary across server versions) do not matter.
  /// [decode] converts the raw decoded `jsonb` value to [T]; when omitted,
  /// the raw value is cast to [T] (works for `Map`, `List` and primitives).
  factory PgmqMessage.fromColumnMap(
    Map<String, dynamic> row, {
    T Function(Object? json)? decode,
  }) {
    Object? rawMessage = row['message'];
    final T payload;
    if (decode != null) {
      payload = decode(rawMessage);
    } else {
      payload = _castPayload<T>(rawMessage);
    }
    return PgmqMessage<T>(
      msgId: _asInt(row['msg_id'], 'msg_id'),
      readCt: _asInt(row['read_ct'], 'read_ct'),
      enqueuedAt: _asDateTime(row['enqueued_at'], 'enqueued_at'),
      lastReadAt: _asDateTimeOrNull(row['last_read_at']),
      visibleAt: _asDateTime(row['vt'], 'vt'),
      message: payload,
      headers: _asStringMapOrNull(row['headers']),
    );
  }

  @override
  String toString() =>
      'PgmqMessage(msgId: $msgId, readCt: $readCt, message: $message)';

  @override
  bool operator ==(Object other) {
    return other is PgmqMessage<T> &&
        other.msgId == msgId &&
        other.readCt == readCt &&
        other.enqueuedAt == enqueuedAt &&
        other.lastReadAt == lastReadAt &&
        other.visibleAt == visibleAt &&
        other.message == message;
  }

  @override
  int get hashCode =>
      Object.hash(msgId, readCt, enqueuedAt, lastReadAt, visibleAt, message);
}

/// Metadata for a queue from `pgmq.list_queues()`.
class QueueRecord {
  /// Queue name.
  final String queueName;

  /// Whether the queue is partitioned (requires `pg_partman`).
  final bool isPartitioned;

  /// Whether the queue table is unlogged.
  final bool isUnlogged;

  /// When the queue was created.
  final DateTime createdAt;

  /// Creates queue metadata.
  const QueueRecord({
    required this.queueName,
    required this.isPartitioned,
    required this.isUnlogged,
    required this.createdAt,
  });

  /// Builds from a row map.
  factory QueueRecord.fromColumnMap(Map<String, dynamic> row) {
    return QueueRecord(
      queueName: row['queue_name'] as String,
      isPartitioned: _asBool(row['is_partitioned']),
      isUnlogged: _asBool(row['is_unlogged']),
      createdAt: _asDateTime(row['created_at'], 'created_at'),
    );
  }

  @override
  String toString() => 'QueueRecord($queueName)';

  @override
  bool operator ==(Object other) {
    return other is QueueRecord &&
        other.queueName == queueName &&
        other.isPartitioned == isPartitioned &&
        other.isUnlogged == isUnlogged &&
        other.createdAt == createdAt;
  }

  @override
  int get hashCode =>
      Object.hash(queueName, isPartitioned, isUnlogged, createdAt);
}

/// Queue metrics from `pgmq.metrics()` / `pgmq.metrics_all()`.
class QueueMetrics {
  /// Queue name.
  final String queueName;

  /// Total messages in the queue (including invisible).
  final int queueLength;

  /// Visible messages (`vt <= now()`).
  ///
  /// `null` on older servers that do not report this column.
  final int? queueVisibleLength;

  /// Age of the newest message in seconds (may be `null` when empty).
  final int? newestMsgAgeSec;

  /// Age of the oldest message in seconds (may be `null` when empty).
  final int? oldestMsgAgeSec;

  /// Total messages ever sent (from the queue sequence).
  final int totalMessages;

  /// When the metrics were scraped.
  final DateTime scrapeTime;

  /// Messages sitting in the partitioned queue's default partition.
  ///
  /// Only reported by PGMQ v1.13.0+; `null` on older servers or when the
  /// queue is not partitioned. A non-zero value means `pg_partman`
  /// maintenance is failing for the queue.
  final int? defaultPartitionLength;

  /// Creates metrics.
  const QueueMetrics({
    required this.queueName,
    required this.queueLength,
    required this.queueVisibleLength,
    required this.newestMsgAgeSec,
    required this.oldestMsgAgeSec,
    required this.totalMessages,
    required this.scrapeTime,
    this.defaultPartitionLength,
  });

  /// Builds from a row map; tolerates 6-column (legacy) and 7-column
  /// (pre-v1.13) rows.
  factory QueueMetrics.fromColumnMap(Map<String, dynamic> row) {
    return QueueMetrics(
      queueName: row['queue_name'] as String,
      queueLength: _asInt(row['queue_length'], 'queue_length'),
      queueVisibleLength: _asIntOrNull(row['queue_visible_length']),
      newestMsgAgeSec: _asIntOrNull(row['newest_msg_age_sec']),
      oldestMsgAgeSec: _asIntOrNull(row['oldest_msg_age_sec']),
      totalMessages: _asInt(row['total_messages'], 'total_messages'),
      scrapeTime: _asDateTime(row['scrape_time'], 'scrape_time'),
      defaultPartitionLength: _asIntOrNull(row['default_partition_length']),
    );
  }

  @override
  String toString() =>
      'QueueMetrics($queueName, length: $queueLength, total: $totalMessages)';
}

/// A topic binding from `pgmq.list_topic_bindings()`.
class TopicBinding {
  /// Topic pattern (`*` = one segment, `#` = zero or more segments).
  final String pattern;

  /// Bound queue name.
  final String queueName;

  /// When the binding was created.
  final DateTime boundAt;

  /// Server-compiled regex for the pattern.
  final String compiledRegex;

  /// Creates a binding.
  const TopicBinding({
    required this.pattern,
    required this.queueName,
    required this.boundAt,
    required this.compiledRegex,
  });

  /// Builds from a row map.
  factory TopicBinding.fromColumnMap(Map<String, dynamic> row) {
    return TopicBinding(
      pattern: row['pattern'] as String,
      queueName: row['queue_name'] as String,
      boundAt: _asDateTime(row['bound_at'], 'bound_at'),
      compiledRegex: (row['compiled_regex'] ?? '') as String,
    );
  }
}

/// A dry-run routing match from `pgmq.test_routing()`.
class RoutingResult {
  /// Matching pattern.
  final String pattern;

  /// Queue the routing key would fan out to.
  final String queueName;

  /// Server-compiled regex for the pattern.
  final String compiledRegex;

  /// Creates a routing result.
  const RoutingResult({
    required this.pattern,
    required this.queueName,
    required this.compiledRegex,
  });

  /// Builds from a row map.
  factory RoutingResult.fromColumnMap(Map<String, dynamic> row) {
    return RoutingResult(
      pattern: row['pattern'] as String,
      queueName: row['queue_name'] as String,
      compiledRegex: (row['compiled_regex'] ?? '') as String,
    );
  }
}

/// One row of `pgmq.send_batch_topic()` output.
class BatchTopicResult {
  /// Queue the message was routed to.
  final String queueName;

  /// Assigned message id in that queue.
  final int msgId;

  /// Creates a result.
  const BatchTopicResult({required this.queueName, required this.msgId});

  /// Builds from a row map.
  factory BatchTopicResult.fromColumnMap(Map<String, dynamic> row) {
    return BatchTopicResult(
      queueName: row['queue_name'] as String,
      msgId: _asInt(row['msg_id'], 'msg_id'),
    );
  }
}

/// A `LISTEN/NOTIFY` throttle row from `pgmq.list_notify_insert_throttles()`.
class NotificationThrottle {
  /// Queue name.
  final String queueName;

  /// Minimum milliseconds between notifications (`0` = no throttle).
  final int throttleIntervalMs;

  /// When a notification was last emitted.
  final DateTime lastNotifiedAt;

  /// Creates a throttle row.
  const NotificationThrottle({
    required this.queueName,
    required this.throttleIntervalMs,
    required this.lastNotifiedAt,
  });

  /// Builds from a row map.
  factory NotificationThrottle.fromColumnMap(Map<String, dynamic> row) {
    return NotificationThrottle(
      queueName: row['queue_name'] as String,
      throttleIntervalMs:
          _asInt(row['throttle_interval_ms'], 'throttle_interval_ms'),
      lastNotifiedAt: _asDateTime(row['last_notified_at'], 'last_notified_at'),
    );
  }
}

// ---------------------------------------------------------------------------
// Internal coercion helpers (tolerant of int64-as-int and numeric strings).
// ---------------------------------------------------------------------------

int _asInt(Object? value, String field) {
  final parsed = _asIntOrNull(value);
  if (parsed == null) {
    throw PgmqException('Expected integer for "$field", got: $value');
  }
  return parsed;
}

int? _asIntOrNull(Object? value) {
  if (value == null) return null;
  if (value is int) return value;
  if (value is num) return value.toInt();
  if (value is String) return int.tryParse(value);
  return null;
}

bool _asBool(Object? value) {
  if (value is bool) return value;
  if (value is num) return value != 0;
  if (value is String) return value.toLowerCase() == 'true';
  return false;
}

DateTime _asDateTime(Object? value, String field) {
  final parsed = _asDateTimeOrNull(value);
  if (parsed == null) {
    throw PgmqException('Expected timestamp for "$field", got: $value');
  }
  return parsed;
}

DateTime? _asDateTimeOrNull(Object? value) {
  if (value == null) return null;
  if (value is DateTime) return value;
  if (value is String) return DateTime.tryParse(value);
  return null;
}

Map<String, dynamic>? _asStringMapOrNull(Object? value) {
  if (value == null) return null;
  if (value is Map<String, dynamic>) return Map<String, dynamic>.of(value);
  if (value is Map) {
    return value.map((k, v) => MapEntry('$k', v));
  }
  return null;
}

T _castPayload<T>(Object? raw) {
  if (raw is T) return raw;
  // Common case: driver decodes jsonb objects as Map<String, dynamic>-ish
  // maps whose static type may be Map<dynamic, dynamic>.
  if (raw is Map) {
    final copy = raw.map((k, v) => MapEntry('$k', v));
    // ignore: avoid_as
    return copy as T;
  }
  // ignore: avoid_as
  return raw as T;
}
