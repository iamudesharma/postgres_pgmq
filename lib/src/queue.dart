import 'models.dart';
import 'pgmq.dart';

/// A queue-scoped handle over a [Pgmq] client with a bound payload type [T].
///
/// A handle removes the repeated queue name from every call and — for typed
/// handles created with [Pgmq.queueOf] — binds payload encoding and decoding
/// once:
///
/// ```dart
/// final jobs = pgmq.queue('jobs');
/// await jobs.create();
/// final id = await jobs.send({'job': 'send-email'});
/// final message = await jobs.readOne();
/// await jobs.delete(message!.msgId);
/// ```
///
/// For domain models, bind the conversion once instead of passing
/// `fromJson`/`toJson` on every call:
///
/// ```dart
/// final users = pgmq.queueOf<User>(
///   'users',
///   fromJson: (json) => User.fromJson((json! as Map).cast()),
///   toJson: (user) => user.toJson(),
/// );
/// await users.send(User(id: 1));
/// final message = await users.readOne(); // PgmqMessage<User>
/// ```
///
/// Handles are cheap, immutable views: they hold no connection state and can
/// be created wherever they are needed (for example in a repository
/// constructor). Use [client] for client-wide operations such as
/// [Pgmq.listQueues] or [Pgmq.metricsAll].
class PgmqQueue<T> {
  /// Creates a handle for [name] backed by [client].
  ///
  /// Prefer [Pgmq.queue] and [Pgmq.queueOf], which validate [name] eagerly
  /// and read better at the call site.
  const PgmqQueue(this.client, this.name, {this.toJson, this.fromJson});

  /// The client that executes the queries.
  final Pgmq client;

  /// The queue name bound to this handle.
  final String name;

  /// Encodes [T] values before they are sent; `null` when [T] is already
  /// driver-encodable (`Map`, `List`, primitives).
  final Object? Function(T value)? toJson;

  /// Decodes raw `jsonb` payloads read from the queue into [T].
  final T Function(Object? json)? fromJson;

  // -------------------------------------------------------------------------
  // Queue lifecycle
  // -------------------------------------------------------------------------

  /// Creates this queue, optionally as an unlogged queue.
  ///
  /// When [ifNotExists] is `true` (default), an "already exists" server error
  /// is ignored, so the call is idempotent.
  Future<void> create({
    bool unlogged = false,
    bool ifNotExists = true,
    Duration? timeout,
  }) {
    return client.createQueue(
      name,
      unlogged: unlogged,
      ifNotExists: ifNotExists,
      timeout: timeout,
    );
  }

  /// Creates this queue as a range-partitioned queue via `pg_partman`.
  ///
  /// See [Pgmq.createPartitionedQueue] for the semantics of
  /// [partitionInterval], [retentionInterval] and [ifNotExists].
  Future<void> createPartitioned({
    String partitionInterval = '10000',
    String retentionInterval = '100000',
    bool ifNotExists = true,
    Duration? timeout,
  }) {
    return client.createPartitionedQueue(
      name,
      partitionInterval: partitionInterval,
      retentionInterval: retentionInterval,
      ifNotExists: ifNotExists,
      timeout: timeout,
    );
  }

  /// Drops this queue and its archive. Returns `false` if it did not exist.
  Future<bool> drop({Duration? timeout}) {
    return client.dropQueue(name, timeout: timeout);
  }

  /// Removes all messages from this queue. Returns the purged count.
  Future<int> purge({Duration? timeout}) {
    return client.purgeQueue(name, timeout: timeout);
  }

  /// Returns metadata for this queue, or `null` when it does not exist.
  Future<QueueRecord?> metadata({Duration? timeout}) {
    return client.queueMetadata(name, timeout: timeout);
  }

  /// Returns `true` when this queue exists.
  Future<bool> exists({Duration? timeout}) {
    return client.queueExists(name, timeout: timeout);
  }

  /// Takes the transaction-scoped advisory lock for this queue.
  ///
  /// Must be called inside a transaction; see [Pgmq.acquireQueueLock].
  Future<void> acquireLock({Duration? timeout}) {
    return client.acquireQueueLock(name, timeout: timeout);
  }

  // -------------------------------------------------------------------------
  // Send
  // -------------------------------------------------------------------------

  /// Sends [message] to this queue. Returns the assigned message id.
  ///
  /// [headers] sets optional `jsonb` metadata (FIFO grouping uses
  /// `{'x-pgmq-group': '<group>'}`); [delay] delays delivery for a relative
  /// duration and [visibleAt] until an absolute timestamp (mutually
  /// exclusive). Values of [T] are encoded with the handle's `toJson`.
  Future<int> send(
    T message, {
    Map<String, dynamic>? headers,
    Duration? delay,
    DateTime? visibleAt,
    Duration? timeout,
  }) {
    return client.send<T>(
      name,
      message,
      headers: headers,
      delay: delay,
      visibleAt: visibleAt,
      toJson: toJson,
      timeout: timeout,
    );
  }

  /// Sends a batch of messages. Returns the assigned message ids in order.
  ///
  /// When [headers] is provided its length must equal `messages.length`
  /// (use `null` entries for messages without headers).
  Future<List<int>> sendBatch(
    List<T> messages, {
    List<Map<String, dynamic>?>? headers,
    Duration? delay,
    DateTime? visibleAt,
    Duration? timeout,
  }) {
    return client.sendBatch<T>(
      name,
      messages,
      headers: headers,
      delay: delay,
      visibleAt: visibleAt,
      toJson: toJson,
      timeout: timeout,
    );
  }

  // -------------------------------------------------------------------------
  // Read
  // -------------------------------------------------------------------------

  /// Reads up to [qty] visible messages, making them invisible for [vt].
  Future<List<PgmqMessage<T>>> read({
    Duration vt = const Duration(seconds: defaultVisibilityTimeoutSec),
    int qty = 1,
    Map<String, dynamic>? conditional,
    Duration? timeout,
  }) {
    return client.read<T>(
      name,
      vt: vt,
      qty: qty,
      conditional: conditional,
      fromJson: fromJson,
      timeout: timeout,
    );
  }

  /// Reads a single message, or `null` when no message is visible.
  Future<PgmqMessage<T>?> readOne({
    Duration vt = const Duration(seconds: defaultVisibilityTimeoutSec),
    Map<String, dynamic>? conditional,
    Duration? timeout,
  }) {
    return client.readOne<T>(
      name,
      vt: vt,
      conditional: conditional,
      fromJson: fromJson,
      timeout: timeout,
    );
  }

  /// Long-polls for up to [qty] messages until one appears or
  /// [maxPollSeconds] elapses.
  Future<List<PgmqMessage<T>>> readWithPoll({
    Duration vt = const Duration(seconds: defaultVisibilityTimeoutSec),
    int qty = 1,
    int maxPollSeconds = defaultMaxPollSeconds,
    int pollIntervalMs = defaultPollIntervalMs,
    Map<String, dynamic>? conditional,
    Duration? timeout,
  }) {
    return client.readWithPoll<T>(
      name,
      vt: vt,
      qty: qty,
      maxPollSeconds: maxPollSeconds,
      pollIntervalMs: pollIntervalMs,
      conditional: conditional,
      fromJson: fromJson,
      timeout: timeout,
    );
  }

  /// Returns a stream that emits messages as they become visible.
  ///
  /// See [Pgmq.watch] for the lifecycle and cancellation semantics. The
  /// stream is single-subscription; call [watch] again for another consumer.
  Stream<PgmqMessage<T>> watch({
    Duration vt = const Duration(seconds: defaultVisibilityTimeoutSec),
    int qty = 1,
    int maxPollSeconds = defaultMaxPollSeconds,
    int pollIntervalMs = defaultPollIntervalMs,
    Map<String, dynamic>? conditional,
    Duration? timeout,
  }) {
    return client.watch<T>(
      name,
      vt: vt,
      qty: qty,
      maxPollSeconds: maxPollSeconds,
      pollIntervalMs: pollIntervalMs,
      conditional: conditional,
      fromJson: fromJson,
      timeout: timeout,
    );
  }

  /// FIFO read: fills the batch from the earliest eligible group first.
  ///
  /// Groups are keyed by `headers->>'x-pgmq-group'`.
  Future<List<PgmqMessage<T>>> readGrouped({
    Duration vt = const Duration(seconds: defaultVisibilityTimeoutSec),
    int qty = 1,
    Duration? timeout,
  }) {
    return client.readGrouped<T>(
      name,
      vt: vt,
      qty: qty,
      fromJson: fromJson,
      timeout: timeout,
    );
  }

  /// Long-polling variant of [readGrouped].
  Future<List<PgmqMessage<T>>> readGroupedWithPoll({
    Duration vt = const Duration(seconds: defaultVisibilityTimeoutSec),
    int qty = 1,
    int maxPollSeconds = defaultMaxPollSeconds,
    int pollIntervalMs = defaultPollIntervalMs,
    Duration? timeout,
  }) {
    return client.readGroupedWithPoll<T>(
      name,
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
  Future<List<PgmqMessage<T>>> readGroupedRr({
    Duration vt = const Duration(seconds: defaultVisibilityTimeoutSec),
    int qty = 1,
    Duration? timeout,
  }) {
    return client.readGroupedRr<T>(
      name,
      vt: vt,
      qty: qty,
      fromJson: fromJson,
      timeout: timeout,
    );
  }

  /// Long-polling variant of [readGroupedRr].
  Future<List<PgmqMessage<T>>> readGroupedRrWithPoll({
    Duration vt = const Duration(seconds: defaultVisibilityTimeoutSec),
    int qty = 1,
    int maxPollSeconds = defaultMaxPollSeconds,
    int pollIntervalMs = defaultPollIntervalMs,
    Duration? timeout,
  }) {
    return client.readGroupedRrWithPoll<T>(
      name,
      vt: vt,
      qty: qty,
      maxPollSeconds: maxPollSeconds,
      pollIntervalMs: pollIntervalMs,
      fromJson: fromJson,
      timeout: timeout,
    );
  }

  /// Reads the head message of up to [qty] groups — one per group.
  Future<List<PgmqMessage<T>>> readGroupedHead({
    Duration vt = const Duration(seconds: defaultVisibilityTimeoutSec),
    int qty = 1,
    Duration? timeout,
  }) {
    return client.readGroupedHead<T>(
      name,
      vt: vt,
      qty: qty,
      fromJson: fromJson,
      timeout: timeout,
    );
  }

  /// Long-polling variant of [readGroupedHead].
  Future<List<PgmqMessage<T>>> readGroupedHeadWithPoll({
    Duration vt = const Duration(seconds: defaultVisibilityTimeoutSec),
    int qty = 1,
    int maxPollSeconds = defaultMaxPollSeconds,
    int pollIntervalMs = defaultPollIntervalMs,
    Duration? timeout,
  }) {
    return client.readGroupedHeadWithPoll<T>(
      name,
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

  /// Reads and deletes a single message (at-most-once), or `null` when none
  /// is visible.
  Future<PgmqMessage<T>?> pop({Duration? timeout}) {
    return client.pop<T>(name, fromJson: fromJson, timeout: timeout);
  }

  /// Reads and deletes up to [qty] messages.
  Future<List<PgmqMessage<T>>> popMany(int qty, {Duration? timeout}) {
    return client.popMany<T>(
      name,
      qty,
      fromJson: fromJson,
      timeout: timeout,
    );
  }

  // -------------------------------------------------------------------------
  // Delete / archive
  // -------------------------------------------------------------------------

  /// Deletes a single message. Returns `true` when it existed.
  Future<bool> delete(int msgId, {Duration? timeout}) {
    return client.delete(name, msgId, timeout: timeout);
  }

  /// Deletes a batch of messages. Returns the ids that existed.
  Future<List<int>> deleteBatch(List<int> msgIds, {Duration? timeout}) {
    return client.deleteBatch(name, msgIds, timeout: timeout);
  }

  /// Moves a single message to the archive. Returns `true` when it existed.
  Future<bool> archive(int msgId, {Duration? timeout}) {
    return client.archive(name, msgId, timeout: timeout);
  }

  /// Moves a batch of messages to the archive. Returns archived ids.
  Future<List<int>> archiveBatch(List<int> msgIds, {Duration? timeout}) {
    return client.archiveBatch(name, msgIds, timeout: timeout);
  }

  // -------------------------------------------------------------------------
  // Visibility timeout / metrics / FIFO
  // -------------------------------------------------------------------------

  /// Extends (or shortens) a message's visibility leash.
  ///
  /// Pass [delay] for a relative lease or [visibleAt] for an absolute one
  /// (mutually exclusive; defaults to 30s). Returns the updated message, or
  /// `null` when the id does not exist.
  Future<PgmqMessage<T>?> setVt(
    int msgId, {
    Duration? delay,
    DateTime? visibleAt,
    Duration? timeout,
  }) {
    return client.setVt<T>(
      name,
      msgId,
      delay: delay,
      visibleAt: visibleAt,
      fromJson: fromJson,
      timeout: timeout,
    );
  }

  /// Batch variant of [setVt]. Returns the updated messages.
  Future<List<PgmqMessage<T>>> setVtBatch(
    List<int> msgIds, {
    Duration? delay,
    DateTime? visibleAt,
    Duration? timeout,
  }) {
    return client.setVtBatch<T>(
      name,
      msgIds,
      delay: delay,
      visibleAt: visibleAt,
      fromJson: fromJson,
      timeout: timeout,
    );
  }

  /// Returns metrics for this queue.
  Future<QueueMetrics> metrics({Duration? timeout}) {
    return client.metrics(name, timeout: timeout);
  }

  /// Creates the `GIN (headers)` index that speeds up grouped reads.
  Future<void> createFifoIndex({Duration? timeout}) {
    return client.createFifoIndex(name, timeout: timeout);
  }

  // -------------------------------------------------------------------------
  // Topics
  // -------------------------------------------------------------------------

  /// Binds a topic [pattern] to this queue (idempotent).
  ///
  /// `*` matches exactly one dot-segment, `#` matches zero or more segments.
  Future<void> bindTopic(String pattern, {Duration? timeout}) {
    return client.bindTopic(pattern, name, timeout: timeout);
  }

  /// Removes a topic binding from this queue. Returns `true` when one was
  /// removed.
  Future<bool> unbindTopic(String pattern, {Duration? timeout}) {
    return client.unbindTopic(pattern, name, timeout: timeout);
  }

  /// Lists the topic bindings of this queue.
  Future<List<TopicBinding>> topicBindings({Duration? timeout}) {
    return client.listTopicBindings(queue: name, timeout: timeout);
  }

  // -------------------------------------------------------------------------
  // Insert notifications
  // -------------------------------------------------------------------------

  /// Creates the insert trigger for this queue.
  ///
  /// [throttleIntervalMs] is the minimum gap between notifications (`0` =
  /// notify on every insert).
  Future<void> enableNotify({
    int throttleIntervalMs = 250,
    Duration? timeout,
  }) {
    return client.enableNotify(
      name,
      throttleIntervalMs: throttleIntervalMs,
      timeout: timeout,
    );
  }

  /// Drops the insert trigger for this queue.
  Future<void> disableNotify({Duration? timeout}) {
    return client.disableNotify(name, timeout: timeout);
  }

  /// Changes the throttle interval of this queue's notify trigger.
  Future<void> updateNotify(int throttleIntervalMs, {Duration? timeout}) {
    return client.updateNotify(
      name,
      throttleIntervalMs,
      timeout: timeout,
    );
  }

  /// Subscribes to insert notifications for this queue (see [enableNotify]).
  ///
  /// Requires the underlying session to be a [Connection]; see
  /// [Pgmq.listenNotifyInsert].
  Stream<String> listenNotifyInsert() {
    return client.listenNotifyInsert(name);
  }

  @override
  String toString() => 'PgmqQueue<$T>($name)';
}
