/// Thrown for client-side PGMQ usage errors (e.g. conflicting arguments).
///
/// Server-side errors (unknown queue, invalid JSON, ...) propagate as the
/// original `postgres` package exceptions (`ServerException`) and are
/// intentionally not wrapped, so callers can inspect SQLSTATE codes.
class PgmqException implements Exception {
  /// Human-readable description of the error.
  final String message;

  /// Creates a new [PgmqException] with the given [message].
  const PgmqException(this.message);

  @override
  String toString() => 'PgmqException: $message';
}

/// Thrown when a single-message operation finds no visible message.
///
/// Returned as `null` by nullable helpers such as [Pgmq.pop] is the norm;
/// this exception is provided for callers that prefer throwing helpers.
class PgmqEmptyException extends PgmqException {
  /// Creates a new empty-queue exception.
  const PgmqEmptyException(super.message);
}
