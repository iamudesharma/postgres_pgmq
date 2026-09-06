import 'dart:io';

import 'package:flutter_test/flutter_test.dart';
import 'package:pgmq_flutter_demo/pgmq_sweep.dart';
import 'package:postgres/postgres.dart';
import 'package:postgres_pgmq/postgres_pgmq.dart';

/// Consumer-side verification: resolves `postgres_pgmq` through this
/// Flutter package's dependencies and runs the full API sweep.
///
/// Needs a live PGMQ database (see `docker-compose.yml` at the repo root):
///
/// ```sh
/// PGMQ_TEST_DSN='postgresql://postgres:postgres@localhost:5432/postgres' \
///   flutter test
/// ```
///
/// Without `PGMQ_TEST_DSN` the suite is skipped.
final String _dsn = Platform.environment['PGMQ_TEST_DSN'] ?? '';

void main() {
  group(
    'flutter consumer sweep',
    () {
      test('full API sweep passes', () async {
        final Connection connection = await openConnectionFromDsn(_dsn);
        try {
          final steps = await runFullSweep(
            Pgmq(connection),
            connection: connection,
          );
          for (final s in steps) {
            // ignore: avoid_print
            print('${s.ok ? 'PASS' : 'FAIL'} ${s.name}: ${s.detail}');
          }
          final failed = steps.where((s) => !s.ok).toList();
          expect(
            failed,
            isEmpty,
            reason: failed.map((s) => '${s.name}: ${s.detail}').join('\n'),
          );
          expect(steps.length, greaterThanOrEqualTo(15));
        } finally {
          await connection.close();
        }
      });
    },
    skip: _dsn.isEmpty ? 'Set PGMQ_TEST_DSN to run the consumer sweep.' : false,
  );
}
