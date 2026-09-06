import 'package:flutter/material.dart';
import 'package:postgres/postgres.dart';
import 'package:postgres_pgmq/postgres_pgmq.dart';

import 'pgmq_sweep.dart';

/// Flutter demo + verification app for `postgres_pgmq`.
///
/// Connects to a PGMQ database (see [_defaults] — adjust for your setup;
/// the official image has no SSL, hence [SslMode.disable]) and either runs
/// the automated full-API sweep ([runFullSweep]) or individual operations.
void main() => runApp(const PgmqDemoApp());

class PgmqDemoApp extends StatelessWidget {
  const PgmqDemoApp({super.key});

  @override
  Widget build(BuildContext context) {
    return MaterialApp(
      title: 'postgres_pgmq demo',
      theme: ThemeData(colorSchemeSeed: Colors.indigo, useMaterial3: true),
      home: const DemoPage(),
    );
  }
}

class DemoPage extends StatefulWidget {
  const DemoPage({super.key});

  @override
  State<DemoPage> createState() => _DemoPageState();
}

class _DemoPageState extends State<DemoPage> {
  final host = TextEditingController(text: 'localhost');
  final port = TextEditingController(text: '5434');
  final database = TextEditingController(text: 'postgres');
  final username = TextEditingController(text: 'postgres');
  final password = TextEditingController(text: 'postgres');
  final queue = TextEditingController(text: 'demo');

  Connection? _connection;
  Pgmq? _pgmq;
  bool _busy = false;
  final List<String> _log = [];

  @override
  void dispose() {
    for (final c in [host, port, database, username, password, queue]) {
      c.dispose();
    }
    _connection?.close();
    super.dispose();
  }

  void _logLine(String line) {
    setState(() {
      _log.add(
        '${DateTime.now().toIso8601String().substring(11, 19)} $line',
      );
    });
  }

  Future<void> _guard(Future<void> Function() body) async {
    setState(() => _busy = true);
    try {
      await body();
    } catch (e) {
      _logLine('ERROR: $e');
    } finally {
      if (mounted) setState(() => _busy = false);
    }
  }

  Future<void> _connect() => _guard(() async {
        await _connection?.close();
        final connection = await Connection.open(
          Endpoint(
            host: host.text,
            port: int.parse(port.text),
            database: database.text,
            username: username.text,
            password: password.text,
          ),
          settings: const ConnectionSettings(sslMode: SslMode.disable),
        );
        final pgmq = Pgmq(connection);
        await pgmq.ensureExtension();
        _connection = connection;
        _pgmq = pgmq;
        _logLine('connected, extension ready');
      });

  Future<void> _disconnect() => _guard(() async {
        await _connection?.close();
        _connection = null;
        _pgmq = null;
        _logLine('disconnected');
      });

  Pgmq? get _client {
    if (_pgmq == null) _logLine('ERROR: connect first');
    return _pgmq;
  }

  Future<void> _runSweep() => _guard(() async {
        final pgmq = _client;
        if (pgmq == null) return;
        _logLine('--- full sweep start ---');
        final steps = await runFullSweep(pgmq, connection: _connection);
        var failed = 0;
        for (final s in steps) {
          if (!s.ok) failed++;
          _logLine('${s.ok ? 'PASS' : 'FAIL'} ${s.name}: ${s.detail}');
        }
        _logLine(
          '--- sweep done: ${steps.length - failed}/${steps.length} passed ---',
        );
      });

  Future<void> _send() => _guard(() async {
        final pgmq = _client;
        if (pgmq == null) return;
        await pgmq.createQueue(queue.text);
        final id = await pgmq.send(queue.text, {
          'from': 'flutter_demo',
          'at': DateTime.now().toIso8601String(),
        });
        _logLine('sent msg $id to ${queue.text}');
      });

  Future<void> _read() => _guard(() async {
        final pgmq = _client;
        if (pgmq == null) return;
        final rows = await pgmq.read<Map<String, dynamic>>(queue.text, qty: 5);
        for (final m in rows) {
          _logLine('read #${m.msgId} ct=${m.readCt} ${m.message}');
        }
        if (rows.isEmpty) _logLine('queue empty');
      });

  Future<void> _pop() => _guard(() async {
        final pgmq = _client;
        if (pgmq == null) return;
        final msg = await pgmq.pop<Map<String, dynamic>>(queue.text);
        _logLine(msg == null ? 'nothing to pop' : 'popped ${msg.message}');
      });

  Future<void> _metrics() => _guard(() async {
        final pgmq = _client;
        if (pgmq == null) return;
        final m = await pgmq.metrics(queue.text);
        _logLine(
          '${m.queueName}: length=${m.queueLength} '
          'visible=${m.queueVisibleLength} total=${m.totalMessages}',
        );
      });

  @override
  Widget build(BuildContext context) {
    final connected = _connection != null;
    return Scaffold(
      appBar: AppBar(title: const Text('postgres_pgmq demo')),
      body: Padding(
        padding: const EdgeInsets.all(12),
        child: Column(
          children: [
            Wrap(
              spacing: 8,
              runSpacing: 8,
              children: [
                _field(host, 'host', 140),
                _field(port, 'port', 70),
                _field(database, 'db', 110),
                _field(username, 'user', 100),
                _field(password, 'password', 110, obscure: true),
                _field(queue, 'queue', 120),
              ],
            ),
            const SizedBox(height: 8),
            Wrap(
              spacing: 8,
              runSpacing: 8,
              children: [
                ElevatedButton(
                  onPressed: _busy || connected ? null : _connect,
                  child: const Text('Connect'),
                ),
                ElevatedButton(
                  onPressed: _busy || !connected ? null : _disconnect,
                  child: const Text('Disconnect'),
                ),
                ElevatedButton.icon(
                  onPressed: _busy || !connected ? null : _runSweep,
                  icon: const Icon(Icons.play_arrow),
                  label: const Text('Run full sweep'),
                ),
                OutlinedButton(
                  onPressed: _busy || !connected ? null : _send,
                  child: const Text('Send'),
                ),
                OutlinedButton(
                  onPressed: _busy || !connected ? null : _read,
                  child: const Text('Read'),
                ),
                OutlinedButton(
                  onPressed: _busy || !connected ? null : _pop,
                  child: const Text('Pop'),
                ),
                OutlinedButton(
                  onPressed: _busy || !connected ? null : _metrics,
                  child: const Text('Metrics'),
                ),
                OutlinedButton(
                  onPressed: _log.clear,
                  child: const Text('Clear log'),
                ),
              ],
            ),
            const SizedBox(height: 8),
            if (_busy) const LinearProgressIndicator(),
            const SizedBox(height: 8),
            Expanded(
              child: Container(
                width: double.infinity,
                padding: const EdgeInsets.all(8),
                decoration: BoxDecoration(
                  color: Theme.of(context).colorScheme.surfaceContainerLow,
                  borderRadius: BorderRadius.circular(8),
                ),
                child: SelectionArea(
                  child: ListView.builder(
                    itemCount: _log.length,
                    itemBuilder: (context, i) => Text(
                      _log[i],
                      style: const TextStyle(
                        fontFamily: 'monospace',
                        fontSize: 12,
                      ),
                    ),
                  ),
                ),
              ),
            ),
          ],
        ),
      ),
    );
  }

  Widget _field(
    TextEditingController controller,
    String label,
    double width, {
    bool obscure = false,
  }) {
    return SizedBox(
      width: width,
      child: TextField(
        controller: controller,
        obscureText: obscure,
        decoration: InputDecoration(
          labelText: label,
          border: const OutlineInputBorder(),
          isDense: true,
        ),
      ),
    );
  }
}
