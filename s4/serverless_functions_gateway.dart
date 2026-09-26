import 'dart:async';
import 'dart:convert';
import 'dart:io';
import 'package:http/http.dart' as http;
import 'package:isolate_manager/isolate_manager.dart';

class TenantTaskRequest {
  final String tenantId;
  final String accessKeyId;
  final String secretAccessKey;
  final String prompt;
  final String taskId;
  final String s4GatewayUrl;
  final bool isDaemon;

  TenantTaskRequest({
    required this.tenantId,
    required this.accessKeyId,
    required this.secretAccessKey,
    required this.prompt,
    required this.taskId,
    required this.s4GatewayUrl,
    required this.isDaemon,
  });

  Map<String, dynamic> toJson() => {
        'tenantId': tenantId,
        'accessKeyId': accessKeyId,
        'secretAccessKey': secretAccessKey,
        'prompt': prompt,
        'taskId': taskId,
        's4GatewayUrl': s4GatewayUrl,
        'isDaemon': isDaemon,
      };

  factory TenantTaskRequest.fromJson(Map<String, dynamic> json) => TenantTaskRequest(
        tenantId: json['tenantId'],
        accessKeyId: json['accessKeyId'],
        secretAccessKey: json['secretAccessKey'],
        prompt: json['prompt'],
        taskId: json['taskId'],
        s4GatewayUrl: json['s4GatewayUrl'],
        isDaemon: json['isDaemon'] ?? false,
      );
}

class TenantTaskResult {
  final String taskId;
  final String s4ObjectPath;
  final String llmResponse;
  final String? serverAddress;
  final bool success;
  final String? error;

  TenantTaskResult({
    required this.taskId,
    required this.s4ObjectPath,
    required this.llmResponse,
    this.serverAddress,
    required this.success,
    this.error,
  });
}

@pragma('vm:entry-point')
Future<Map<String, dynamic>> s4LambdaWorker(dynamic rawParams) async {
  final request = TenantTaskRequest.fromJson(Map<String, dynamic>.from(rawParams));

  try {
    if (request.isDaemon) {
      final server = await HttpServer.bind(InternetAddress.loopbackIPv4, 0);
      final port = server.port;
      final token = 'token_${DateTime.now().millisecondsSinceEpoch}_${request.taskId}';

      server.listen((HttpRequest httpRequest) async {
        final authHeader = httpRequest.headers.value('Authorization');
        if (authHeader != 'Bearer $token') {
          httpRequest.response
            ..statusCode = HttpStatus.unauthorized
            ..write('Unauthorized daemon request')
            ..close();
          return;
        }

        httpRequest.response
          ..statusCode = HttpStatus.ok
          ..write(jsonEncode({'status': 'active', 'taskId': request.taskId}))
          ..close();
      });

      return {
        'taskId': request.taskId,
        's4ObjectPath': '',
        'llmResponse': '',
        'serverAddress': '127.0.0.1:$port:$token',
        'success': true,
      };
    } else {
      final llmOutput = await _stubLlmCall(request.prompt);

      final timestamp = DateTime.now().millisecondsSinceEpoch;
      final objectKey = 'isolates/${request.taskId}-$timestamp';
      final payload = jsonEncode({
        'prompt': request.prompt,
        'response': llmOutput,
        'timestamp': timestamp,
      });

      final uri = Uri.parse('${request.s4GatewayUrl}/buckets/default-functions/keys/$objectKey');
      final response = await http.put(
        uri,
        headers: {
          'X-Tenant-ID': request.tenantId,
          'Authorization': 'S4-HMAC-SHA256 Credential=${request.accessKeyId}',
          'X-S4-Secret': request.secretAccessKey,
          'Content-Type': 'application/json',
        },
        body: payload,
      );

      if (response.statusCode != 200 && response.statusCode != 201) {
        throw Exception('S4 Gateway responded with status: ${response.statusCode}, body: ${response.body}');
      }

      return {
        'taskId': request.taskId,
        's4ObjectPath': objectKey,
        'llmResponse': llmOutput,
        'serverAddress': null,
        'success': true,
      };
    }
  } catch (e) {
    return {
      'taskId': request.taskId,
      's4ObjectPath': '',
      'llmResponse': '',
      'serverAddress': null,
      'success': false,
      'error': e.toString(),
    };
  }
}

Future<String> _stubLlmCall(String prompt) async {
  await Future.delayed(const Duration(milliseconds: 500));
  return "LLM Processed Result for prompt: '${prompt.substring(0, prompt.length > 30 ? 30 : prompt.length)}...'";
}

class S4ServerlessGateway {
  final String s4GatewayUrl;
  final Map<String, Map<String, String>> _activeDaemons = {};
  HttpServer? _proxyServer;

  S4ServerlessGateway({
    this.s4GatewayUrl = 'http://localhost:8080',
  });

  Future<void> initialize({int proxyPort = 8081}) async {
    _proxyServer = await HttpServer.bind(InternetAddress.anyIPv4, proxyPort);
    _proxyServer!.listen(_handleProxyRequest);
  }

  Future<void> _handleProxyRequest(HttpRequest request) async {
    final segments = request.uri.pathSegments;
    if (segments.isEmpty) {
      request.response
        ..statusCode = HttpStatus.badRequest
        ..write('Missing task ID routing segment')
        ..close();
      return;
    }

    final taskId = segments.first;
    final daemonInfo = _activeDaemons[taskId];
    if (daemonInfo == null) {
      request.response
        ..statusCode = HttpStatus.notFound
        ..write('Serverless daemon not found or inactive')
        ..close();
      return;
    }

    final parts = daemonInfo['address']!.split(':');
    final isolatePort = int.parse(parts[1]);
    final token = parts[2];

    try {
      final subPath = '/' + segments.skip(1).join('/');
      final targetUri = Uri.parse('http://127.0.0.1:$isolatePort$subPath');
      
      final httpClientRequest = await http.Client().get(
        targetUri,
        headers: {'Authorization': 'Bearer $token'},
      );

      request.response
        ..statusCode = httpClientRequest.statusCode
        ..write(httpClientRequest.body)
        ..close();
    } catch (e) {
      request.response
        ..statusCode = HttpStatus.internalServerError
        ..write('Proxy error: $e')
        ..close();
    }
  }

  Future<TenantTaskResult> invokeTask({
    required String tenantId,
    required String accessKeyId,
    required String secretAccessKey,
    required String prompt,
    bool isDaemon = false,
  }) async {
    final taskId = 'task_${DateTime.now().microsecondsSinceEpoch}';
    
    final request = TenantTaskRequest(
      tenantId: tenantId,
      accessKeyId: accessKeyId,
      secretAccessKey: secretAccessKey,
      prompt: prompt,
      taskId: taskId,
      s4GatewayUrl: s4GatewayUrl,
      isDaemon: isDaemon,
    );

    final manager = IsolateManager.create(s4LambdaWorker, concurrent: 1, isDebug: false);
    await manager.start();
    final rawResult = await manager.compute(request.toJson());
    if (!isDaemon) {
      await manager.stop();
    }

    final resMap = Map<String, dynamic>.from(rawResult);
    if (isDaemon && resMap['success'] == true && resMap['serverAddress'] != null) {
      _activeDaemons[taskId] = {
        'address': resMap['serverAddress'],
        'managerId': manager.hashCode.toString(),
      };
    }

    return TenantTaskResult(
      taskId: resMap['taskId'],
      s4ObjectPath: resMap['s4ObjectPath'],
      llmResponse: resMap['llmResponse'],
      serverAddress: resMap['serverAddress'] != null ? 'http://localhost:8081/$taskId' : null,
      success: resMap['success'],
      error: resMap['error'],
    );
  }

  Future<void> shutdown() async {
    await _proxyServer?.close(force: true);
  }
}