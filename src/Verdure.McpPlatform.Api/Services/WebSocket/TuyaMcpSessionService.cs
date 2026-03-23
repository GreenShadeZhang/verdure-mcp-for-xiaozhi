using System.Net.Http.Json;
using System.Net.WebSockets;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using ModelContextProtocol.Client;
using ModelContextProtocol.Protocol;
using Verdure.McpPlatform.Application.Services;

namespace Verdure.McpPlatform.Api.Services.WebSocket;

/// <summary>
/// Manages a Tuya IoT Cloud MCP WebSocket session.
/// Authenticates with Tuya's REST API, establishes a signed WebSocket connection,
/// and handles tools/list + tools/call requests forwarded from the Tuya gateway
/// by routing them to the configured local MCP service endpoints.
///
/// Protocol reference: https://github.com/tuya/tuya-mcp-sdk/tree/master/mcp-csharp
/// </summary>
public sealed class TuyaMcpSessionService : ISessionService
{
    private readonly ILogger<TuyaMcpSessionService> _logger;
    private readonly ILoggerFactory _loggerFactory;
    private readonly IMcpClientService _mcpClientService;
    private readonly IServiceScopeFactory _serviceScopeFactory;
    private readonly McpSessionConfiguration _config;
    private readonly ReconnectionSettings _reconnectionSettings;
    private readonly HttpClient _httpClient;
    private readonly CancellationTokenSource _cancellationTokenSource = new();

    // Auth state
    private string _authToken = string.Empty;
    private string _tuyaClientId = string.Empty;

    // Session state
    private ClientWebSocket? _webSocket;
    private bool _isRunning;
    private int _reconnectAttempt;
    private int _currentBackoffMs;

    // Ping timeout (Tuya gateway sends keep-alive pings)
    private DateTime _lastActivityTime = DateTime.UtcNow;
    private readonly TimeSpan _activityTimeout = TimeSpan.FromSeconds(180);
    private readonly object _activityLock = new();

    public string ServerId => _config.ServerId;
    public string ServerName => _config.ServerName;
    public bool IsConnected => _webSocket?.State == WebSocketState.Open;
    public int ConnectedClientsCount => 0;
    public int TotalConfiguredClients => _config.McpServices.Count;
    public int ReconnectAttempts => _reconnectAttempt;
    public DateTime? LastConnectedTime { get; private set; }
    public DateTime? LastDisconnectedTime { get; private set; }

    public DateTime LastPingReceivedTime
    {
        get { lock (_activityLock) { return _lastActivityTime; } }
    }

    public bool IsPingTimeout
    {
        get { lock (_activityLock) { return DateTime.UtcNow - _lastActivityTime > _activityTimeout; } }
    }

    public event Func<Task>? OnConnected;
    public event Func<string, Task>? OnConnectionFailed;
    public event Func<Task>? OnDisconnected;

    public TuyaMcpSessionService(
        McpSessionConfiguration config,
        ReconnectionSettings reconnectionSettings,
        IMcpClientService mcpClientService,
        IServiceScopeFactory serviceScopeFactory,
        ILoggerFactory loggerFactory)
    {
        _config = config ?? throw new ArgumentNullException(nameof(config));
        _reconnectionSettings = reconnectionSettings ?? throw new ArgumentNullException(nameof(reconnectionSettings));
        _mcpClientService = mcpClientService ?? throw new ArgumentNullException(nameof(mcpClientService));
        _serviceScopeFactory = serviceScopeFactory ?? throw new ArgumentNullException(nameof(serviceScopeFactory));
        _loggerFactory = loggerFactory ?? throw new ArgumentNullException(nameof(loggerFactory));
        _logger = loggerFactory.CreateLogger<TuyaMcpSessionService>();
        _httpClient = new HttpClient();
        _currentBackoffMs = reconnectionSettings.InitialBackoffMs;
    }

    public async Task StartAsync(CancellationToken cancellationToken = default)
    {
        if (_isRunning)
        {
            _logger.LogWarning("Tuya session for server {ServerId} is already running", ServerId);
            return;
        }

        // Validate required Tuya credentials early
        if (string.IsNullOrWhiteSpace(_config.TuyaEndpoint))
            throw new InvalidOperationException($"Tuya session for server {ServerId}: TuyaEndpoint is not configured.");
        if (string.IsNullOrWhiteSpace(_config.TuyaAccessId))
            throw new InvalidOperationException($"Tuya session for server {ServerId}: TuyaAccessId is not configured.");
        if (string.IsNullOrWhiteSpace(_config.TuyaAccessSecret))
            throw new InvalidOperationException($"Tuya session for server {ServerId}: TuyaAccessSecret is not configured.");

        _isRunning = true;
        _logger.LogInformation("Starting Tuya session for server {ServerId} ({ServerName})", ServerId, ServerName);

        var linked = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, _cancellationTokenSource.Token);

        try
        {
            await ConnectWithRetryAsync(linked.Token);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Tuya session for server {ServerId} failed to start", ServerId);
            throw;
        }
    }

    public async Task StopAsync()
    {
        _logger.LogInformation("Stopping Tuya session for server {ServerId}", ServerId);
        _isRunning = false;
        _cancellationTokenSource.Cancel();
        await CleanupConnectionAsync();
    }

    // ─── Reconnect loop ────────────────────────────────────────────────────

    private async Task ConnectWithRetryAsync(CancellationToken cancellationToken)
    {
        while (_isRunning && !cancellationToken.IsCancellationRequested)
        {
            try
            {
                if (_reconnectAttempt > 0)
                {
                    if (_reconnectionSettings.MaxAttempts > 0 && _reconnectAttempt >= _reconnectionSettings.MaxAttempts)
                    {
                        _logger.LogWarning(
                            "Tuya server {ServerId} reached max reconnection attempts ({Max})",
                            ServerId, _reconnectionSettings.MaxAttempts);

                        if (OnConnectionFailed != null)
                            await OnConnectionFailed.Invoke($"Max reconnection attempts ({_reconnectionSettings.MaxAttempts}) reached");
                        break;
                    }

                    var jitter = Random.Shared.NextDouble() * 0.1;
                    var waitMs = (int)(_currentBackoffMs * (1 + jitter));
                    _logger.LogInformation(
                        "Tuya server {ServerId}: waiting {Ms}ms before reconnect attempt {Attempt}",
                        ServerId, waitMs, _reconnectAttempt);
                    await Task.Delay(waitMs, cancellationToken);
                }

                await ConnectOnceAsync(cancellationToken);

                if (cancellationToken.IsCancellationRequested || !_isRunning)
                    break;

                _logger.LogInformation("Tuya server {ServerId} connection ended, will retry", ServerId);
                _reconnectAttempt++;
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                _reconnectAttempt++;
                _logger.LogWarning(
                    "Tuya server {ServerId} connection failed (attempt {Attempt}): {Error}",
                    ServerId, _reconnectAttempt, ex.Message);
                _currentBackoffMs = Math.Min(_currentBackoffMs * 2, _reconnectionSettings.MaxBackoffMs);
            }
        }
    }

    // ─── Single connection attempt ─────────────────────────────────────────

    private async Task ConnectOnceAsync(CancellationToken cancellationToken)
    {
        await CleanupConnectionAsync();

        // Step 1: register / authenticate with Tuya
        await RegisterAsync(cancellationToken);

        // Step 2: build WS URL + signed headers
        var (wsUri, headers) = BuildWsConnectParams();

        _webSocket = new ClientWebSocket();
        foreach (var (key, value) in headers)
            _webSocket.Options.SetRequestHeader(key, value);

        _logger.LogInformation("Tuya server {ServerId}: connecting WebSocket to {Uri}", ServerId, wsUri);
        await _webSocket.ConnectAsync(wsUri, cancellationToken);
        _logger.LogInformation("Tuya server {ServerId}: WebSocket connected", ServerId);

        LastConnectedTime = DateTime.UtcNow;
        UpdateActivity();

        if (OnConnected != null) await OnConnected.Invoke();

        // Step 3: receive loop (blocks until disconnect)
        await ReceiveLoopAsync(cancellationToken);

        LastDisconnectedTime = DateTime.UtcNow;
        if (OnDisconnected != null) await OnDisconnected.Invoke();
    }

    // ─── Authentication ────────────────────────────────────────────────────

    private async Task RegisterAsync(CancellationToken ct)
    {
        var endpoint = _config.TuyaEndpoint
            ?? throw new InvalidOperationException("TuyaEndpoint not configured");
        var accessId = _config.TuyaAccessId
            ?? throw new InvalidOperationException("TuyaAccessId not configured");
        var accessSecret = _config.TuyaAccessSecret
            ?? throw new InvalidOperationException("TuyaAccessSecret not configured");

        var ts = UnixTimeMs();
        var nonce = NewNonce();
        var path = "/v1/client/registration";
        var sign = SignRestRequest(accessSecret, accessId, ts, nonce, path);

        var url = BuildRestUrl(endpoint, path);
        _logger.LogInformation("Tuya server {ServerId}: registering at {Url}", ServerId, url);

        using var request = new HttpRequestMessage(HttpMethod.Get, url);
        request.Headers.TryAddWithoutValidation("access_id", accessId);
        request.Headers.TryAddWithoutValidation("t", ts);
        request.Headers.TryAddWithoutValidation("nonce", nonce);
        request.Headers.TryAddWithoutValidation("sign_method", "HMAC-SHA256");
        request.Headers.TryAddWithoutValidation("sign", sign);

        using var response = await _httpClient.SendAsync(request, ct);
        response.EnsureSuccessStatusCode();

        var body = await response.Content.ReadFromJsonAsync<TuyaAuthApiResponse>(cancellationToken: ct)
            ?? throw new InvalidOperationException("Empty response from Tuya auth API");

        if (!body.Success || body.Data is null)
            throw new InvalidOperationException($"Tuya auth failed: success={body.Success}");

        _authToken = body.Data.Token;
        _tuyaClientId = body.Data.ClientId;
        _logger.LogInformation("Tuya server {ServerId}: registered. client_id={ClientId}", ServerId, _tuyaClientId);
    }

    private (Uri WsUri, Dictionary<string, string> Headers) BuildWsConnectParams()
    {
        var endpoint = _config.TuyaEndpoint!;
        var accessId = _config.TuyaAccessId!;
        var ts = UnixTimeMs();
        var nonce = NewNonce();
        var path = "/ws/mcp";
        var queryPart = $"client_id={_tuyaClientId}";
        var wsBase = BuildWsUrl(endpoint, path);
        var fullUrl = $"{wsBase}?client_id={Uri.EscapeDataString(_tuyaClientId)}";
        var sign = SignRestRequest(_authToken, accessId, ts, nonce, path, queryPart);

        var headers = new Dictionary<string, string>
        {
            ["access_id"] = accessId,
            ["t"] = ts,
            ["nonce"] = nonce,
            ["sign_method"] = "HMAC-SHA256",
            ["sign"] = sign
        };

        return (new Uri(fullUrl), headers);
    }

    // ─── Receive loop ──────────────────────────────────────────────────────

    private async Task ReceiveLoopAsync(CancellationToken ct)
    {
        var buffer = new byte[64 * 1024];

        try
        {
            while (!ct.IsCancellationRequested && _webSocket?.State == WebSocketState.Open)
            {
                using var ms = new MemoryStream();
                WebSocketReceiveResult result;
                var closed = false;

                do
                {
                    result = await _webSocket.ReceiveAsync(buffer, ct);
                    if (result.MessageType == WebSocketMessageType.Close)
                    {
                        _logger.LogInformation("Tuya server {ServerId}: close frame received", ServerId);
                        await _webSocket.CloseAsync(WebSocketCloseStatus.NormalClosure, "bye", ct);
                        closed = true;
                        break;
                    }
                    ms.Write(buffer, 0, result.Count);
                }
                while (!result.EndOfMessage);

                if (closed) break;

                var frame = ms.ToArray();
                if (frame.Length > 0)
                {
                    UpdateActivity();
                    _ = Task.Run(() => HandleFrameAsync(frame, ct), ct);
                }
            }
        }
        catch (OperationCanceledException) { /* expected */ }
        catch (WebSocketException ex)
        {
            _logger.LogWarning(ex, "Tuya server {ServerId}: WebSocket error in receive loop", ServerId);
        }

        _logger.LogInformation("Tuya server {ServerId}: receive loop ended", ServerId);
    }

    // ─── Frame handling ────────────────────────────────────────────────────

    private async Task HandleFrameAsync(byte[] frame, CancellationToken ct)
    {
        TuyaRequest? req;
        try
        {
            req = JsonSerializer.Deserialize<TuyaRequest>(Encoding.UTF8.GetString(frame), TuyaJsonOptions);
        }
        catch (JsonException ex)
        {
            _logger.LogError(ex, "Tuya server {ServerId}: failed to deserialise frame", ServerId);
            return;
        }

        if (req is null)
        {
            _logger.LogWarning("Tuya server {ServerId}: received null frame", ServerId);
            return;
        }

        _logger.LogDebug("Tuya server {ServerId}: received method={Method} id={Id}", ServerId, req.Method, req.RequestId);

        // Verify HMAC-SHA256 signature
        if (!VerifyWsFrame(RequestFields(req), _authToken, req.Sign))
        {
            _logger.LogError("Tuya server {ServerId}: signature verification failed for request_id={Id}", ServerId, req.RequestId);
            return;
        }

        switch (req.Method)
        {
            case "tools/list":
                await HandleToolsListAsync(req, ct);
                break;
            case "tools/call":
                await HandleToolsCallAsync(req, ct);
                break;
            case "root/kickout":
                _logger.LogInformation("Tuya server {ServerId}: kickout received; will reconnect", ServerId);
                await CleanupConnectionAsync();
                break;
            case "root/migrate":
                _logger.LogInformation("Tuya server {ServerId}: migrate received; will reconnect", ServerId);
                await CleanupConnectionAsync();
                break;
            default:
                _logger.LogWarning("Tuya server {ServerId}: unknown method {Method}", ServerId, req.Method);
                break;
        }
    }

    private async Task HandleToolsListAsync(TuyaRequest req, CancellationToken ct)
    {
        try
        {
            // Collect tools from all bound MCP services
            var tools = new List<Tool>();
            foreach (var service in _config.McpServices)
            {
                try
                {
                    await using var client = await _mcpClientService.CreateMcpClientAsync(
                        service.ServiceName,
                        service.NodeAddress,
                        service.Protocol,
                        service.AuthenticationType,
                        service.AuthenticationConfig,
                        cancellationToken: ct);

                    var serviceTools = await client.ListToolsAsync(cancellationToken: ct);
                    // Apply tool filter if configured
                    var filtered = service.SelectedTools.Count > 0
                        ? serviceTools.Where(t => service.SelectedTools.Any(s => s.Name == t.ProtocolTool.Name))
                        : serviceTools;
                    tools.AddRange(filtered.Select(t => t.ProtocolTool));
                }
                catch (Exception ex)
                {
                    _logger.LogWarning(ex, "Tuya server {ServerId}: failed to list tools from service {Service}", ServerId, service.ServiceName);
                }
            }

            var result = new ListToolsResult { Tools = tools };
            var resultJson = JsonSerializer.Serialize(result, McpJsonOptions);
            await ReplyAsync(req, resultJson, ct);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Tuya server {ServerId}: tools/list failed", ServerId);
            await ReplyErrorAsync(req, ex.Message, ct);
        }
    }

    private async Task HandleToolsCallAsync(TuyaRequest req, CancellationToken ct)
    {
        string? toolName;
        IReadOnlyDictionary<string, object?>? toolArgs;

        try
        {
            var node = JsonNode.Parse(req.Request);
            toolName = node?["params"]?["name"]?.GetValue<string>();
            var argsNode = node?["params"]?["arguments"];
            toolArgs = argsNode is not null
                ? argsNode.AsObject().ToDictionary(kv => kv.Key, kv => (object?)kv.Value?.DeepClone())
                : null;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Tuya server {ServerId}: failed to parse tools/call body", ServerId);
            await ReplyErrorAsync(req, "invalid request body", ct);
            return;
        }

        if (string.IsNullOrEmpty(toolName))
        {
            await ReplyErrorAsync(req, "missing params.name in tools/call request", ct);
            return;
        }

        // Find which service owns the requested tool
        McpServiceEndpoint? targetService = null;
        foreach (var service in _config.McpServices)
        {
            if (service.SelectedTools.Count > 0)
            {
                if (service.SelectedTools.Any(t => t.Name == toolName))
                {
                    targetService = service;
                    break;
                }
            }
            else
            {
                // No tool filter – try calling this service
                targetService = service;
                break;
            }
        }

        if (targetService == null)
        {
            // Fallback: try each service
            targetService = _config.McpServices.FirstOrDefault();
        }

        if (targetService == null)
        {
            await ReplyErrorAsync(req, $"no service found for tool '{toolName}'", ct);
            return;
        }

        try
        {
            await using var client = await _mcpClientService.CreateMcpClientAsync(
                targetService.ServiceName,
                targetService.NodeAddress,
                targetService.Protocol,
                targetService.AuthenticationType,
                targetService.AuthenticationConfig,
                cancellationToken: ct);

            var callResult = await client.CallToolAsync(toolName, toolArgs, cancellationToken: ct);
            var resultJson = JsonSerializer.Serialize(callResult, McpJsonOptions);
            await ReplyAsync(req, resultJson, ct);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Tuya server {ServerId}: tools/call failed for tool {Tool}", ServerId, toolName);
            await ReplyErrorAsync(req, ex.Message, ct);
        }
    }

    // ─── Response helpers ──────────────────────────────────────────────────

    private async Task ReplyAsync(TuyaRequest req, string responseJson, CancellationToken ct = default)
    {
        var resp = new TuyaResponse
        {
            RequestId = req.RequestId,
            Endpoint = req.Endpoint,
            Version = req.Version,
            Method = req.Method,
            Ts = req.Ts,
            Response = responseJson,
        };

        resp = resp with { Sign = SignWsFrame(ResponseFields(resp), _authToken) };

        var json = JsonSerializer.Serialize(resp, TuyaJsonOptions);
        _logger.LogDebug("Tuya server {ServerId}: sending response for request_id={Id}", ServerId, resp.RequestId);

        if (_webSocket?.State == WebSocketState.Open)
        {
            var bytes = Encoding.UTF8.GetBytes(json);
            using var sendCts = CancellationTokenSource.CreateLinkedTokenSource(ct, _cancellationTokenSource.Token);
            await _webSocket.SendAsync(bytes, WebSocketMessageType.Binary, true, sendCts.Token);
        }
    }

    private async Task ReplyErrorAsync(TuyaRequest req, string message, CancellationToken ct = default)
    {
        var errorResult = new CallToolResult
        {
            Content = [new TextContentBlock { Text = message }],
            IsError = true,
        };
        var errorJson = JsonSerializer.Serialize(errorResult, McpJsonOptions);
        await ReplyAsync(req, errorJson, ct);
    }

    // ─── Cleanup ───────────────────────────────────────────────────────────

    private async Task CleanupConnectionAsync()
    {
        if (_webSocket != null)
        {
            try
            {
                if (_webSocket.State == WebSocketState.Open)
                    await _webSocket.CloseAsync(WebSocketCloseStatus.NormalClosure, "cleanup", CancellationToken.None);
            }
            catch { /* best-effort */ }
            _webSocket.Dispose();
            _webSocket = null;
        }
    }

    public async ValueTask DisposeAsync()
    {
        await CleanupConnectionAsync();
        _cancellationTokenSource.Dispose();
        _httpClient.Dispose();
    }

    // ─── Activity tracking ─────────────────────────────────────────────────

    private void UpdateActivity()
    {
        lock (_activityLock) { _lastActivityTime = DateTime.UtcNow; }
    }

    // ─── HMAC-SHA256 signing helpers (mirrors Tuya Go/Python SDK) ──────────

    private static string HmacSha256Hex(string secret, string input)
    {
        var key = Encoding.UTF8.GetBytes(secret);
        var data = Encoding.UTF8.GetBytes(input);
        var hash = HMACSHA256.HashData(key, data);
        return Convert.ToHexString(hash); // uppercase, no dashes
    }

    private static string SignRestRequest(
        string secret, string accessId, string ts, string nonce, string path,
        string? queryParams = null, string? payload = null)
    {
        var headerPart = $"{accessId}\n{ts}\nHMAC-SHA256\n{nonce}\n";
        var signStr = $"{headerPart}\n{queryParams ?? string.Empty}\n{payload ?? string.Empty}\n{path}";
        return HmacSha256Hex(secret, signStr);
    }

    private static string SignWsFrame(IReadOnlyDictionary<string, string> fields, string token)
    {
        var canonical = fields
            .Where(kv => kv.Key != "sign")
            .OrderBy(kv => kv.Key, StringComparer.Ordinal)
            .Select(kv => $"{kv.Key}:{kv.Value}");
        return HmacSha256Hex(token, string.Join('\n', canonical));
    }

    private static bool VerifyWsFrame(IReadOnlyDictionary<string, string> fields, string token, string sign)
    {
        var computed = SignWsFrame(fields, token);
        return CryptographicOperations.FixedTimeEquals(
            Encoding.ASCII.GetBytes(computed),
            Encoding.ASCII.GetBytes(sign));
    }

    private static string NewNonce()
    {
        Span<byte> buf = stackalloc byte[16];
        RandomNumberGenerator.Fill(buf);
        return Convert.ToHexString(buf).ToLowerInvariant();
    }

    private static string UnixTimeMs() =>
        DateTimeOffset.UtcNow.ToUnixTimeMilliseconds().ToString();

    private static string BuildRestUrl(string endpoint, string path)
    {
        var b = new UriBuilder(endpoint) { Path = path };
        return b.Uri.ToString();
    }

    private static string BuildWsUrl(string endpoint, string path)
    {
        var b = new UriBuilder(endpoint) { Path = path };
        b.Scheme = b.Scheme switch
        {
            "http" => "ws",
            "https" => "wss",
            _ => b.Scheme
        };
        return b.Uri.ToString();
    }

    // ─── Signing field helpers ─────────────────────────────────────────────

    private static IReadOnlyDictionary<string, string> RequestFields(TuyaRequest r) =>
        new Dictionary<string, string>
        {
            ["request_id"] = r.RequestId,
            ["endpoint"] = r.Endpoint,
            ["version"] = r.Version,
            ["method"] = r.Method,
            ["ts"] = r.Ts,
            ["request"] = r.Request,
        };

    private static IReadOnlyDictionary<string, string> ResponseFields(TuyaResponse r) =>
        new Dictionary<string, string>
        {
            ["request_id"] = r.RequestId,
            ["endpoint"] = r.Endpoint,
            ["version"] = r.Version,
            ["method"] = r.Method,
            ["ts"] = r.Ts,
            ["response"] = r.Response,
        };

    // ─── JSON options ──────────────────────────────────────────────────────

    private static readonly JsonSerializerOptions TuyaJsonOptions = new()
    {
        DefaultIgnoreCondition = System.Text.Json.Serialization.JsonIgnoreCondition.WhenWritingNull,
        PropertyNamingPolicy = null, // use JsonPropertyName attributes
    };

    private static readonly JsonSerializerOptions McpJsonOptions =
        ModelContextProtocol.McpJsonUtilities.DefaultOptions;
}

// ─── Tuya wire models ──────────────────────────────────────────────────────────

internal record TuyaMsgBase
{
    [System.Text.Json.Serialization.JsonPropertyName("request_id")] public string RequestId { get; init; } = string.Empty;
    [System.Text.Json.Serialization.JsonPropertyName("endpoint")] public string Endpoint { get; init; } = string.Empty;
    [System.Text.Json.Serialization.JsonPropertyName("version")] public string Version { get; init; } = string.Empty;
    [System.Text.Json.Serialization.JsonPropertyName("method")] public string Method { get; init; } = string.Empty;
    [System.Text.Json.Serialization.JsonPropertyName("ts")] public string Ts { get; init; } = string.Empty;
    [System.Text.Json.Serialization.JsonPropertyName("sign")] public string Sign { get; init; } = string.Empty;
}

internal sealed record TuyaRequest : TuyaMsgBase
{
    [System.Text.Json.Serialization.JsonPropertyName("request")] public string Request { get; init; } = string.Empty;
}

internal sealed record TuyaResponse : TuyaMsgBase
{
    [System.Text.Json.Serialization.JsonPropertyName("response")] public string Response { get; init; } = string.Empty;
}

internal sealed class TuyaAuthApiResponse
{
    [System.Text.Json.Serialization.JsonPropertyName("success")] public bool Success { get; init; }
    [System.Text.Json.Serialization.JsonPropertyName("data")] public TuyaAuthData? Data { get; init; }
}

internal sealed class TuyaAuthData
{
    [System.Text.Json.Serialization.JsonPropertyName("token")] public string Token { get; init; } = string.Empty;
    [System.Text.Json.Serialization.JsonPropertyName("client_id")] public string ClientId { get; init; } = string.Empty;
}
