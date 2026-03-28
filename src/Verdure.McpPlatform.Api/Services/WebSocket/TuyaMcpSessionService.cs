using System.Net.Http.Json;
using System.Net.WebSockets;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
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
            case "sys/error":
                // Tuya gateway pushes sys/error to notify the client of a server-side error
                // (e.g. invalid token, malformed request, rate limiting).
                // The error detail is carried in the request payload.
                _logger.LogError(
                    "Tuya server {ServerId}: received sys/error from gateway, payload={Payload}",
                    ServerId, req.Request);
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
            if (_config.McpServices.Count == 0)
            {
                _logger.LogWarning("Tuya server {ServerId}: no MCP services configured for tools/list request", ServerId);
                await ReplyErrorAsync(req, "No MCP services configured for this endpoint", ct);
                return;
            }

            var tools = new List<object>();

            foreach (var service in _config.McpServices)
            {
                try
                {
                    foreach (var tool in service.SelectedTools)
                    {
                        var (properties, required) = ParseToolSchema(tool);

                        tools.Add(new
                        {
                            name = tool.Name,
                            description = tool.Description,
                            inputSchema = new
                            {
                                properties,
                                required,
                                title = $"{tool.Name}Arguments",
                                type = "object"
                            }
                        });
                    }

                    _logger.LogDebug(
                        "Tuya server {ServerId}: loaded {Count} tools from binding for service {ServiceName}",
                        ServerId,
                        service.SelectedTools.Count,
                        service.ServiceName);
                }
                catch (Exception ex)
                {
                    _logger.LogError(
                        ex,
                        "Tuya server {ServerId}: failed to build tools/list payload for service {ServiceName}",
                        ServerId,
                        service.ServiceName);
                }
            }

            var result = new
            {
                tools = tools.ToArray()
            };

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
        Dictionary<string, object?>? toolArgs;

        try
        {
            using var requestDocument = JsonDocument.Parse(req.Request);
            var paramsElement = requestDocument.RootElement.GetProperty("params");

            toolName = paramsElement.GetProperty("name").GetString();
            toolArgs = new Dictionary<string, object?>();

            if (paramsElement.TryGetProperty("arguments", out var argsElement))
            {
                foreach (var property in argsElement.EnumerateObject())
                {
                    toolArgs[property.Name] = JsonElementToObject(property.Value);
                }
            }
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

        var candidateServices = _config.McpServices
            .Where(service => service.SelectedTools != null && service.SelectedTools.Any(tool => tool.Name == toolName))
            .ToList();

        if (candidateServices.Count == 0)
        {
            _logger.LogWarning("Tuya server {ServerId}: tool {ToolName} not found in any configured service", ServerId, toolName);
            await ReplyErrorAsync(req, $"tool {toolName} not configured", ct);
            return;
        }

        Exception? lastException = null;
        object? finalResult = null;
        var userContextHeaders = await GetUserContextHeadersAsync();

        try
        {
            foreach (var service in candidateServices)
            {
                McpClient? transientClient = null;
                try
                {
                    transientClient = await _mcpClientService.CreateMcpClientAsync(
                        $"McpService_{service.ServiceName}",
                        service.NodeAddress,
                        service.Protocol,
                        service.AuthenticationType,
                        service.AuthenticationConfig,
                        additionalHeaders: userContextHeaders,
                        cancellationToken: ct);

                    finalResult = await transientClient.CallToolAsync(toolName, toolArgs, cancellationToken: ct);

                    await transientClient.DisposeAsync();
                    transientClient = null;
                    break;
                }
                catch (Exception ex)
                {
                    lastException = ex;
                    _logger.LogWarning(
                        ex,
                        "Tuya server {ServerId}: tool {ToolName} call failed on service {ServiceName}, trying next",
                        ServerId,
                        toolName,
                        service.ServiceName);
                }
                finally
                {
                    if (transientClient != null)
                    {
                        try { await transientClient.DisposeAsync(); } catch { }
                    }
                }
            }

            if (finalResult != null)
            {
                var resultJson = JsonSerializer.Serialize(finalResult, McpJsonOptions);
                await ReplyAsync(req, resultJson, ct);
                return;
            }

            throw lastException ?? new InvalidOperationException($"Tool {toolName} call failed on all candidate services");
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

    private (Dictionary<string, object> Properties, string[] Required) ParseToolSchema(SelectedToolInfo tool)
    {
        var properties = new Dictionary<string, object>();
        var required = Array.Empty<string>();

        if (string.IsNullOrEmpty(tool.InputSchema))
        {
            return (properties, required);
        }

        try
        {
            using var schemaDoc = JsonDocument.Parse(tool.InputSchema);

            if (schemaDoc.RootElement.TryGetProperty("properties", out var propsElement))
            {
                properties = JsonElementToObject(propsElement) as Dictionary<string, object>
                    ?? new Dictionary<string, object>();
            }

            if (schemaDoc.RootElement.TryGetProperty("required", out var requiredElement))
            {
                required = requiredElement.EnumerateArray()
                    .Select(item => item.GetString() ?? string.Empty)
                    .ToArray();
            }
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Tuya server {ServerId}: failed to parse InputSchema for tool {ToolName}", ServerId, tool.Name);
        }

        return (properties, required);
    }

    private static object? JsonElementToObject(JsonElement element)
    {
        return element.ValueKind switch
        {
            JsonValueKind.String => element.GetString(),
            JsonValueKind.Number => element.TryGetInt32(out var intValue) ? intValue : element.GetDouble(),
            JsonValueKind.True => true,
            JsonValueKind.False => false,
            JsonValueKind.Null => null,
            JsonValueKind.Array => element.EnumerateArray().Select(JsonElementToObject).ToArray(),
            JsonValueKind.Object => element.EnumerateObject().ToDictionary(property => property.Name, property => JsonElementToObject(property.Value)),
            _ => element.ToString()
        };
    }

    private async Task<Dictionary<string, string>?> GetUserContextHeadersAsync()
    {
        try
        {
            using var scope = _serviceScopeFactory.CreateScope();
            var userInfoService = scope.ServiceProvider.GetRequiredService<IUserInfoService>();

            var userInfoMap = await userInfoService.GetUsersByIdsAsync(new[] { _config.UserId });
            if (!userInfoMap.TryGetValue(_config.UserId, out var userInfo))
            {
                _logger.LogWarning(
                    "Tuya server {ServerId}: user {UserId} not found, user context headers will not be added",
                    ServerId,
                    _config.UserId);
                return null;
            }

            var headers = new Dictionary<string, string>
            {
                ["X-User-Id"] = userInfo.UserId
            };

            if (!string.IsNullOrEmpty(userInfo.Email))
            {
                headers["X-User-Email"] = userInfo.Email;
            }

            _logger.LogDebug(
                "Tuya server {ServerId}: adding user context headers: UserId={UserId}, Email={Email}",
                ServerId,
                userInfo.UserId,
                userInfo.Email ?? "(not set)");

            return headers;
        }
        catch (Exception ex)
        {
            _logger.LogError(
                ex,
                "Tuya server {ServerId}: error fetching user information, user context headers will not be added",
                ServerId);
            return null;
        }
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
