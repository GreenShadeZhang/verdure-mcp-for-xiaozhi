namespace Verdure.McpPlatform.Api.Services.WebSocket;

/// <summary>
/// Common interface for MCP session services (Xiaozhi and Tuya).
/// </summary>
public interface ISessionService : IAsyncDisposable
{
    string ServerId { get; }
    string ServerName { get; }
    bool IsConnected { get; }
    int ConnectedClientsCount { get; }
    int TotalConfiguredClients { get; }
    int ReconnectAttempts { get; }
    DateTime? LastConnectedTime { get; }
    DateTime? LastDisconnectedTime { get; }
    DateTime LastPingReceivedTime { get; }
    bool IsPingTimeout { get; }

    event Func<Task>? OnConnected;
    event Func<string, Task>? OnConnectionFailed;
    event Func<Task>? OnDisconnected;

    Task StartAsync(CancellationToken cancellationToken = default);
    Task StopAsync();
}
