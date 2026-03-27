using Verdure.McpPlatform.Domain.SeedWork;

namespace Verdure.McpPlatform.Domain.AggregatesModel.XiaozhiMcpEndpointAggregate;

/// <summary>
/// Xiaozhi MCP Endpoint aggregate root - represents an MCP endpoint configuration.
/// Supports both Xiaozhi AI (WebSocket) and Tuya IoT Cloud (HMAC-signed WebSocket) connection types.
/// </summary>
public class XiaozhiMcpEndpoint : Entity, IAggregateRoot
{
    public string Name { get; private set; }
    /// <summary>
    /// For Xiaozhi: WebSocket endpoint URL (ws://...).
    /// For Tuya: Tuya IoT platform REST base URL (https://openapi.tuyaeu.com).
    /// </summary>
    public string Address { get; private set; }
    public string UserId { get; private set; }
    public string? Description { get; private set; }
    public bool IsEnabled { get; private set; }
    public bool IsConnected { get; private set; }
    public DateTime CreatedAt { get; private set; }
    public DateTime? UpdatedAt { get; private set; }
    public DateTime? LastConnectedAt { get; private set; }
    public DateTime? LastDisconnectedAt { get; private set; }

    /// <summary>
    /// Connection type: "xiaozhi" (default) or "tuya".
    /// </summary>
    public string ConnectionType { get; private set; }

    /// <summary>Tuya Access ID (also called access_key). Required when ConnectionType is "tuya".</summary>
    public string? TuyaAccessId { get; private set; }

    /// <summary>Tuya Access Secret. Required when ConnectionType is "tuya".</summary>
    public string? TuyaAccessSecret { get; private set; }

    private readonly List<McpServiceBinding> _serviceBindings;
    public IReadOnlyCollection<McpServiceBinding> ServiceBindings => _serviceBindings.AsReadOnly();

    protected XiaozhiMcpEndpoint()
    {
        _serviceBindings = new List<McpServiceBinding>();
        Name = string.Empty;
        Address = string.Empty;
        UserId = string.Empty;
        ConnectionType = "xiaozhi";
    }

    public XiaozhiMcpEndpoint(string name, string address, string userId, string? description = null,
        string? connectionType = null, string? tuyaAccessId = null, string? tuyaAccessSecret = null) : this()
    {
        GenerateId(); // Generate Guid Version 7 ID
        Name = name ?? throw new ArgumentNullException(nameof(name));
        Address = address ?? throw new ArgumentNullException(nameof(address));
        UserId = userId ?? throw new ArgumentNullException(nameof(userId));
        Description = description;
        ConnectionType = string.IsNullOrWhiteSpace(connectionType) ? "xiaozhi" : connectionType.ToLowerInvariant();
        TuyaAccessId = tuyaAccessId;
        TuyaAccessSecret = tuyaAccessSecret;
        IsEnabled = false; // Disabled by default until user enables
        IsConnected = false;
        CreatedAt = DateTime.UtcNow;
    }

    public void UpdateInfo(string name, string address, string? description = null,
        string? connectionType = null, string? tuyaAccessId = null, string? tuyaAccessSecret = null)
    {
        Name = name ?? throw new ArgumentNullException(nameof(name));
        Address = address ?? throw new ArgumentNullException(nameof(address));
        Description = description;
        ConnectionType = string.IsNullOrWhiteSpace(connectionType) ? "xiaozhi" : connectionType.ToLowerInvariant();
        TuyaAccessId = tuyaAccessId;
        TuyaAccessSecret = tuyaAccessSecret;
        UpdatedAt = DateTime.UtcNow;
    }

    public McpServiceBinding AddServiceBinding(
        string mcpServiceConfigId,
        string? description = null,
        IEnumerable<string>? selectedToolNames = null)
    {
        var binding = new McpServiceBinding(Id, mcpServiceConfigId, UserId, description, selectedToolNames);
        _serviceBindings.Add(binding);
        return binding;
    }

    public void RemoveServiceBinding(McpServiceBinding binding)
    {
        _serviceBindings.Remove(binding);
    }

    public void Enable()
    {
        IsEnabled = true;
        UpdatedAt = DateTime.UtcNow;
    }

    public void Disable()
    {
        IsEnabled = false;
        IsConnected = false;
        UpdatedAt = DateTime.UtcNow;
    }

    public void SetConnected()
    {
        IsConnected = true;
        LastConnectedAt = DateTime.UtcNow;
    }

    public void SetDisconnected()
    {
        IsConnected = false;
        LastDisconnectedAt = DateTime.UtcNow;
    }
}
