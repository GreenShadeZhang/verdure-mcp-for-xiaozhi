using System.ComponentModel.DataAnnotations;

namespace Verdure.McpPlatform.Contracts.Requests;

/// <summary>
/// Request to create a new MCP Server
/// </summary>
public record CreateXiaozhiMcpEndpointRequest
{
    [Required(ErrorMessage = "Server name is required")]
    [StringLength(200, ErrorMessage = "Name cannot exceed 200 characters")]
    public string Name { get; init; } = string.Empty;

    [Required(ErrorMessage = "Server address is required")]
    [StringLength(500, ErrorMessage = "Address cannot exceed 500 characters")]
    public string Address { get; init; } = string.Empty;

    [StringLength(1000, ErrorMessage = "Description cannot exceed 1000 characters")]
    public string? Description { get; init; }

    /// <summary>Connection type: "xiaozhi" (default) or "tuya".</summary>
    [StringLength(20, ErrorMessage = "ConnectionType cannot exceed 20 characters")]
    public string? ConnectionType { get; init; }

    /// <summary>Tuya Access ID. Required when ConnectionType is "tuya".</summary>
    [StringLength(200, ErrorMessage = "TuyaAccessId cannot exceed 200 characters")]
    public string? TuyaAccessId { get; init; }

    /// <summary>Tuya Access Secret. Required when ConnectionType is "tuya".</summary>
    [StringLength(200, ErrorMessage = "TuyaAccessSecret cannot exceed 200 characters")]
    public string? TuyaAccessSecret { get; init; }
}
