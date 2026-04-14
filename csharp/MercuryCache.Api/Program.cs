using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using MercuryCache.Core;
using MercuryCache.Cluster;
using System;
using System.Net;

var builder = WebApplication.CreateBuilder(args);

// ── Kestrel: prevent idle-timeout disconnects ──────────────────────────────
// Without these, long-lived gRPC streams and idle HTTP/1.1 keep-alives drop.
builder.WebHost.ConfigureKestrel(opts =>
{
    // How long a connection can be idle before Kestrel closes it (30 min)
    opts.Limits.KeepAliveTimeout          = TimeSpan.FromMinutes(30);
    // Max time for a client to send the full request headers (10 s)
    opts.Limits.RequestHeadersTimeout     = TimeSpan.FromSeconds(10);
    // Minimum throughput before Kestrel aborts (0 = disabled; avoids
    // slow-client disconnects on large cache payloads)
    opts.Limits.MinRequestBodyDataRate    = null;
    opts.Limits.MinResponseDataRate       = null;
    // Allow HTTP/2 for gRPC internal transport alongside HTTP/1.1 REST
    opts.ConfigureEndpointDefaults(ep => ep.Protocols = HttpProtocols.Http1AndHttp2);
    // gRPC HTTP/2 ping: keep the channel alive through idle periods
    opts.Limits.Http2.KeepAlivePingDelay  = TimeSpan.FromSeconds(15);
    opts.Limits.Http2.KeepAlivePingTimeout = TimeSpan.FromSeconds(10);
});

// ── Configuration ─────────────────────────────────────────────────────────
var config = builder.Configuration;

// ── Logging ───────────────────────────────────────────────────────────────
builder.Logging.ClearProviders();
builder.Logging.AddConsole(o => o.TimestampFormat = "[HH:mm:ss] ");
builder.Logging.SetMinimumLevel(
    builder.Environment.IsDevelopment() ? LogLevel.Debug : LogLevel.Information);

// ── Services ──────────────────────────────────────────────────────────────
builder.Services.AddControllers();
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen(c =>
{
    c.SwaggerDoc("v1", new() {
        Title       = "MercuryCache++ API",
        Version     = "v1",
        Description = "Amazon-style distributed cache (DAX/ElastiCache patterns)"
    });
});

// Consistent-hash ring seeded from config
builder.Services.AddSingleton<ConsistentHashRing>(sp =>
{
    var ring  = new ConsistentHashRing(virtualNodesPerPhysical: 150);
    var nodes = config.GetSection("Cache:Nodes").Get<string[]>() ?? Array.Empty<string>();
    foreach (var node in nodes) ring.AddNode(node);
    return ring;
});

// gRPC node client factory — channels configured with keepalive
builder.Services.AddSingleton<CacheNodeClientFactory>(sp =>
{
    var rf = config.GetValue<int>("Cache:ReplicationFactor", 2);
    return new CacheNodeClientFactory();
});

// Backing store
builder.Services.AddSingleton<BackingStoreAdapter>(sp =>
{
    var connStr = config.GetConnectionString("BackingStore")
        ?? "Host=localhost;Database=mercury_source;Username=mercury;Password=mercury";
    return new BackingStoreAdapter(connStr);
});

builder.Services.AddSingleton<SingleFlightManager>();
builder.Services.AddSingleton<CircuitBreaker>(sp => new CircuitBreaker(
    failureThreshold:     config.GetValue("Cache:CircuitBreaker:FailureThreshold",  5),
    recoveryTimeoutSecs:  config.GetValue("Cache:CircuitBreaker:RecoveryTimeoutSecs", 30)));

builder.Services.AddSingleton<ReplicationCoordinator>();
builder.Services.AddSingleton<HotKeyReplicationManager>();
builder.Services.AddSingleton<InvalidationPublisher>();
builder.Services.AddSingleton<ClusterMembershipService>();
builder.Services.AddSingleton<MetricsAggregator>();
builder.Services.AddSingleton<RequestCoordinator>();

// Hosted service: starts membership heartbeat loop on application start
builder.Services.AddHostedService<ClusterHeartbeatHostedService>();

// Health checks
builder.Services.AddHealthChecks()
    .AddCheck<ClusterHealthCheck>("cluster");

// ── Request timeout middleware (protects against slow clients) ─────────────
builder.Services.AddRequestTimeouts(opts =>
{
    opts.DefaultPolicy = new Microsoft.AspNetCore.Http.Timeouts.RequestTimeoutPolicy
    {
        Timeout = TimeSpan.FromSeconds(10)
    };
});

var app = builder.Build();

// ── Middleware pipeline ────────────────────────────────────────────────────
app.UseRequestTimeouts();

if (app.Environment.IsDevelopment())
{
    app.UseSwagger();
    app.UseSwaggerUI(c => c.SwaggerEndpoint("/swagger/v1/swagger.json",
                                            "MercuryCache++ v1"));
}

app.UseRouting();
// HTTPS redirect intentionally off in dev/Docker to avoid cert issues
if (!app.Environment.IsDevelopment())
    app.UseHttpsRedirection();

app.MapControllers();
app.MapHealthChecks("/health");

// ── Startup summary ───────────────────────────────────────────────────────
var ring = app.Services.GetRequiredService<ConsistentHashRing>();
var log  = app.Services.GetRequiredService<ILogger<Program>>();
log.LogInformation("MercuryCache++ API started. Ring nodes: {N}", ring.NodeCount);

app.Run();
