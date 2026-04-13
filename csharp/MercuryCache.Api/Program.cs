using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Configuration;
using MercuryCache.Core;
using MercuryCache.Cluster;
using System;

var builder = WebApplication.CreateBuilder(args);

// ---- Configuration ---------------------------------------------------------
var config = builder.Configuration;

// ---- Services --------------------------------------------------------------
builder.Services.AddControllers();
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen(c =>
{
    c.SwaggerDoc("v1", new() { Title = "MercuryCache++ API", Version = "v1",
        Description = "Amazon-style distributed cache inspired by DynamoDB DAX and ElastiCache" });
});

// Core services
builder.Services.AddSingleton<ConsistentHashRing>(sp =>
{
    var ring = new ConsistentHashRing(virtualNodesPerPhysical: 150);
    // Seed initial nodes from config
    var nodes = config.GetSection("Cache:Nodes").Get<string[]>() ?? Array.Empty<string>();
    foreach (var node in nodes) ring.AddNode(node);
    return ring;
});

builder.Services.AddSingleton<CacheNodeClientFactory>();
builder.Services.AddSingleton<BackingStoreAdapter>(sp =>
{
    var connStr = config.GetConnectionString("BackingStore")
                  ?? "Host=localhost;Database=mercury_source;Username=mercury;Password=mercury";
    return new BackingStoreAdapter(connStr);
});

builder.Services.AddSingleton<SingleFlightManager>();
builder.Services.AddSingleton<CircuitBreaker>(sp =>
    new CircuitBreaker(failureThreshold: 5, recoveryTimeoutSecs: 30));
builder.Services.AddSingleton<ReplicationCoordinator>();
builder.Services.AddSingleton<HotKeyReplicationManager>();
builder.Services.AddSingleton<InvalidationPublisher>();
builder.Services.AddSingleton<ClusterMembershipService>();
builder.Services.AddSingleton<MetricsAggregator>();
builder.Services.AddSingleton<RequestCoordinator>();

// Health checks
builder.Services.AddHealthChecks()
    .AddCheck<ClusterHealthCheck>("cluster");

var app = builder.Build();

// ---- Middleware Pipeline ----------------------------------------------------
if (app.Environment.IsDevelopment())
{
    app.UseSwagger();
    app.UseSwaggerUI();
}

app.UseRouting();
app.UseHttpsRedirection();
app.MapControllers();
app.MapHealthChecks("/health");

// ---- Startup validation ----------------------------------------------------
var ring = app.Services.GetRequiredService<ConsistentHashRing>();
Console.WriteLine($"[MercuryCache] API gateway started. Ring has {ring.NodeCount} nodes.");

app.Run();
