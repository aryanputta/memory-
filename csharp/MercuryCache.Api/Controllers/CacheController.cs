using System;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using MercuryCache.Api.DTOs;
using MercuryCache.Core;

namespace MercuryCache.Api.Controllers
{
    [ApiController]
    [Route("api/cache")]
    public class CacheController : ControllerBase
    {
        private readonly RequestCoordinator _coordinator;

        public CacheController(RequestCoordinator coordinator)
        {
            _coordinator = coordinator;
        }

        /// <summary>
        /// GET /api/cache/{namespace}/{key}?consistency=eventual&amp;allowStale=false
        /// </summary>
        [HttpGet("{ns}/{key}")]
        public async Task<IActionResult> Get(
            [FromRoute(Name = "ns")] string ns,
            [FromRoute] string key,
            [FromQuery] string consistency = "eventual",
            [FromQuery] bool allowStale = false)
        {
            string traceId = GenerateTraceId();
            var result = await _coordinator.HandleGetAsync(ns, key, consistency,
                                                           allowStale, traceId);
            if (!result.Found)
                return NotFound(result);
            return Ok(result);
        }

        /// <summary>
        /// PUT /api/cache/{namespace}/{key}
        /// </summary>
        [HttpPut("{ns}/{key}")]
        public async Task<IActionResult> Put(
            [FromRoute(Name = "ns")] string ns,
            [FromRoute] string key,
            [FromBody] PutCacheRequest request)
        {
            string traceId = GenerateTraceId();
            var result = await _coordinator.HandlePutAsync(ns, key, request, traceId);
            return Ok(result);
        }

        /// <summary>
        /// DELETE /api/cache/{namespace}/{key}
        /// </summary>
        [HttpDelete("{ns}/{key}")]
        public async Task<IActionResult> Delete(
            [FromRoute(Name = "ns")] string ns,
            [FromRoute] string key)
        {
            string traceId = GenerateTraceId();
            var result = await _coordinator.HandleDeleteAsync(ns, key, traceId);
            return Ok(result);
        }

        /// <summary>
        /// POST /api/cache/bulk-get
        /// </summary>
        [HttpPost("bulk-get")]
        public async Task<IActionResult> BulkGet([FromBody] BulkGetRequest request)
        {
            string traceId = GenerateTraceId();
            var result = await _coordinator.HandleBulkGetAsync(request, traceId);
            return Ok(result);
        }

        // ----------------------------------------------------------------
        private static string GenerateTraceId()
            => $"mercury-{Guid.NewGuid():N}"[..24];
    }
}
