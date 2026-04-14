using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using MercuryCache.Core;
using Xunit;

namespace MercuryCache.Tests
{
    public class SingleFlightManagerTests
    {
        [Fact]
        public async Task ExecuteOncePerKey_ReturnsCorrectResult()
        {
            var sfm = new SingleFlightManager();
            var result = await sfm.ExecuteOncePerKeyAsync("key1", () => Task.FromResult(42));
            Assert.Equal(42, result);
        }

        [Fact]
        public async Task ConcurrentCallers_SameKey_LoaderCalledOnce()
        {
            var sfm    = new SingleFlightManager();
            int called = 0;
            var tcs    = new TaskCompletionSource<int>(
                             TaskCreationOptions.RunContinuationsAsynchronously);

            // Loader blocks until we release it
            Func<Task<int>> loader = () =>
            {
                Interlocked.Increment(ref called);
                return tcs.Task;
            };

            // Launch 20 concurrent callers for the same key
            var tasks = new List<Task<int>>();
            for (int i = 0; i < 20; i++)
                tasks.Add(sfm.ExecuteOncePerKeyAsync("hot-key", loader));

            // Let the loader coroutines start
            await Task.Delay(50);
            Assert.Equal(1, called); // loader invoked exactly once

            tcs.SetResult(99);
            var results = await Task.WhenAll(tasks);

            // All callers get the same value
            foreach (var r in results)
                Assert.Equal(99, r);
        }

        [Fact]
        public async Task DifferentKeys_LoaderCalledPerKey()
        {
            var sfm    = new SingleFlightManager();
            int called = 0;

            Func<string, Task<string>> loader = async (key) =>
            {
                Interlocked.Increment(ref called);
                await Task.Yield();
                return $"value-{key}";
            };

            var t1 = sfm.ExecuteOncePerKeyAsync("key-a", () => loader("key-a"));
            var t2 = sfm.ExecuteOncePerKeyAsync("key-b", () => loader("key-b"));

            var r1 = await t1;
            var r2 = await t2;

            Assert.Equal("value-key-a", r1);
            Assert.Equal("value-key-b", r2);
            Assert.Equal(2, called); // each key has its own loader
        }

        [Fact]
        public async Task ExceptionInLoader_PropagatedToAllCallers()
        {
            var sfm = new SingleFlightManager();
            var tcs = new TaskCompletionSource<int>(
                         TaskCreationOptions.RunContinuationsAsynchronously);

            var tasks = new List<Task<int>>();
            for (int i = 0; i < 5; i++)
                tasks.Add(sfm.ExecuteOncePerKeyAsync("err-key", () => tcs.Task));

            await Task.Delay(20);
            tcs.SetException(new InvalidOperationException("downstream error"));

            foreach (var t in tasks)
            {
                var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => t);
                Assert.Equal("downstream error", ex.Message);
            }
        }

        [Fact]
        public async Task InFlightCount_ReflectsActiveFlights()
        {
            var sfm = new SingleFlightManager();
            var tcs = new TaskCompletionSource<int>(
                         TaskCreationOptions.RunContinuationsAsynchronously);

            Assert.Equal(0, sfm.InFlightCount);

            var t = sfm.ExecuteOncePerKeyAsync("k", () => tcs.Task);
            await Task.Delay(20); // let it register

            Assert.Equal(1, sfm.InFlightCount);

            tcs.SetResult(1);
            await t;

            // After completion, flight is removed
            await Task.Delay(10);
            Assert.Equal(0, sfm.InFlightCount);
        }

        [Fact]
        public async Task AfterCompletion_NextCall_Executes_NewLoader()
        {
            var sfm    = new SingleFlightManager();
            int called = 0;

            await sfm.ExecuteOncePerKeyAsync("k", async () =>
            {
                Interlocked.Increment(ref called);
                await Task.Yield();
                return "first";
            });

            await sfm.ExecuteOncePerKeyAsync("k", async () =>
            {
                Interlocked.Increment(ref called);
                await Task.Yield();
                return "second";
            });

            Assert.Equal(2, called); // both loaders run because they're sequential
        }
    }
}
