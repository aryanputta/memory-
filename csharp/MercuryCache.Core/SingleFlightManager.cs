using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace MercuryCache.Core
{
    /// <summary>
    /// C# equivalent of the C++ SingleFlightTable.
    /// Prevents thundering herd: only one coroutine executes a loader per key;
    /// all concurrent callers share the same Task.
    ///
    /// Referenced in Amazon Builders' Library:
    /// "Caching challenges and strategies" — request coalescing pattern.
    /// </summary>
    public class SingleFlightManager
    {
        private readonly Dictionary<string, object> _tasks = new();
        private readonly object _taskLock = new();

        public async Task<T> ExecuteOncePerKeyAsync<T>(string key, Func<Task<T>> loader)
        {
            Task<T>? existing;
            TaskCompletionSource<T>? tcs = null;

            lock (_taskLock)
            {
                if (_tasks.TryGetValue(key, out var obj))
                {
                    existing = (Task<T>)obj;
                }
                else
                {
                    tcs = new TaskCompletionSource<T>(
                        TaskCreationOptions.RunContinuationsAsynchronously);
                    _tasks[key] = tcs.Task;
                    existing    = null;
                }
            }

            if (existing != null)
                return await existing;

            // This goroutine is the leader
            try
            {
                T result = await loader();
                tcs!.SetResult(result);
                return result;
            }
            catch (Exception ex)
            {
                tcs!.SetException(ex);
                throw;
            }
            finally
            {
                lock (_taskLock) { _tasks.Remove(key); }
            }
        }

        public int InFlightCount
        {
            get { lock (_taskLock) { return _tasks.Count; } }
        }
    }
}
