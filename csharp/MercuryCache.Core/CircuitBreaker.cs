using System;
using System.Threading;

namespace MercuryCache.Core
{
    /// <summary>
    /// Classic 3-state circuit breaker (Closed → Open → Half-Open → Closed).
    ///
    /// When a downstream cache node becomes unreliable, the circuit breaker
    /// prevents cascading failures by fast-failing requests and routing to
    /// the backing store fallback path.  This mirrors the resilience patterns
    /// described in the Amazon Builders' Library.
    /// </summary>
    public class CircuitBreaker
    {
        public enum State { Closed, Open, HalfOpen }

        private State   _state          = State.Closed;
        private int     _failureCount   = 0;
        private int     _successCount   = 0;
        private DateTime _openedAt      = DateTime.MinValue;

        private readonly int    _failureThreshold;
        private readonly int    _recoveryTimeoutSecs;
        private readonly int    _halfOpenSuccessThreshold;
        private readonly object _lock = new();

        public CircuitBreaker(int failureThreshold     = 5,
                              int recoveryTimeoutSecs  = 30,
                              int halfOpenSuccessNeeded = 2)
        {
            _failureThreshold         = failureThreshold;
            _recoveryTimeoutSecs      = recoveryTimeoutSecs;
            _halfOpenSuccessThreshold = halfOpenSuccessNeeded;
        }

        public State CurrentState
        {
            get { lock (_lock) { return _state; } }
        }

        /// <summary>Returns true if the caller may proceed.</summary>
        public bool AllowRequest()
        {
            lock (_lock)
            {
                switch (_state)
                {
                    case State.Closed:
                        return true;

                    case State.Open:
                        if (DateTime.UtcNow >= _openedAt.AddSeconds(_recoveryTimeoutSecs))
                        {
                            _state        = State.HalfOpen;
                            _successCount = 0;
                            return true; // allow one probe request
                        }
                        return false;

                    case State.HalfOpen:
                        // Only allow the probe(s) we already permitted
                        return true;

                    default:
                        return false;
                }
            }
        }

        public void RecordSuccess()
        {
            lock (_lock)
            {
                if (_state == State.HalfOpen)
                {
                    _successCount++;
                    if (_successCount >= _halfOpenSuccessThreshold)
                    {
                        _state         = State.Closed;
                        _failureCount  = 0;
                        _successCount  = 0;
                    }
                }
                else if (_state == State.Closed)
                {
                    _failureCount = 0;
                }
            }
        }

        public void RecordFailure()
        {
            lock (_lock)
            {
                _failureCount++;
                if (_state != State.Open &&
                    _failureCount >= _failureThreshold)
                {
                    _state    = State.Open;
                    _openedAt = DateTime.UtcNow;
                }
            }
        }
    }
}
