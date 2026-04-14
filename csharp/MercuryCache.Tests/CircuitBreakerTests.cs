using System;
using System.Threading;
using MercuryCache.Core;
using Xunit;

namespace MercuryCache.Tests
{
    public class CircuitBreakerTests
    {
        [Fact]
        public void InitialState_IsClosed()
        {
            var cb = new CircuitBreaker();
            Assert.Equal(CircuitBreaker.State.Closed, cb.CurrentState);
        }

        [Fact]
        public void AllowRequest_WhenClosed_ReturnsTrue()
        {
            var cb = new CircuitBreaker();
            Assert.True(cb.AllowRequest());
        }

        [Fact]
        public void RecordFailures_TripsToOpen_AtThreshold()
        {
            var cb = new CircuitBreaker(failureThreshold: 3, recoveryTimeoutSecs: 60);
            cb.RecordFailure();
            cb.RecordFailure();
            Assert.Equal(CircuitBreaker.State.Closed, cb.CurrentState);

            cb.RecordFailure(); // reaches threshold
            Assert.Equal(CircuitBreaker.State.Open, cb.CurrentState);
        }

        [Fact]
        public void AllowRequest_WhenOpen_ReturnsFalse()
        {
            var cb = new CircuitBreaker(failureThreshold: 1, recoveryTimeoutSecs: 60);
            cb.RecordFailure();
            Assert.Equal(CircuitBreaker.State.Open, cb.CurrentState);
            Assert.False(cb.AllowRequest());
        }

        [Fact]
        public void RecordSuccess_WhenClosed_ResetsFailureCount()
        {
            var cb = new CircuitBreaker(failureThreshold: 3, recoveryTimeoutSecs: 60);
            cb.RecordFailure();
            cb.RecordFailure();
            cb.RecordSuccess(); // reset failures
            cb.RecordFailure();
            cb.RecordFailure();
            // Only 2 failures after the reset — should still be Closed
            Assert.Equal(CircuitBreaker.State.Closed, cb.CurrentState);
        }

        [Fact]
        public void OpenCircuit_TransitionsToHalfOpen_AfterRecoveryTimeout()
        {
            // Use 0-second recovery to trigger immediately
            var cb = new CircuitBreaker(failureThreshold: 1, recoveryTimeoutSecs: 0);
            cb.RecordFailure();
            Assert.Equal(CircuitBreaker.State.Open, cb.CurrentState);

            // Allow a moment for the clock to advance past the 0-second timeout
            Thread.Sleep(10);

            bool allowed = cb.AllowRequest(); // should flip to HalfOpen + allow
            Assert.True(allowed);
            Assert.Equal(CircuitBreaker.State.HalfOpen, cb.CurrentState);
        }

        [Fact]
        public void HalfOpen_SuccessesClose_Circuit()
        {
            var cb = new CircuitBreaker(failureThreshold: 1, recoveryTimeoutSecs: 0,
                                        halfOpenSuccessNeeded: 2);
            cb.RecordFailure();
            Thread.Sleep(10);
            cb.AllowRequest(); // → HalfOpen

            cb.RecordSuccess();
            Assert.Equal(CircuitBreaker.State.HalfOpen, cb.CurrentState); // not yet

            cb.RecordSuccess(); // second success → Closed
            Assert.Equal(CircuitBreaker.State.Closed, cb.CurrentState);
        }

        [Fact]
        public void HalfOpen_FailureReopens_Circuit()
        {
            var cb = new CircuitBreaker(failureThreshold: 1, recoveryTimeoutSecs: 0);
            cb.RecordFailure();
            Thread.Sleep(10);
            cb.AllowRequest(); // → HalfOpen
            Assert.Equal(CircuitBreaker.State.HalfOpen, cb.CurrentState);

            cb.RecordFailure(); // fail again while half-open → Open again
            Assert.Equal(CircuitBreaker.State.Open, cb.CurrentState);
        }

        [Fact]
        public void ClosedCircuit_DoesNotOpen_BelowThreshold()
        {
            var cb = new CircuitBreaker(failureThreshold: 5, recoveryTimeoutSecs: 30);
            for (int i = 0; i < 4; i++) cb.RecordFailure();
            Assert.Equal(CircuitBreaker.State.Closed, cb.CurrentState);
        }

        [Fact]
        public void MultipleTrips_RecoverCorrectly()
        {
            var cb = new CircuitBreaker(failureThreshold: 2, recoveryTimeoutSecs: 0,
                                        halfOpenSuccessNeeded: 1);

            // Trip 1
            cb.RecordFailure(); cb.RecordFailure();
            Assert.Equal(CircuitBreaker.State.Open, cb.CurrentState);

            Thread.Sleep(10);
            cb.AllowRequest();   // HalfOpen
            cb.RecordSuccess();  // Closed
            Assert.Equal(CircuitBreaker.State.Closed, cb.CurrentState);

            // Trip 2
            cb.RecordFailure(); cb.RecordFailure();
            Assert.Equal(CircuitBreaker.State.Open, cb.CurrentState);

            Thread.Sleep(10);
            cb.AllowRequest();  // HalfOpen
            cb.RecordSuccess(); // Closed again
            Assert.Equal(CircuitBreaker.State.Closed, cb.CurrentState);
        }
    }
}
