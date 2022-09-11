using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Medallion.Threading.Internal
{
#if DEBUG
    public
#else
    internal
#endif
    class InternalDistributedInstrumentedLockHandle : IDistributedSynchronizationHandle
    {
        private IDistributedSynchronizationHandle? _innerHandle;
        private IDisposable? _activeLockTracker;

        internal InternalDistributedInstrumentedLockHandle(IDistributedSynchronizationHandle innerHandle, IDisposable? activeLockTracker)
        {
            this._innerHandle = innerHandle;
            this._activeLockTracker = activeLockTracker;
        }

        /// <summary>
        /// Implements <see cref="IDistributedSynchronizationHandle.HandleLostToken"/>
        /// </summary>
        public CancellationToken HandleLostToken => this._innerHandle?.HandleLostToken ?? throw this.ObjectDisposed();

        /// <summary>
        /// Releases the lock
        /// </summary>
        public void Dispose()
        {
            Interlocked.Exchange(ref this._activeLockTracker, null)?.Dispose();
            Interlocked.Exchange(ref this._innerHandle, null)?.Dispose();
        }

        /// <summary>
        /// Releases the lock asynchronously
        /// </summary>
        public ValueTask DisposeAsync()
        {
            Interlocked.Exchange(ref this._activeLockTracker, null)?.Dispose();
            return Interlocked.Exchange(ref this._innerHandle, null)?.DisposeAsync() ?? default;
        }
    }
}
