using Medallion.Threading.Internal;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;

namespace Medallion.Threading.MySql
{
    /// <summary>
    /// Specifies options for connecting to and locking against a MySQL database
    /// </summary>
    public sealed class MySqlConnectionOptionsBuilder : IInternalInstrumentationOptionsBuilder<MySqlConnectionOptionsBuilder>
    {
        private TimeoutValue? _keepaliveCadence;
        private bool? _useMultiplexing;
        private bool? _useInstrumentation;

        internal MySqlConnectionOptionsBuilder() { }

        /// <summary>
        /// MySQL's wait_timeout system variable determines how long the server will allow a connection to be idle before killing it.
        /// For more information, see https://dev.mysql.com/doc/refman/5.7/en/server-system-variables.html#sysvar_wait_timeout.
        /// 
        /// To prevent this, this option sets the cadence at which we run a no-op "keepalive" query on a connection that is holding a lock.
        /// 
        /// Because MySQL's default for this setting is 8 hours, the default <paramref name="keepaliveCadence"/> is 3.5 hours.
        /// 
        /// Setting a value of <see cref="Timeout.InfiniteTimeSpan"/> disables keepalive.
        /// </summary>
        public MySqlConnectionOptionsBuilder KeepaliveCadence(TimeSpan keepaliveCadence)
        {
            this._keepaliveCadence = new TimeoutValue(keepaliveCadence, nameof(keepaliveCadence));
            return this;
        }

        /// <summary>
        /// This mode takes advantage of the fact that while "holding" a lock (or other synchronization primitive)
        /// a connection is essentially idle. Thus, rather than creating a new connection for each held lock it is 
        /// often possible to multiplex a shared connection so that that connection can hold multiple locks at the same time.
        /// 
        /// Multiplexing is on by default.
        /// 
        /// This is implemented in such a way that releasing a lock held on such a connection will never be blocked by an
        /// Acquire() call that is waiting to acquire a lock on that same connection. For this reason, the multiplexing
        /// strategy is "optimistic": if the lock can't be acquired instantaneously on the shared connection, a new (shareable) 
        /// connection will be allocated.
        /// 
        /// This option can improve performance and avoid connection pool starvation in high-load scenarios. It is also
        /// particularly applicable to cases where <see cref="IDistributedLock.TryAcquire(TimeSpan, System.Threading.CancellationToken)"/>
        /// semantics are used with a zero-length timeout.
        /// </summary>
        public MySqlConnectionOptionsBuilder UseMultiplexing(bool useMultiplexing = true)
        {
            this._useMultiplexing = useMultiplexing;
            return this;
        }

        /// <inheritdoc />
        public MySqlConnectionOptionsBuilder UseInstrumentation(bool useInstrumentation = true)
        {
            this._useInstrumentation = useInstrumentation;
            return this;
        }

        internal static MySqlDistributedLockOptions GetOptions(Action<MySqlConnectionOptionsBuilder>? optionsBuilder)
        {
            MySqlConnectionOptionsBuilder? options;
            if (optionsBuilder != null)
            {
                options = new MySqlConnectionOptionsBuilder();
                optionsBuilder(options);
            }
            else
            {
                options = null;
            }

            return new MySqlDistributedLockOptions(
                keepaliveCadence: options?._keepaliveCadence ?? TimeSpan.FromHours(3.5),
                useMultiplexing: options?._useMultiplexing ?? true,
                useInstrumentation: options?._useInstrumentation ?? false
            );
        }
    }

    internal readonly struct MySqlDistributedLockOptions
    {
        public MySqlDistributedLockOptions(
            TimeoutValue keepaliveCadence,
            bool useMultiplexing,
            bool useInstrumentation)
        {
            this.KeepaliveCadence = keepaliveCadence;
            this.UseMultiplexing = useMultiplexing;
            this.UseInstrumentation = useInstrumentation;
        }

        public TimeoutValue KeepaliveCadence { get; }
        public bool UseMultiplexing { get; }
        public bool UseInstrumentation { get; }
    }
}
