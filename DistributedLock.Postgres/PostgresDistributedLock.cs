using Medallion.Threading.Internal;
using Medallion.Threading.Internal.Data;
using System;
using System.Collections.Generic;
using System.Data;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Medallion.Threading.Postgres
{
    /// <summary>
    /// Implements a distributed lock using Postgres advisory locks
    /// (see https://www.postgresql.org/docs/12/functions-admin.html#FUNCTIONS-ADVISORY-LOCKS)
    /// </summary>
    public sealed partial class PostgresDistributedLock : IInternalDistributedLock<PostgresDistributedLockHandle>
    {
        private readonly IDbDistributedLock _internalLock;
        private readonly PostgresDistributedLockOptions _options;

        /// <summary>
        /// Constructs a lock with the given <paramref name="key"/> (effectively the lock name), <paramref name="connectionString"/>,
        /// and <paramref name="options"/>
        /// </summary>
        public PostgresDistributedLock(PostgresAdvisoryLockKey key, string connectionString, Action<PostgresConnectionOptionsBuilder>? options = null)
            : this(key, o => CreateInternalLock(key, connectionString, o), options)
        {
        }

        /// <summary>
        /// Constructs a lock with the given <paramref name="key"/> (effectively the lock name) and <paramref name="connection"/>.
        /// </summary>
        public PostgresDistributedLock(PostgresAdvisoryLockKey key, IDbConnection connection)
            : this(key, o => CreateInternalLock(key, connection))
        {
        }

        private PostgresDistributedLock(PostgresAdvisoryLockKey key, Func<PostgresDistributedLockOptions, IDbDistributedLock> internalLockFactory, Action<PostgresConnectionOptionsBuilder>? options = null)
        {
            this.Key = key;
            this._options = PostgresConnectionOptionsBuilder.GetOptions(options);
            this._internalLock = internalLockFactory(this._options);
        }

        /// <summary>
        /// The <see cref="PostgresAdvisoryLockKey"/> that uniquely identifies the lock on the database
        /// </summary>
        public PostgresAdvisoryLockKey Key { get; }

        string IDistributedLock.Name => this.Key.ToString();

        ValueTask<PostgresDistributedLockHandle?> IInternalDistributedLock<PostgresDistributedLockHandle>.InternalTryAcquireAsync(TimeoutValue timeout, CancellationToken cancellationToken) =>
            this._internalLock.TryAcquireAsync(timeout, PostgresAdvisoryLock.ExclusiveLock, cancellationToken, contextHandle: null)
                .Instrument(this._options.UseInstrumentation, @lock: this, timeout, cancellationToken)
                .Wrap(h => new PostgresDistributedLockHandle(h));

        internal static IDbDistributedLock CreateInternalLock(PostgresAdvisoryLockKey key, string connectionString, PostgresDistributedLockOptions options)
        {
            if (connectionString == null) { throw new ArgumentNullException(nameof(connectionString)); }

            if (options.UseMultiplexing)
            {
                return new OptimisticConnectionMultiplexingDbDistributedLock(key.ToString(), connectionString, PostgresMultiplexedConnectionLockPool.Instance, options.KeepaliveCadence);
            }

            return new DedicatedConnectionOrTransactionDbDistributedLock(key.ToString(), () => new PostgresDatabaseConnection(connectionString), useTransaction: false, options.KeepaliveCadence);
        }

        internal static IDbDistributedLock CreateInternalLock(PostgresAdvisoryLockKey key, IDbConnection connection)
        {
            if (connection == null) { throw new ArgumentNullException(nameof(connection)); }
            return new DedicatedConnectionOrTransactionDbDistributedLock(key.ToString(), () => new PostgresDatabaseConnection(connection));
        }
    }
}
