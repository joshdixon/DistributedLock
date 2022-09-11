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
    public sealed partial class PostgresDistributedReaderWriterLock : IInternalDistributedReaderWriterLock<PostgresDistributedReaderWriterLockHandle>
    {
        private readonly IDbDistributedLock _internalLock;
        private readonly PostgresDistributedLockOptions _options;

        /// <summary>
        /// Constructs a lock with the given <paramref name="key"/> (effectively the lock name), <paramref name="connectionString"/>,
        /// and <paramref name="options"/>
        /// </summary>
        public PostgresDistributedReaderWriterLock(PostgresAdvisoryLockKey key, string connectionString, Action<PostgresConnectionOptionsBuilder>? options = null)
            : this(key, o => PostgresDistributedLock.CreateInternalLock(key, connectionString, o), options)
        {
        }

        /// <summary>
        /// Constructs a lock with the given <paramref name="key"/> (effectively the lock name) and <paramref name="connection"/>.
        /// </summary>
        public PostgresDistributedReaderWriterLock(PostgresAdvisoryLockKey key, IDbConnection connection)
            : this(key, o => PostgresDistributedLock.CreateInternalLock(key, connection))
        {
        }

        private PostgresDistributedReaderWriterLock(PostgresAdvisoryLockKey key, Func<PostgresDistributedLockOptions, IDbDistributedLock> internalLockFactory, Action<PostgresConnectionOptionsBuilder>? options = null)
        {
            this.Key = key;
            this._options = PostgresConnectionOptionsBuilder.GetOptions(options);
            this._internalLock = internalLockFactory(this._options);
        }

        /// <summary>
        /// The <see cref="PostgresAdvisoryLockKey"/> that uniquely identifies the lock on the database
        /// </summary>
        public PostgresAdvisoryLockKey Key { get; }

        string IDistributedReaderWriterLock.Name => this.Key.ToString();

        ValueTask<PostgresDistributedReaderWriterLockHandle?> IInternalDistributedReaderWriterLock<PostgresDistributedReaderWriterLockHandle>.InternalTryAcquireAsync(
            TimeoutValue timeout,
            CancellationToken cancellationToken,
            bool isWrite) =>
            this._internalLock.TryAcquireAsync(timeout, isWrite ? PostgresAdvisoryLock.ExclusiveLock : PostgresAdvisoryLock.SharedLock, cancellationToken, contextHandle: null)
                .Instrument(this._options.UseInstrumentation, @lock: this, isWrite ? Instrumentation.ReaderWriterLockLevel.Write : Instrumentation.ReaderWriterLockLevel.Read, timeout, cancellationToken)
                .Wrap(h => new PostgresDistributedReaderWriterLockHandle(h));
    }
}
