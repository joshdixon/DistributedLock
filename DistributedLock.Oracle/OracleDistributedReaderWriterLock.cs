using Medallion.Threading.Internal;
using Medallion.Threading.Internal.Data;
using System;
using System.Collections.Generic;
using System.Data;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Medallion.Threading.Oracle
{
    /// <summary>
    /// Implements an upgradeable distributed reader-writer lock for the Oracle database using the DBMS_LOCK package.
    /// </summary>
    public sealed partial class OracleDistributedReaderWriterLock : IInternalDistributedUpgradeableReaderWriterLock<OracleDistributedReaderWriterLockHandle, OracleDistributedReaderWriterLockUpgradeableHandle>
    {
        private readonly IDbDistributedLock _internalLock;
        private readonly OracleDistributedLockOptions _options;

        /// <summary>
        /// Constructs a new lock using the provided <paramref name="name"/>. 
        /// 
        /// The provided <paramref name="connectionString"/> will be used to connect to the database.
        /// 
        /// Unless <paramref name="exactName"/> is specified, <paramref name="name"/> will be escaped/hashed to ensure name validity.
        /// </summary>
        public OracleDistributedReaderWriterLock(string name, string connectionString, Action<OracleConnectionOptionsBuilder>? options = null, bool exactName = false)
            : this(name, exactName, (n, o) => OracleDistributedLock.CreateInternalLock(n, connectionString, o), options)
        {
        }

        /// <summary>
        /// Constructs a new lock using the provided <paramref name="name"/>.
        /// 
        /// The provided <paramref name="connection"/> will be used to connect to the database and will provide lock scope. It is assumed to be externally managed and
        /// will not be opened or closed.
        /// 
        /// Unless <paramref name="exactName"/> is specified, <paramref name="name"/> will be escaped/hashed to ensure name validity.
        /// </summary>
        public OracleDistributedReaderWriterLock(string name, IDbConnection connection, bool exactName = false)
            : this(name, exactName, (n, o) => OracleDistributedLock.CreateInternalLock(n, connection))
        {
        }

        private OracleDistributedReaderWriterLock(string name, bool exactName, Func<string, OracleDistributedLockOptions, IDbDistributedLock> internalLockFactory, Action<OracleConnectionOptionsBuilder>? options = null)
        {
            this.Name = OracleDistributedLock.GetName(name, exactName);
            this._options = OracleConnectionOptionsBuilder.GetOptions(options);
            this._internalLock = internalLockFactory(this.Name, this._options);
        }

        /// <summary>
        /// Implements <see cref="IDistributedLock.Name"/>
        /// </summary>
        public string Name { get; }

        ValueTask<OracleDistributedReaderWriterLockUpgradeableHandle?> IInternalDistributedUpgradeableReaderWriterLock<OracleDistributedReaderWriterLockHandle, OracleDistributedReaderWriterLockUpgradeableHandle>.InternalTryAcquireUpgradeableReadLockAsync(
            TimeoutValue timeout,
            CancellationToken cancellationToken) =>
            this._internalLock.TryAcquireAsync(timeout, OracleDbmsLock.UpdateLock, cancellationToken, contextHandle: null)
                .Instrument(this._options.UseInstrumentation, @lock: this, Instrumentation.ReaderWriterLockLevel.UpgradeableRead, timeout, cancellationToken)
                .Wrap(h => new OracleDistributedReaderWriterLockUpgradeableHandle(h, this._internalLock, this, this._options.UseInstrumentation));

        ValueTask<OracleDistributedReaderWriterLockHandle?> IInternalDistributedReaderWriterLock<OracleDistributedReaderWriterLockHandle>.InternalTryAcquireAsync(
            TimeoutValue timeout,
            CancellationToken cancellationToken,
            bool isWrite) =>
            this._internalLock.TryAcquireAsync(timeout, isWrite ? OracleDbmsLock.ExclusiveLock : OracleDbmsLock.SharedLock, cancellationToken, contextHandle: null)
                .Instrument(this._options.UseInstrumentation, @lock: this, isWrite ? Instrumentation.ReaderWriterLockLevel.Write : Instrumentation.ReaderWriterLockLevel.Read, timeout, cancellationToken)
                .Wrap<OracleDistributedReaderWriterLockHandle>(h => new OracleDistributedReaderWriterLockNonUpgradeableHandle(h));
    }
}
