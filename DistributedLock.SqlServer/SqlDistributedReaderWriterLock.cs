using Medallion.Threading.Internal;
using Medallion.Threading.Internal.Data;
using System;
using System.Data;
using System.Threading;
using System.Threading.Tasks;

namespace Medallion.Threading.SqlServer
{
    /// <summary>
    /// Implements reader-writer lock semantics using a SQL server application lock
    /// (see https://msdn.microsoft.com/en-us/library/ms189823.aspx).
    /// 
    /// This class supports the following patterns:
    /// * Multiple readers AND single writer (using <see cref="AcquireReadLock(TimeSpan?, CancellationToken)"/> and <see cref="AcquireUpgradeableReadLock(TimeSpan?, CancellationToken)"/>)
    /// * Multiple readers OR single writer (using <see cref="AcquireReadLock(TimeSpan?, CancellationToken)"/> and <see cref="AcquireWriteLock(TimeSpan?, CancellationToken)"/>)
    /// * Upgradeable read locks similar to <see cref="ReaderWriterLockSlim.EnterUpgradeableReadLock"/> (using <see cref="AcquireUpgradeableReadLock(TimeSpan?, CancellationToken)"/> and <see cref="IDistributedLockUpgradeableHandle.UpgradeToWriteLock(TimeSpan?, CancellationToken)"/>)
    /// </summary>
    public sealed partial class SqlDistributedReaderWriterLock : IInternalDistributedUpgradeableReaderWriterLock<SqlDistributedReaderWriterLockHandle, SqlDistributedReaderWriterLockUpgradeableHandle>
    {
        private readonly IDbDistributedLock _internalLock;
        private readonly SqlDistributedLockOptions _options;

        #region ---- Constructors ----
        /// <summary>
        /// Constructs a new lock using the provided <paramref name="name"/>. 
        /// 
        /// The provided <paramref name="connectionString"/> will be used to connect to the database.
        /// 
        /// Unless <paramref name="exactName"/> is specified, <paramref name="name"/> will be escaped/hashed to ensure name validity.
        /// </summary>
        public SqlDistributedReaderWriterLock(string name, string connectionString, Action<SqlConnectionOptionsBuilder>? options = null, bool exactName = false)
            : this(name, exactName, (n, o) => SqlDistributedLock.CreateInternalLock(n, connectionString, o))
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
        public SqlDistributedReaderWriterLock(string name, IDbConnection connection, bool exactName = false)
            : this(name, exactName, (n, o) => SqlDistributedLock.CreateInternalLock(n, connection))
        {
        }

        /// <summary>
        /// Constructs a new lock using the provided <paramref name="name"/>.
        /// 
        /// The provided <paramref name="transaction"/> will be used to connect to the database and will provide lock scope. It is assumed to be externally managed and
        /// will not be committed or rolled back.
        /// 
        /// Unless <paramref name="exactName"/> is specified, <paramref name="name"/> will be escaped/hashed to ensure name validity.
        /// </summary>
        public SqlDistributedReaderWriterLock(string name, IDbTransaction transaction, bool exactName = false)
            : this(name, exactName, (n, o) => SqlDistributedLock.CreateInternalLock(n, transaction))
        {
        }

        private SqlDistributedReaderWriterLock(string name, bool exactName, Func<string, SqlDistributedLockOptions, IDbDistributedLock> internalLockFactory, Action<SqlConnectionOptionsBuilder>? optionsBuilder = null)
        {
            if (exactName)
            {
                if (name == null) { throw new ArgumentNullException(nameof(name)); }
                if (name.Length > MaxNameLength) { throw new FormatException($"{nameof(name)}: must be at most {MaxNameLength} characters"); }
                this.Name = name;
            }
            else
            {
                this.Name = GetSafeName(name);
            }

            this._options = SqlConnectionOptionsBuilder.GetOptions(optionsBuilder);
            this._internalLock = internalLockFactory(this.Name, this._options);
        }
        #endregion

        /// <summary>
        /// Implements <see cref="IDistributedLock.Name"/>
        /// </summary>
        public string Name { get; }

        /// <summary>
        /// The maximum allowed length for lock names. See https://msdn.microsoft.com/en-us/library/ms189823.aspx
        /// </summary>
        internal static int MaxNameLength => SqlDistributedLock.MaxNameLength;

        internal static string GetSafeName(string name) => SqlDistributedLock.GetSafeName(name);

        ValueTask<SqlDistributedReaderWriterLockUpgradeableHandle?> IInternalDistributedUpgradeableReaderWriterLock<SqlDistributedReaderWriterLockHandle, SqlDistributedReaderWriterLockUpgradeableHandle>.InternalTryAcquireUpgradeableReadLockAsync(
            TimeoutValue timeout,
            CancellationToken cancellationToken) =>
            this._internalLock.TryAcquireAsync(timeout, SqlApplicationLock.UpdateLock, cancellationToken, contextHandle: null)
                .Instrument(this._options.UseInstrumentation, @lock: this, Instrumentation.ReaderWriterLockLevel.UpgradeableRead, timeout, cancellationToken)
                .Wrap(h => new SqlDistributedReaderWriterLockUpgradeableHandle(h, this._internalLock, this, this._options.UseInstrumentation));

        ValueTask<SqlDistributedReaderWriterLockHandle?> IInternalDistributedReaderWriterLock<SqlDistributedReaderWriterLockHandle>.InternalTryAcquireAsync(
            TimeoutValue timeout,
            CancellationToken cancellationToken,
            bool isWrite) =>
            this._internalLock.TryAcquireAsync(timeout, isWrite ? SqlApplicationLock.ExclusiveLock : SqlApplicationLock.SharedLock, cancellationToken, contextHandle: null)
                .Instrument(this._options.UseInstrumentation, @lock: this, isWrite ? Instrumentation.ReaderWriterLockLevel.Write : Instrumentation.ReaderWriterLockLevel.Read, timeout, cancellationToken)
                .Wrap<SqlDistributedReaderWriterLockHandle>(h => new SqlDistributedReaderWriterLockNonUpgradeableHandle(h));
    }
}
