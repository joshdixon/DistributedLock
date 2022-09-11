using Medallion.Threading.Internal;
using Medallion.Threading.Internal.Data;
using System;
using System.Data;
using System.Threading;
using System.Threading.Tasks;

namespace Medallion.Threading.SqlServer
{
    /// <summary>
    /// Implements a distributed semaphore using SQL Server constructs.
    /// </summary>
    public sealed partial class SqlDistributedSemaphore : IInternalDistributedSemaphore<SqlDistributedSemaphoreHandle>
    {
        private readonly IDbDistributedLock _internalLock;
        private readonly SqlSemaphore _strategy;
        private readonly SqlDistributedLockOptions _options;

        #region ---- Constructors ----
        /// <summary>
        /// Creates a semaphore with name <paramref name="name"/> that can be acquired up to <paramref name="maxCount"/> 
        /// times concurrently. The provided <paramref name="connectionString"/> will be used to connect to the database.
        /// </summary>
        public SqlDistributedSemaphore(string name, int maxCount, string connectionString, Action<SqlConnectionOptionsBuilder>? options = null)
            : this(name, maxCount, (n, o) => SqlDistributedLock.CreateInternalLock(n, connectionString, o), options)
        {
        }

        /// <summary>
        /// Creates a semaphore with name <paramref name="name"/> that can be acquired up to <paramref name="maxCount"/> 
        /// times concurrently. When acquired, the semaphore will be scoped to the given <paramref name="connection"/>. 
        /// The <paramref name="connection"/> is assumed to be externally managed: the <see cref="SqlDistributedSemaphore"/> will 
        /// not attempt to open, close, or dispose it
        /// </summary>
        public SqlDistributedSemaphore(string name, int maxCount, IDbConnection connection)
            : this(name, maxCount, (n, o) => SqlDistributedLock.CreateInternalLock(n, connection))
        {
        }

        /// <summary>
        /// Creates a semaphore with name <paramref name="name"/> that can be acquired up to <paramref name="maxCount"/> 
        /// times concurrently. When acquired, the semaphore will be scoped to the given <paramref name="transaction"/>. 
        /// The <paramref name="transaction"/> and its <see cref="IDbTransaction.Connection"/> are assumed to be externally managed: 
        /// the <see cref="SqlDistributedSemaphore"/> will not attempt to open, close, commit, roll back, or dispose them
        /// </summary>
        public SqlDistributedSemaphore(string name, int maxCount, IDbTransaction transaction)
            : this(name, maxCount, (n, o) => SqlDistributedLock.CreateInternalLock(n, transaction))
        {
        }

        private SqlDistributedSemaphore(string name, int maxCount, Func<string, SqlDistributedLockOptions, IDbDistributedLock> internalLockFactory, Action<SqlConnectionOptionsBuilder>? optionsBuilder = null)
        {
            if (maxCount < 1) { throw new ArgumentOutOfRangeException(nameof(maxCount), maxCount, "must be positive"); }

            this.Name = name ?? throw new ArgumentNullException(nameof(name));
            this._strategy = new SqlSemaphore(maxCount);
            this._options = SqlConnectionOptionsBuilder.GetOptions(optionsBuilder);
            this._internalLock = internalLockFactory(SqlSemaphore.ToSafeName(name), this._options);
        }
        #endregion

        /// <summary>
        /// Implements <see cref="IDistributedSemaphore.Name"/>
        /// </summary>
        public string Name { get; }

        /// <summary>
        /// Implements <see cref="IDistributedSemaphore.MaxCount"/>
        /// </summary>
        public int MaxCount => this._strategy.MaxCount;

        ValueTask<SqlDistributedSemaphoreHandle?> IInternalDistributedSemaphore<SqlDistributedSemaphoreHandle>.InternalTryAcquireAsync(TimeoutValue timeout, CancellationToken cancellationToken) =>
            this._internalLock.TryAcquireAsync(timeout, this._strategy, cancellationToken, contextHandle: null)
                .Instrument(this._options.UseInstrumentation, semaphore: this, timeout, cancellationToken)
                .Wrap(h => new SqlDistributedSemaphoreHandle(h));
    }
}
