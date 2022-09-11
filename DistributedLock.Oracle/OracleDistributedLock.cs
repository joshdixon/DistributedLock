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
    /// Implements a distributed lock for Oracle databse based on the DBMS_LOCK package
    /// </summary>
    public sealed partial class OracleDistributedLock : IInternalDistributedLock<OracleDistributedLockHandle>
    {
        internal const int MaxNameLength = 128;

        private readonly IDbDistributedLock _internalLock;
        private readonly OracleDistributedLockOptions _options;

        /// <summary>
        /// Constructs a lock with the given <paramref name="name"/> that connects using the provided <paramref name="connectionString"/> and
        /// <paramref name="options"/>.
        /// 
        /// Unless <paramref name="exactName"/> is specified, <paramref name="name"/> will be escaped/hashed to ensure name validity.
        /// </summary>
        public OracleDistributedLock(string name, string connectionString, Action<OracleConnectionOptionsBuilder>? options = null, bool exactName = false)
            : this(name, exactName, (n, o) => CreateInternalLock(n, connectionString, o), options)
        {
        }

        /// <summary>
        /// Constructs a lock with the given <paramref name="name"/> that connects using the provided <paramref name="connection" />.
        /// 
        /// Unless <paramref name="exactName"/> is specified, <paramref name="name"/> will be escaped/hashed to ensure name validity.
        /// </summary>
        public OracleDistributedLock(string name, IDbConnection connection, bool exactName = false)
            : this(name, exactName, (n, o) => CreateInternalLock(n, connection))
        {
        }

        private OracleDistributedLock(string name, bool exactName, Func<string, OracleDistributedLockOptions, IDbDistributedLock> internalLockFactory, Action<OracleConnectionOptionsBuilder>? options = null)
        {
            this.Name = GetName(name, exactName);
            this._options = OracleConnectionOptionsBuilder.GetOptions(options);
            this._internalLock = internalLockFactory(this.Name, this._options);
        }

        internal static string GetName(string name, bool exactName)
        {
            if (name == null) { throw new ArgumentNullException(nameof(name)); }

            if (exactName)
            {
                if (name.Length > MaxNameLength) { throw new FormatException($"{nameof(name)}: must be at most {MaxNameLength} characters"); }
                // Oracle treats NULL as the empty string. See https://stackoverflow.com/questions/13278773/null-vs-empty-string-in-oracle
                if (name.Length == 0) { throw new FormatException($"{nameof(name)} must not be empty"); }
                return name;
            }

            return DistributedLockHelpers.ToSafeName(name, MaxNameLength, s => s.Length == 0 ? "EMPTY" : s);
        }

        /// <summary>
        /// Implements <see cref="IDistributedLock.Name"/>
        /// </summary>
        public string Name { get; }

        ValueTask<OracleDistributedLockHandle?> IInternalDistributedLock<OracleDistributedLockHandle>.InternalTryAcquireAsync(TimeoutValue timeout, CancellationToken cancellationToken) =>
            this._internalLock.TryAcquireAsync(timeout, OracleDbmsLock.ExclusiveLock, cancellationToken, contextHandle: null)
                .Instrument(this._options.UseInstrumentation, @lock: this, timeout, cancellationToken)
                .Wrap(h => new OracleDistributedLockHandle(h));

        internal static IDbDistributedLock CreateInternalLock(string name, string connectionString, OracleDistributedLockOptions options)
        {
            if (connectionString == null) { throw new ArgumentNullException(nameof(connectionString)); }

            if (options.UseMultiplexing)
            {
                return new OptimisticConnectionMultiplexingDbDistributedLock(name, connectionString, OracleMultiplexedConnectionLockPool.Instance, options.KeepaliveCadence);
            }

            return new DedicatedConnectionOrTransactionDbDistributedLock(name, () => new OracleDatabaseConnection(connectionString), useTransaction: false, options.KeepaliveCadence);
        }

        internal static IDbDistributedLock CreateInternalLock(string name, IDbConnection connection)
        {
            if (connection == null) { throw new ArgumentNullException(nameof(connection)); }

            return new DedicatedConnectionOrTransactionDbDistributedLock(name, () => new OracleDatabaseConnection(connection));
        }
    }
}
