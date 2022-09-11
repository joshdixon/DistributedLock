using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.Metrics;
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
    static class Instrumentation
    {
        private static bool _isConfigured = false;

        private static Meter? _meter;

        private static Counter<long>? _locksActive;
        private static Counter<long>? _acquireTotal;
        private static Counter<long>? _errorsTotal;
        private static Histogram<double>? _acquireDuration;
        private static Histogram<double>? _heldDuration;

        private const string NameTagLabel = "distributedlock.name";
        private const string TypeTagLabel = "distributedlock.type";
        private const string TimeoutTagLabel = "distributedlock.timeout";
        private const string MaxCountLabel = "distributedlock.maxcount";
        private const string LevelLabel = "distributedlock.level";
        private const string ResultLabel = "distributedlock.result";

        private const string SuccessResultTagValue = "success";
        private const string TimeoutResultTagValue = "timeout";
        private const string CanceledResultTagValue = "canceled";
        private const string FaultedResultTagValue = "faulted";

        public static string? TryConfigure()
        {
            if (_isConfigured) { return null; }

            _meter = new Meter("DistributedLock");

            _locksActive = _meter.CreateCounter<long>("distributedlock.active");
            _acquireTotal = _meter.CreateCounter<long>("distributedlock.acquire");
            _errorsTotal = _meter.CreateCounter<long>("distributedlock.errors");
            _acquireDuration = _meter.CreateHistogram<double>("distributedlock.acquire_time_millis");
            _heldDuration = _meter.CreateHistogram<double>("distributedlock.held_time_millis");

            _isConfigured = true;
            return _meter.Name;
        }

        public static ValueTask<IDistributedSynchronizationHandle?> Instrument(this ValueTask<IDistributedSynchronizationHandle?> handleTask, bool instrument, IDistributedLock @lock, TimeoutValue timeout, CancellationToken cancellationToken) =>
            Instrument(handleTask, instrument, @lock.Name, @lock.GetType().Name, timeout, cancellationToken);

        public static ValueTask<IDistributedSynchronizationHandle?> Instrument(this ValueTask<IDistributedSynchronizationHandle?> handleTask, bool instrument, IDistributedReaderWriterLock @lock, ReaderWriterLockLevel level, TimeoutValue timeout, CancellationToken cancellationToken) =>
            Instrument(handleTask, instrument, @lock.Name, @lock.GetType().Name, timeout, cancellationToken);

        public static ValueTask<IDistributedSynchronizationHandle?> Instrument<THandle>(this ValueTask<IDistributedSynchronizationHandle?> handleTask, bool instrument, IInternalDistributedSemaphore<THandle> semaphore, TimeoutValue timeout, CancellationToken cancellationToken) where THandle : class, IDistributedSynchronizationHandle =>
            Instrument(handleTask, instrument, semaphore.Name, semaphore.GetType().Name, timeout, cancellationToken, semaphore.MaxCount);

        private static async ValueTask<IDistributedSynchronizationHandle?> Instrument(ValueTask<IDistributedSynchronizationHandle?> handleTask, bool instrument, string name, string type, TimeoutValue timeout, CancellationToken cancellationToken, int? maxCount = null, ReaderWriterLockLevel? level = null)
        {
            if (!instrument) { return await handleTask; }

            IDistributedSynchronizationHandle? handle = null;
            Exception? exception = null;
            Stopwatch sw = Stopwatch.StartNew();
            double elapsedMillis = 0;

            try
            {
                handle = await handleTask.ConfigureAwait(false);
                elapsedMillis = sw.ElapsedMilliseconds;
            }
            catch (Exception ex)
            {
                exception = ex;
                elapsedMillis = sw.ElapsedMilliseconds;
                throw;
            }
            finally
            {
                MeasureAcquireLock(handle, name, type, timeout, cancellationToken, elapsedMillis, exception, maxCount, level);
            }

            if (handle == null) { return null; };

            return new InternalDistributedInstrumentedLockHandle(handle, TrackActiveHandle(name, type, maxCount, level));
        }

        private static void MeasureAcquireLock(IDistributedSynchronizationHandle? handle,
            string name,
            string type,
            TimeoutValue timeout,
            CancellationToken cancellationToken,
            double elapsedMillis,
            Exception? exception,
            int? maxCount,
            ReaderWriterLockLevel? level)
        {
            if (!_isConfigured) { return; }

            string result;
            if (exception == null)
            {
                result = handle == null ? TimeoutResultTagValue : SuccessResultTagValue;
            }
            else
            {
                result = exception is OperationCanceledException && cancellationToken.IsCancellationRequested ? CanceledResultTagValue : FaultedResultTagValue;
            }

            var tagList = new TagList()
            {
                { NameTagLabel, name },
                { TypeTagLabel, type },
                { TimeoutTagLabel, timeout.InMilliseconds },
                { ResultLabel, result },
            };

            if (maxCount.HasValue) { tagList.Add(MaxCountLabel, maxCount.Value); }
            if (level.HasValue) { tagList.Add(MaxCountLabel, level.Value); }

            _acquireDuration?.Record(elapsedMillis, tagList);
            _acquireTotal?.Add(1, tagList);

            if (result == FaultedResultTagValue) { _errorsTotal?.Add(1, tagList); }
        }

        private static IDisposable? TrackActiveHandle(string name, string type, int? maxCount, ReaderWriterLockLevel? level)
        {
            if (!_isConfigured) { return null; }

            var tagList = new TagList()
            {
                { NameTagLabel, name },
                { TypeTagLabel, type },
            };

            if (maxCount.HasValue) { tagList.Add(MaxCountLabel, maxCount.Value); }
            if (level.HasValue) { tagList.Add(MaxCountLabel, level.Value); }


            return TrackActive(_locksActive, _heldDuration, tagList);
        }

        private static IDisposable TrackActive(Counter<long>? counter, Histogram<double>? duration, TagList tagList)
        {
            counter?.Add(1, tagList);

            return new ActiveTracker(counter, duration, tagList);
        }

        private class ActiveTracker : IDisposable
        {
            private readonly Counter<long>? _counter;
            private readonly Histogram<double>? _duration;
            private readonly TagList _tagList;
            private readonly Stopwatch _stopwatch;

            public ActiveTracker(Counter<long>? counter, Histogram<double>? duration, TagList tagList)
            {
                _counter = counter;
                _duration = duration;
                _tagList = tagList;
                _stopwatch = Stopwatch.StartNew();
            }

            public void Dispose()
            {
                _duration?.Record(_stopwatch.Elapsed.TotalMilliseconds, _tagList);
                _counter?.Add(-1, _tagList);
            }
        }

        public enum ReaderWriterLockLevel
        {
            Read,
            UpgradeableRead,
            Upgrade,
            Write,
        }
    }
}
