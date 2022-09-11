using Medallion.Threading.Internal;
using OpenTelemetry.Metrics;
using System;
using System.Collections.Generic;
using System.Text;

namespace Medallion.Threading
{
    /// <summary>
    /// Extension methods to opt-in to metrics collection.
    /// </summary>
    public static class MeterProviderBuilderExtensions
    {
        /// <summary>
        /// Enable DistributedLock instrumentation.
        /// </summary>
        public static MeterProviderBuilder AddDistributedLockInstrumentation(this MeterProviderBuilder builder, string? serviceName = null)
        {
            string? meterName = Instrumentation.TryConfigure();
            if (meterName != null)
            {
                builder.AddMeter(meterName);
            }

            return builder;
        }
    }
}
