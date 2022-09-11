using System;
using System.Collections.Generic;
using System.Text;

namespace Medallion.Threading.Internal
{
#if DEBUG
    public
#else
    internal
#endif
    interface IInternalInstrumentationOptionsBuilder<T>
    {
        /// <summary>
        /// Configure whether Instrumentation should be enabled. This is disabled by default.
        /// </summary>
        T UseInstrumentation(bool useInstrumentation = true);
    }
}
