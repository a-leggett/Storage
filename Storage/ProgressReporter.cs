using System;

namespace Storage
{
    /// <summary>
    /// Delegated <see cref="IProgress{T}"/>.
    /// </summary>
    public sealed class ProgressReporter : IProgress<ProgressReport>
    {
        /// <summary>
        /// Delegate that handles a <see cref="ProgressReport"/>.
        /// </summary>
        /// <param name="report">The <see cref="ProgressReport"/> that describes the current progress.</param>
        public delegate void ReportHandler(ProgressReport report);

        private ReportHandler reportHandler;

        /// <summary>
        /// <see cref="ProgressReporter"/> constructor.
        /// </summary>
        /// <param name="reportHandler">The <see cref="ReportHandler"/> that will handle the <see cref="ProgressReport"/>s.</param>
        /// <exception cref="ArgumentNullException">Thrown if <paramref name="reportHandler"/> is null.</exception>
        public ProgressReporter(ReportHandler reportHandler)
        {
            this.reportHandler = reportHandler ?? throw new ArgumentNullException(nameof(reportHandler));
        }

        /// <summary>
        /// Reports the current progress.
        /// </summary>
        /// <param name="value">The <see cref="ProgressReport"/>.</param>
        public void Report(ProgressReport value)
        {
            this.reportHandler(value);
        }
    }
}
