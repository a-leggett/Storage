namespace Storage
{
    /// <summary>
    /// Structure that reports the current progress of an operation.
    /// </summary>
    public struct ProgressReport
    {
        /// <summary>
        /// The target value, or null if unknown.
        /// </summary>
        public long? Target { get; private set; }
        
        /// <summary>
        /// The current value.
        /// </summary>
        /// <remarks>
        /// This should always be at least zero, and at most <see cref="Target"/> (if it is known).
        /// </remarks>
        public long Current { get; private set; }

        /// <summary>
        /// <see cref="ProgressReport"/> constructor.
        /// </summary>
        /// <param name="current">The current value. See <see cref="Current"/>.</param>
        /// <param name="target">The target value, or null if unknown.</param>
        public ProgressReport(long current, long? target)
        {
            this.Target = target;
            this.Current = current;
        }
    }
}
