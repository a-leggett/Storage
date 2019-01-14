using System;
using System.IO;

namespace Storage.Data
{
    /// <summary>
    /// <see cref="IOException"/> that indicates corrupt data.
    /// </summary>
    public class CorruptDataException : IOException
    {
        /// <summary>
        /// <see cref="CorruptDataException"/> constructor.
        /// </summary>
        /// <param name="message">Descriptive message of the error.</param>
        public CorruptDataException(string message) : base(message) { }

        /// <summary>
        /// <see cref="CorruptDataException"/> constructor.
        /// </summary>
        /// <param name="innerException">The <see cref="Exception"/> that caused this
        /// <see cref="CorruptDataException"/> to be thrown, if any.</param>
        /// <param name="message">Descriptive message of the error.</param>
        public CorruptDataException(Exception innerException, string message) : base(message, innerException) { }
    }
}
