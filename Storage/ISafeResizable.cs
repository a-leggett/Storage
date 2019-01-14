using System;

namespace Storage
{
    /// <summary>
    /// Interface that represents something with payload that can be resized safely.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Resizing 'safely' does not mean that the payload can be resized to any value.
    /// Obviously this is likely impossible (a file cannot be resized to 2^64 bytes,
    /// for example). Instead, it means that the implementation is able to try
    /// to resize the payload, and if it fails, it ensures that the payload remains
    /// at its original size and its data remains unchanged.
    /// </para>
    /// <para>
    /// Note that the implementation may be in a read-only or fixed-size state,
    /// in which case this interface is practically useless. This may seem like a 
    /// contradiction: Why would an object inherit this interface if it is read-only? 
    /// The answer is that writability or resizability may depend on runtime arguments. 
    /// It is generally expected that the caller/application already knows whether the 
    /// contained payload is read-only or fixed-size, and would thus avoid using the
    /// members defined in this interface if so. If the application attempts to resize
    /// the payload of a read-only or fixed-size instance (via <see cref="TrySetSize(long)"/>), 
    /// then the implementation will throw an <see cref="InvalidOperationException"/>.
    /// </para>
    /// </remarks>
    public interface ISafeResizable
    {
        /// <summary>
        /// The approximate maximum payload size, measured in bytes, or null if unknown.
        /// </summary>
        /// <remarks>
        /// This property represents a best-guess of the maximum payload size, 
        /// if it is known. The actual maximum payload size may be slightly more or
        /// less than this amount, if the implementation is unable to determine
        /// a confident maximum. Generally, the implementation should round down
        /// if it lacks confidence. If the implementation is unable to determine
        /// a reasonable guess, then it may return null instead.
        /// </remarks>
        long? MaxSize { get; }

        /// <summary>
        /// Attempts to safely resize the payload.
        /// </summary>
        /// <param name="size">The desired payload size, measured in bytes.</param>
        /// <returns>True if the payload was successfully resized, otherwise false.</returns>
        /// <remarks>
        /// <para>
        /// The implementation will try to resize the payload to the specified
        /// <paramref name="size"/>. If the resize fails, then false will be returned and
        /// the implementation must ensure that the original payload size and content remains
        /// unchanged. If for some reason the implementation is unable to prevent data corruption
        /// after a failed resize, then it must throw an <see cref="Exception"/> instead of returning
        /// false.
        /// </para>
        /// <para>
        /// When shrinking, data will only be removed from the very end of the payload. When increasing
        /// the size, undefined data will only be added to the very end of the payload. The original
        /// portion of the payload must remain unchanged (unless an <see cref="Exception"/> is thrown,
        /// indicating potential data corruption).
        /// </para>
        /// <para>
        /// When false is returned, it means that nothing has changed - the payload size and content
        /// is exactly what it was before this method was called. When true is returned, it means that
        /// the remaining portion of the original payload is exactly what was stored before this method
        /// was called and that the new size is exactly what was requested by the <paramref name="size"/>
        /// argument.
        /// </para>
        /// <para>
        /// If <paramref name="size"/> is less than zero, an <see cref="ArgumentOutOfRangeException"/>
        /// will be thrown. There is <em>no</em> strict upper limit which will result in an
        /// <see cref="ArgumentOutOfRangeException"/> being thrown, not even
        /// <see cref="MaxSize"/> (since that is just a best guess).
        /// </para>
        /// <para>
        /// The implementation may throw an <see cref="InvalidOperationException"/> if the payload
        /// is read-only or fixed-size. This may seem like a contradiction: Why would an object
        /// inherit this interface if it is read-only? The answer is that writability or resizability
        /// may depend on runtime arguments. It is generally expected that the caller already
        /// knows whether the contained payload is read-only or fixed-size, and would thus avoid
        /// calling this method if so.
        /// </para>
        /// <para>
        /// If any <see cref="Exception"/> is thrown, except the <see cref="ArgumentOutOfRangeException"/>
        /// and <see cref="InvalidOperationException"/> described above, then the application
        /// should expect that the resize operation has failed, and that it may have corrupted 
        /// the payload.
        /// </para>
        /// </remarks>
        /// <exception cref="ArgumentOutOfRangeException">Thrown if <paramref name="size"/>
        /// is less than zero.</exception>
        /// <exception cref="InvalidOperationException">May be thrown by implementation if
        /// the payload is read-only or fixed-size.</exception>
        bool TrySetSize(long size);
    }
}
