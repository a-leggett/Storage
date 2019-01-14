using System;

namespace Storage.Data.Serializers
{
    /// <summary>
    /// Interface that can serialize and deserialize types stored in fixed-length buffers.
    /// </summary>
    public interface ISerializer<T>
    {
        /// <summary>
        /// The size, in bytes, of a serialized <typeparamref name="T"/>.
        /// </summary>
        /// <remarks>
        /// This property must remain constant. The <see cref="Serialize(T, byte[])"/>
        /// and <see cref="Deserialize(byte[])"/> methods will be given buffers of this size.
        /// </remarks>
        long DataSize { get; }

        /// <summary>
        /// Writes the state of a <typeparamref name="T"/> to a buffer.
        /// </summary>
        /// <param name="value">The <typeparamref name="T"/> to serialize.</param>
        /// <param name="buffer">The buffer into which to write the serialized
        /// data. Must be exactly <see cref="DataSize"/> bytes long.</param>
        /// <exception cref="ArgumentException">Thrown if <paramref name="buffer"/>
        /// is not exactly <see cref="DataSize"/> bytes long.</exception>
        void Serialize(T value, byte[] buffer);

        /// <summary>
        /// Reads the state of a <typeparamref name="T"/> from a buffer.
        /// </summary>
        /// <param name="buffer">The buffer from which to read the serialized
        /// data. Must be exactly <see cref="DataSize"/> bytes long.</param>
        /// <exception cref="ArgumentException">Thrown if <paramref name="buffer"/>
        /// is not exactly <see cref="DataSize"/> bytes long.</exception>
        /// <exception cref="CorruptDataException">Thrown if the data stored in
        /// the <paramref name="buffer"/> is corrupt. Note that some implementations
        /// may throw other <see cref="Exception"/> types to indicate data corruption
        /// too, but <see cref="CorruptDataException"/> is the preferred type.</exception>
        /// <returns>The resulting <typeparamref name="T"/>.</returns>
        T Deserialize(byte[] buffer);
    }
}
