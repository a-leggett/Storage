using System;

namespace Storage.Data.Serializers
{
    /// <summary>
    /// <see cref="ISerializer{T}"/> for 64-bit integers.
    /// </summary>
    public sealed class Int32Serializer : ISerializer<Int32>
    {
        /// <summary>
        /// Will values be represented and interpreted as little-endian? If false,
        /// big-endian is assumed.
        /// </summary>
        public bool StoreAsLittleEndian { get; private set; }

        /// <summary>
        /// The size, in bytes, of the serialized data.
        /// </summary>
        public long DataSize { get { return sizeof(Int32); } }

        /// <summary>
        /// <see cref="Int32Serializer"/> constructor.
        /// </summary>
        /// <param name="useLittleEndian">Will values be represented and interpreted as little-endian? If false,
        /// big-endian is assumed.</param>
        public Int32Serializer(bool useLittleEndian = true)
        {
            this.StoreAsLittleEndian = useLittleEndian;
        }

        /// <summary>
        /// Deserializes a buffer to an <see cref="Int32"/>.
        /// </summary>
        /// <param name="buffer">The buffer.</param>
        /// <returns>The resulting <see cref="Int32"/>.</returns>
        /// <exception cref="ArgumentNullException">Thrown if <paramref name="buffer"/> is null.</exception>
        /// <exception cref="ArgumentException">Thrown if <paramref name="buffer"/> is not <see cref="DataSize"/>
        /// bytes long.</exception>
        public Int32 Deserialize(byte[] buffer)
        {
            if (buffer == null)
                throw new ArgumentNullException(nameof(buffer));
            if (buffer.Length != DataSize)
                throw new ArgumentException("Invalid buffer size", nameof(buffer));

            return Binary.ReadInt32(buffer, 0, StoreAsLittleEndian);
        }

        /// <summary>
        /// Serializes an <see cref="Int32"/> to a byte array.
        /// </summary>
        /// <param name="value">The value to serialize.</param>
        /// <param name="buffer">The buffer into which to write the serialized data.</param>
        /// <exception cref="ArgumentNullException">Thrown if <paramref name="buffer"/> is null.</exception>
        /// <exception cref="ArgumentException">Thrown if <paramref name="buffer"/> is not <see cref="DataSize"/>
        /// bytes long.</exception>
        public void Serialize(Int32 value, byte[] buffer)
        {
            if (buffer == null)
                throw new ArgumentNullException(nameof(buffer));
            if (buffer.Length != DataSize)
                throw new ArgumentException("Invalid buffer size", nameof(buffer));

            byte[] bytes = Binary.GetInt32Bytes(value, StoreAsLittleEndian);
            Array.Copy(bytes, buffer, bytes.Length);
        }
    }
}
