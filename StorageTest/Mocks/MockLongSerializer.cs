using Storage.Data;
using Storage.Data.Serializers;
using System;

namespace StorageTest.Mocks
{
    class MockLongSerializer : ISerializer<long>
    {
        public long DataSize { get { return sizeof(long); } }

        public const long CDataSize = sizeof(long);

        public long Deserialize(byte[] buffer)
        {
            if (buffer == null)
                throw new ArgumentNullException(nameof(buffer));
            if (buffer.Length != DataSize)
                throw new ArgumentException("Invalid buffer size.");

            return Binary.ReadInt64(buffer, 0, false);
        }

        public void Serialize(long value, byte[] buffer)
        {
            if (buffer == null)
                throw new ArgumentNullException(nameof(buffer));
            if (buffer.Length != DataSize)
                throw new ArgumentException("Invalid buffer size.");
            
            byte[] bytes = Binary.GetInt64Bytes(value, false);
            Array.Copy(bytes, 0, buffer, 0, bytes.Length);
        }
    }
}
