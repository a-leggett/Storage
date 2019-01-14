using Storage.Data;
using Storage.Data.Serializers;
using System;
using System.Text;

namespace StorageTest.Mocks
{
    class MockStringSerializer : ISerializer<string>
    {
        public long DataSize { get { return CDataSize; } }

        public const long CDataSize = 32;

        public string Deserialize(byte[] buffer)
        {
            if (buffer == null)
                throw new ArgumentNullException(nameof(buffer));
            if (buffer.Length != DataSize)
                throw new ArgumentException("Invalid buffer size.");

            string got = Encoding.ASCII.GetString(buffer);
            return got.TrimEnd('\0');//Remove null terminators
        }

        public void Serialize(string value, byte[] buffer)
        {
            if (buffer == null)
                throw new ArgumentNullException(nameof(buffer));
            if (buffer.Length != DataSize)
                throw new ArgumentException("Invalid buffer size.");
            Array.Clear(buffer, 0, buffer.Length);

            byte[] bytes = Encoding.ASCII.GetBytes(value);
            if (bytes.Length > DataSize)
                throw new ArgumentException("The value is too large.");
            Array.Copy(bytes, 0, buffer, 0, bytes.Length);
        }
    }
}
