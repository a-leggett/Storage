using Microsoft.VisualStudio.TestTools.UnitTesting;
using Storage.Data.Serializers;
using System;

namespace StorageTest
{
    [TestClass]
    public class Int64SerializerTest
    {
        [DataTestMethod]
        [DataRow(true)]
        [DataRow(false)]
        public void ConstructorWorks(bool littleEndian)
        {
            Int64Serializer serializer = new Int64Serializer(littleEndian);
            Assert.AreEqual(littleEndian, serializer.StoreAsLittleEndian);
        }

        [DataTestMethod]
        [DataRow(true)]
        [DataRow(false)]
        public void SerializeThenDeserializeWorks(bool littleEndian)
        {
            long[] numbers = new long[] {
                long.MinValue,
                long.MinValue + 1,
                long.MinValue + 2,
                long.MinValue + 3,
                long.MinValue / 2,
                -65537,
                -65536,
                -65535,
                -65534,
                -257,
                -256,
                -255,
                -254
                -126,
                -127,
                -128
                -5,
                -4,
                -3,
                -2,
                -1,
                0,
                long.MaxValue,
                long.MaxValue - 1,
                long.MaxValue - 2,
                long.MaxValue - 3,
                long.MaxValue / 2,
                65537,
                65536,
                65535,
                65534,
                257,
                256,
                255,
                254,
                126,
                127,
                128,
                5,
                4,
                3,
                2,
                1,
            };

            Int64Serializer serializer = new Int64Serializer(littleEndian);
            for(long i = 0; i < numbers.Length; i++)
            {
                byte[] buffer = new byte[serializer.DataSize];
                serializer.Serialize(numbers[i], buffer);
                Assert.AreEqual(numbers[i], serializer.Deserialize(buffer));
            }
        }

        [TestMethod]
        public void SerializeThrowsWhenBufferIsNull()
        {
            Int64Serializer serializer = new Int64Serializer();
            Assert.ThrowsException<ArgumentNullException>(() => {
                serializer.Serialize(0, null);
            });
        }

        [DataTestMethod]
        [DataRow(0)]
        [DataRow(sizeof(Int64) - 1)]
        [DataRow(sizeof(Int64) + 1)]
        public void SerializeThrowsWhenBufferIsWrongSize(long badSize)
        {
            Int64Serializer serializer = new Int64Serializer();
            Assert.ThrowsException<ArgumentException>(() => {
                serializer.Serialize(0, new byte[badSize]);
            });
        }

        [TestMethod]
        public void DeserializeThrowsWhenBufferIsNull()
        {
            Int64Serializer serializer = new Int64Serializer();
            Assert.ThrowsException<ArgumentNullException>(() => {
                serializer.Deserialize(null);
            });
        }

        [DataTestMethod]
        [DataRow(0)]
        [DataRow(sizeof(Int64) - 1)]
        [DataRow(sizeof(Int64) + 1)]
        public void DeserializeThrowsWhenBufferIsWrongSize(long badSize)
        {
            Int64Serializer serializer = new Int64Serializer();
            Assert.ThrowsException<ArgumentException>(() => {
                serializer.Deserialize(new byte[badSize]);
            });
        }
    }
}
