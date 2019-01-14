using Microsoft.VisualStudio.TestTools.UnitTesting;
using Storage.Data.Serializers;
using System;

namespace StorageTest
{
    [TestClass]
    public class Int32SerializerTest
    {
        [DataTestMethod]
        [DataRow(true)]
        [DataRow(false)]
        public void ConstructorWorks(bool littleEndian)
        {
            Int32Serializer serializer = new Int32Serializer(littleEndian);
            Assert.AreEqual(littleEndian, serializer.StoreAsLittleEndian);
        }

        [DataTestMethod]
        [DataRow(true)]
        [DataRow(false)]
        public void SerializeThenDeserializeWorks(bool littleEndian)
        {
            int[] numbers = new int[] {
                int.MinValue,
                int.MinValue + 1,
                int.MinValue + 2,
                int.MinValue + 3,
                int.MinValue / 2,
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
                int.MaxValue,
                int.MaxValue - 1,
                int.MaxValue - 2,
                int.MaxValue - 3,
                int.MaxValue / 2,
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

            Int32Serializer serializer = new Int32Serializer(littleEndian);
            for(int i = 0; i < numbers.Length; i++)
            {
                byte[] buffer = new byte[serializer.DataSize];
                serializer.Serialize(numbers[i], buffer);
                Assert.AreEqual(numbers[i], serializer.Deserialize(buffer));
            }
        }

        [TestMethod]
        public void SerializeThrowsWhenBufferIsNull()
        {
            Int32Serializer serializer = new Int32Serializer();
            Assert.ThrowsException<ArgumentNullException>(() => {
                serializer.Serialize(0, null);
            });
        }

        [DataTestMethod]
        [DataRow(0)]
        [DataRow(sizeof(Int32) - 1)]
        [DataRow(sizeof(Int32) + 1)]
        public void SerializeThrowsWhenBufferIsWrongSize(int badSize)
        {
            Int32Serializer serializer = new Int32Serializer();
            Assert.ThrowsException<ArgumentException>(() => {
                serializer.Serialize(0, new byte[badSize]);
            });
        }

        [TestMethod]
        public void DeserializeThrowsWhenBufferIsNull()
        {
            Int32Serializer serializer = new Int32Serializer();
            Assert.ThrowsException<ArgumentNullException>(() => {
                serializer.Deserialize(null);
            });
        }

        [DataTestMethod]
        [DataRow(0)]
        [DataRow(sizeof(Int32) - 1)]
        [DataRow(sizeof(Int32) + 1)]
        public void DeserializeThrowsWhenBufferIsWrongSize(int badSize)
        {
            Int32Serializer serializer = new Int32Serializer();
            Assert.ThrowsException<ArgumentException>(() => {
                serializer.Deserialize(new byte[badSize]);
            });
        }
    }
}
