using Microsoft.VisualStudio.TestTools.UnitTesting;
using Storage.Data;
using StorageTest.Mocks;
using System;
using System.IO;
using System.Text;

namespace StorageTest
{
    [TestClass]
    public class BinaryReaderExTest
    {
        [TestMethod]
        public void ConstructorThrowsWhenArgumentIsNull()
        {
            Assert.ThrowsException<ArgumentNullException>(() => {
                var reader = new BinaryReaderEx(null, true, true);
            });

            Assert.ThrowsException<ArgumentNullException>(() => {
                var reader = new BinaryReaderEx(null, Encoding.UTF8, true, true);
            });

            Assert.ThrowsException<ArgumentNullException>(() => {
                using (MemoryStream ms = new MemoryStream())
                {
                    var reader = new BinaryReaderEx(ms, null, true, true);
                }
            });
        }

        [TestMethod]
        public void DisposeWillLeaveBaseStreamOpenIfRequested()
        {
            using (MemoryStream ms = new MemoryStream())
            {
                bool canDisposeNow = false;
                using (MockStream stream = new MockStream(ms, true, true, true))
                {
                    stream.OnDispose += (x)=> {
                        Assert.IsTrue(canDisposeNow);
                    };
                    using (BinaryReaderEx reader = new BinaryReaderEx(stream, true, true))
                    {
                        //Do nothing
                    }

                    canDisposeNow = true;
                }
            }
        }

        [TestMethod]
        public void DisposeWillDisposeBaseStreamIfNotRequestedToLeaveOpen()
        {
            using (MemoryStream ms = new MemoryStream())
            {
                bool didDispose = false;
                MockStream stream = new MockStream(ms, true, true, true);
                stream.OnDispose += (x) => { didDispose = true; };
                using (BinaryReaderEx reader = new BinaryReaderEx(stream, false, true))
                {
                    //Do nothing
                }

                //Ensure that the base Stream was disposed
                Assert.IsTrue(didDispose);
            }
        }

        [DataTestMethod]
        [DataRow((byte)0x00, false)]
        [DataRow((byte)0x01, true)]
        [DataRow((byte)0x02, true)]
        [DataRow((byte)0xFF, true)]
        public void ReadBooleanWorks(byte byteValue, bool result)
        {
            byte[] buffer = new byte[1];
            buffer[0] = byteValue;
            using (MemoryStream ms = new MemoryStream(buffer))
            {
                using (BinaryReaderEx reader = new BinaryReaderEx(ms, true, true))
                {
                    Assert.AreEqual(result, reader.ReadBoolean());
                }
            }
        }

        [DataTestMethod]
        [DataRow((Int16)0)]
        [DataRow((Int16)1)]
        [DataRow((Int16)2)]
        [DataRow((Int16)126)]
        [DataRow((Int16)127)]
        [DataRow((Int16)128)]
        [DataRow((Int16)(-126))]
        [DataRow((Int16)(-127))]
        [DataRow((Int16)(-128))]
        [DataRow((Int16)254)]
        [DataRow((Int16)255)]
        [DataRow((Int16)256)]
        [DataRow((Int16)257)]
        [DataRow((Int16)Int16.MinValue)]
        [DataRow((Int16)Int16.MaxValue)]
        [DataRow((Int16)(Int16.MinValue / 2))]
        [DataRow((Int16)(Int16.MaxValue / 2))]
        [DataRow((Int16)(Int16.MinValue + 1))]
        [DataRow((Int16)(Int16.MaxValue - 1))]
        public void ReadInt16Works(Int16 value)
        {
            for (int i = 0; i < 2; i++)
            {
                bool littleEndian = i != 0;

                byte[] buffer = Binary.GetInt16Bytes(value, littleEndian);
                using (MemoryStream ms = new MemoryStream(buffer))
                {
                    using (BinaryReaderEx reader = new BinaryReaderEx(ms, true, littleEndian))
                    {
                        Assert.AreEqual(value, reader.ReadInt16());
                    }
                }
            }
        }

        [DataTestMethod]
        [DataRow((UInt16)0)]
        [DataRow((UInt16)1)]
        [DataRow((UInt16)2)]
        [DataRow((UInt16)126)]
        [DataRow((UInt16)127)]
        [DataRow((UInt16)128)]
        [DataRow((UInt16)254)]
        [DataRow((UInt16)255)]
        [DataRow((UInt16)256)]
        [DataRow((UInt16)257)]
        [DataRow((UInt16)(UInt16.MaxValue))]
        [DataRow((UInt16)(UInt16.MaxValue / 2))]
        [DataRow((UInt16)(UInt16.MinValue + 1))]
        [DataRow((UInt16)(UInt16.MaxValue - 1))]
        public void ReadUInt16Works(UInt16 value)
        {
            for (int i = 0; i < 2; i++)
            {
                bool littleEndian = i != 0;

                byte[] buffer = Binary.GetUInt16Bytes(value, littleEndian);
                using (MemoryStream ms = new MemoryStream(buffer))
                {
                    using (BinaryReaderEx reader = new BinaryReaderEx(ms, true, littleEndian))
                    {
                        Assert.AreEqual(value, reader.ReadUInt16());
                    }
                }
            }
        }

        [DataTestMethod]
        [DataRow((Int32)0)]
        [DataRow((Int32)1)]
        [DataRow((Int32)2)]
        [DataRow((Int32)126)]
        [DataRow((Int32)127)]
        [DataRow((Int32)128)]
        [DataRow((Int32)(-126))]
        [DataRow((Int32)(-127))]
        [DataRow((Int32)(-128))]
        [DataRow((Int32)254)]
        [DataRow((Int32)255)]
        [DataRow((Int32)256)]
        [DataRow((Int32)257)]
        [DataRow((Int32)Int32.MinValue)]
        [DataRow((Int32)Int32.MaxValue)]
        [DataRow((Int32)(Int32.MinValue / 2))]
        [DataRow((Int32)(Int32.MaxValue / 2))]
        [DataRow((Int32)(Int32.MinValue + 1))]
        [DataRow((Int32)(Int32.MaxValue - 1))]
        public void ReadInt32Works(Int32 value)
        {
            for (int i = 0; i < 2; i++)
            {
                bool littleEndian = i != 0;

                byte[] buffer = Binary.GetInt32Bytes(value, littleEndian);
                using (MemoryStream ms = new MemoryStream(buffer))
                {
                    using (BinaryReaderEx reader = new BinaryReaderEx(ms, true, littleEndian))
                    {
                        Assert.AreEqual(value, reader.ReadInt32());
                    }
                }
            }
        }

        [DataTestMethod]
        [DataRow((UInt32)0)]
        [DataRow((UInt32)1)]
        [DataRow((UInt32)2)]
        [DataRow((UInt32)126)]
        [DataRow((UInt32)127)]
        [DataRow((UInt32)128)]
        [DataRow((UInt32)254)]
        [DataRow((UInt32)255)]
        [DataRow((UInt32)256)]
        [DataRow((UInt32)257)]
        [DataRow((UInt32)(UInt32.MaxValue))]
        [DataRow((UInt32)(UInt32.MaxValue / 2))]
        [DataRow((UInt32)(UInt32.MinValue + 1))]
        [DataRow((UInt32)(UInt32.MaxValue - 1))]
        public void ReadUInt32Works(UInt32 value)
        {
            for (int i = 0; i < 2; i++)
            {
                bool littleEndian = i != 0;

                byte[] buffer = Binary.GetUInt32Bytes(value, littleEndian);
                using (MemoryStream ms = new MemoryStream(buffer))
                {
                    using (BinaryReaderEx reader = new BinaryReaderEx(ms, true, littleEndian))
                    {
                        Assert.AreEqual(value, reader.ReadUInt32());
                    }
                }
            }
        }

        [DataTestMethod]
        [DataRow((Int64)0)]
        [DataRow((Int64)1)]
        [DataRow((Int64)2)]
        [DataRow((Int64)126)]
        [DataRow((Int64)127)]
        [DataRow((Int64)128)]
        [DataRow((Int64)(-126))]
        [DataRow((Int64)(-127))]
        [DataRow((Int64)(-128))]
        [DataRow((Int64)254)]
        [DataRow((Int64)255)]
        [DataRow((Int64)256)]
        [DataRow((Int64)257)]
        [DataRow((Int64)Int64.MinValue)]
        [DataRow((Int64)Int64.MaxValue)]
        [DataRow((Int64)(Int64.MinValue / 2))]
        [DataRow((Int64)(Int64.MaxValue / 2))]
        [DataRow((Int64)(Int64.MinValue + 1))]
        [DataRow((Int64)(Int64.MaxValue - 1))]
        public void ReadInt64Works(Int64 value)
        {
            for (int i = 0; i < 2; i++)
            {
                bool littleEndian = i != 0;

                byte[] buffer = Binary.GetInt64Bytes(value, littleEndian);
                using (MemoryStream ms = new MemoryStream(buffer))
                {
                    using (BinaryReaderEx reader = new BinaryReaderEx(ms, true, littleEndian))
                    {
                        Assert.AreEqual(value, reader.ReadInt64());
                    }
                }
            }
        }

        [DataTestMethod]
        [DataRow((UInt64)0)]
        [DataRow((UInt64)1)]
        [DataRow((UInt64)2)]
        [DataRow((UInt64)126)]
        [DataRow((UInt64)127)]
        [DataRow((UInt64)128)]
        [DataRow((UInt64)254)]
        [DataRow((UInt64)255)]
        [DataRow((UInt64)256)]
        [DataRow((UInt64)257)]
        [DataRow((UInt64)(UInt64.MaxValue))]
        [DataRow((UInt64)(UInt64.MaxValue / 2))]
        [DataRow((UInt64)(UInt64.MinValue + 1))]
        [DataRow((UInt64)(UInt64.MaxValue - 1))]
        public void ReadUInt64Works(UInt64 value)
        {
            for (int i = 0; i < 2; i++)
            {
                bool littleEndian = i != 0;

                byte[] buffer = Binary.GetUInt64Bytes(value, littleEndian);
                using (MemoryStream ms = new MemoryStream(buffer))
                {
                    using (BinaryReaderEx reader = new BinaryReaderEx(ms, true, littleEndian))
                    {
                        Assert.AreEqual(value, reader.ReadUInt64());
                    }
                }
            }
        }

        [DataTestMethod]
        [DataRow((Single)0)]
        [DataRow((Single)0.12f)]
        [DataRow((Single)1)]
        [DataRow((Single)2)]
        [DataRow((Single)126)]
        [DataRow((Single)127)]
        [DataRow((Single)128)]
        [DataRow((Single)(-126))]
        [DataRow((Single)(-127))]
        [DataRow((Single)(-128))]
        [DataRow((Single)254)]
        [DataRow((Single)255)]
        [DataRow((Single)256)]
        [DataRow((Single)257)]
        [DataRow(1234.56f)]
        [DataRow(-1234.56f)]
        [DataRow((Single)(Single.MinValue))]
        [DataRow((Single)(Single.MaxValue))]
        [DataRow((Single)(Single.MinValue / 2))]
        [DataRow((Single)(Single.MaxValue / 2))]
        [DataRow((Single)(Single.MinValue + 1))]
        [DataRow((Single)(Single.MaxValue - 1))]
        [DataRow((Single)(Single.NegativeInfinity))]
        [DataRow((Single)(Single.PositiveInfinity))]
        public void ReadSingleWorks(Single value)
        {
            for (int i = 0; i < 2; i++)
            {
                bool littleEndian = i != 0;

                byte[] buffer = Binary.GetSingleBytes(value, littleEndian);
                using (MemoryStream ms = new MemoryStream(buffer))
                {
                    using (BinaryReaderEx reader = new BinaryReaderEx(ms, true, littleEndian))
                    {
                        Assert.AreEqual(value, reader.ReadSingle());
                    }
                }
            }
        }


        [DataTestMethod]
        [DataRow((Double)0)]
        [DataRow((Double)0.12)]
        [DataRow((Double)1)]
        [DataRow((Double)2)]
        [DataRow((Double)126)]
        [DataRow((Double)127)]
        [DataRow((Double)128)]
        [DataRow((Double)(-126))]
        [DataRow((Double)(-127))]
        [DataRow((Double)(-128))]
        [DataRow((Double)254)]
        [DataRow((Double)255)]
        [DataRow((Double)256)]
        [DataRow((Double)257)]
        [DataRow(1234.56)]
        [DataRow(-1234.56)]
        [DataRow((Double)(Double.MinValue))]
        [DataRow((Double)(Double.MaxValue))]
        [DataRow((Double)(Double.MinValue / 2))]
        [DataRow((Double)(Double.MaxValue / 2))]
        [DataRow((Double)(Double.MinValue + 1))]
        [DataRow((Double)(Double.MaxValue - 1))]
        [DataRow((Double)(Double.NegativeInfinity))]
        [DataRow((Double)(Double.PositiveInfinity))]
        public void ReadDoubleWorks(Double value)
        {
            for (int i = 0; i < 2; i++)
            {
                bool littleEndian = i != 0;

                byte[] buffer = Binary.GetDoubleBytes(value, littleEndian);
                using (MemoryStream ms = new MemoryStream(buffer))
                {
                    using (BinaryReaderEx reader = new BinaryReaderEx(ms, true, littleEndian))
                    {
                        Assert.AreEqual(value, reader.ReadDouble());
                    }
                }
            }
        }
        
        [DataTestMethod]
        [DataRow("")]
        [DataRow("H")]
        [DataRow("Hello World!")]
        [DataRow("猫")]
        [DataRow("Hello World! 猫")]
        public void ReadShortStringWorks(string value)
        {
            for (int i = 0; i < 2; i++)
            {
                bool littleEndian = i != 0;

                byte[] bytes = Encoding.UTF8.GetBytes(value);
                using (MemoryStream ms = new MemoryStream())
                {
                    ms.Write(Binary.GetUInt16Bytes((UInt16)bytes.Length, littleEndian), 0, sizeof(UInt16));
                    ms.Write(bytes, 0, bytes.Length);
                    ms.Position = 0;
                    using (BinaryReaderEx reader = new BinaryReaderEx(ms, true, littleEndian))
                    {
                        Assert.AreEqual(value, reader.ReadShortString());
                    }
                }
            }
        }
    }
}
