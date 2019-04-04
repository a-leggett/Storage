using Microsoft.VisualStudio.TestTools.UnitTesting;
using Storage.Data;
using StorageTest.Mocks;
using System;
using System.IO;
using System.Text;

namespace StorageTest
{
    [TestClass]
    public class BinaryWriterExTest
    {
        [TestMethod]
        public void ConstructorThrowsWhenArgumentIsNull()
        {
            Assert.ThrowsException<ArgumentNullException>(() => {
                var reader = new BinaryWriterEx(null, true, true);
            });

            Assert.ThrowsException<ArgumentNullException>(() => {
                var reader = new BinaryWriterEx(null, Encoding.UTF8, true, true);
            });

            Assert.ThrowsException<ArgumentNullException>(() => {
                using (MemoryStream ms = new MemoryStream())
                {
                    var reader = new BinaryWriterEx(ms, null, true, true);
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
                    stream.OnDispose += (x) => {
                        Assert.IsTrue(canDisposeNow);
                    };
                    using (BinaryWriterEx reader = new BinaryWriterEx(stream, true, true))
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
                using (BinaryWriterEx writer = new BinaryWriterEx(stream, false, true))
                {
                    //Do nothing
                }

                //Ensure that the base Stream was disposed
                Assert.IsTrue(didDispose);
            }
        }

        [DataTestMethod]
        [DataRow(true)]
        [DataRow(false)]
        public void WriteBooleanWorks(bool value)
        {
            for (int i = 0; i < 2; i++)
            {
                bool littleEndian = i != 0;

                using (MemoryStream ms = new MemoryStream())
                {
                    using (BinaryWriterEx writer = new BinaryWriterEx(ms, true, littleEndian))
                    {
                        writer.WriteBoolean(value);
                        Assert.AreEqual(sizeof(Boolean), ms.Length);
                        ms.Position = 0;
                        Assert.AreEqual(value, ms.ToArray()[0] != 0x00);
                    }
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
        public void WriteInt16Works(Int16 value)
        {
            for (int i = 0; i < 2; i++)
            {
                bool littleEndian = i != 0;

                using (MemoryStream ms = new MemoryStream())
                {
                    using (BinaryWriterEx writer = new BinaryWriterEx(ms, true, littleEndian))
                    {
                        writer.WriteInt16(value);
                        Assert.AreEqual(sizeof(Int16), ms.Length);
                        ms.Position = 0;
                        CollectionAssert.AreEqual(ms.ToArray(), Binary.GetInt16Bytes(value, littleEndian));
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
        public void WriteUInt16Works(UInt16 value)
        {
            for (int i = 0; i < 2; i++)
            {
                bool littleEndian = i != 0;

                using (MemoryStream ms = new MemoryStream())
                {
                    using (BinaryWriterEx writer = new BinaryWriterEx(ms, true, littleEndian))
                    {
                        writer.WriteUInt16(value);
                        Assert.AreEqual(sizeof(UInt16), ms.Length);
                        ms.Position = 0;
                        CollectionAssert.AreEqual(ms.ToArray(), Binary.GetUInt16Bytes(value, littleEndian));
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
        public void WriteInt32Works(Int32 value)
        {
            for (int i = 0; i < 2; i++)
            {
                bool littleEndian = i != 0;

                using (MemoryStream ms = new MemoryStream())
                {
                    using (BinaryWriterEx writer = new BinaryWriterEx(ms, true, littleEndian))
                    {
                        writer.WriteInt32(value);
                        Assert.AreEqual(sizeof(Int32), ms.Length);
                        ms.Position = 0;
                        CollectionAssert.AreEqual(ms.ToArray(), Binary.GetInt32Bytes(value, littleEndian));
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
        public void WriteUInt32Works(UInt32 value)
        {
            for (int i = 0; i < 2; i++)
            {
                bool littleEndian = i != 0;

                using (MemoryStream ms = new MemoryStream())
                {
                    using (BinaryWriterEx writer = new BinaryWriterEx(ms, true, littleEndian))
                    {
                        writer.WriteUInt32(value);
                        Assert.AreEqual(sizeof(UInt32), ms.Length);
                        ms.Position = 0;
                        CollectionAssert.AreEqual(ms.ToArray(), Binary.GetUInt32Bytes(value, littleEndian));
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
        public void WriteInt64Works(Int64 value)
        {
            for (int i = 0; i < 2; i++)
            {
                bool littleEndian = i != 0;

                using (MemoryStream ms = new MemoryStream())
                {
                    using (BinaryWriterEx writer = new BinaryWriterEx(ms, true, littleEndian))
                    {
                        writer.WriteInt64(value);
                        Assert.AreEqual(sizeof(Int64), ms.Length);
                        ms.Position = 0;
                        CollectionAssert.AreEqual(ms.ToArray(), Binary.GetInt64Bytes(value, littleEndian));
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
        public void WriteUInt64Works(UInt64 value)
        {
            for (int i = 0; i < 2; i++)
            {
                bool littleEndian = i != 0;

                using (MemoryStream ms = new MemoryStream())
                {
                    using (BinaryWriterEx writer = new BinaryWriterEx(ms, true, littleEndian))
                    {
                        writer.WriteUInt64(value);
                        Assert.AreEqual(sizeof(UInt64), ms.Length);
                        ms.Position = 0;
                        CollectionAssert.AreEqual(ms.ToArray(), Binary.GetUInt64Bytes(value, littleEndian));
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
        public void WriteSingleWorks(Single value)
        {
            for (int i = 0; i < 2; i++)
            {
                bool littleEndian = i != 0;

                using (MemoryStream ms = new MemoryStream())
                {
                    using (BinaryWriterEx writer = new BinaryWriterEx(ms, true, littleEndian))
                    {
                        writer.WriteSingle(value);
                        Assert.AreEqual(sizeof(Single), ms.Length);
                        ms.Position = 0;
                        CollectionAssert.AreEqual(ms.ToArray(), Binary.GetSingleBytes(value, littleEndian));
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
        public void WriteDoubleWorks(Double value)
        {
            for (int i = 0; i < 2; i++)
            {
                bool littleEndian = i != 0;

                using (MemoryStream ms = new MemoryStream())
                {
                    using (BinaryWriterEx writer = new BinaryWriterEx(ms, true, littleEndian))
                    {
                        writer.WriteDouble(value);
                        Assert.AreEqual(sizeof(Double), ms.Length);
                        ms.Position = 0;
                        CollectionAssert.AreEqual(ms.ToArray(), Binary.GetDoubleBytes(value, littleEndian));
                    }
                }
            }
        }

        [TestMethod]
        public void WriteShortStringThrowsWhenArgIsNull()
        {
            using (MemoryStream ms = new MemoryStream())
            {
                using (BinaryWriterEx writer = new BinaryWriterEx(ms, true, true))
                {
                    Assert.ThrowsException<ArgumentNullException>(() => {
                        writer.WriteShortString(null);
                    });

                    //Make sure nothing was written to the stream
                    Assert.AreEqual(0, ms.Length);
                }
            }

        }

        [TestMethod]
        public void WriteShortStringThrowsWhenThereAreTooManyBytes()
        {
            string tooLong = "";
            Char append = '猫';//Takes 3 bytes in UTF-8
            for (int i = 0; i < (UInt16.MaxValue / 3) + 1; i++)
                tooLong += append;

            using (MemoryStream ms = new MemoryStream())
            {
                using (BinaryWriterEx writer = new BinaryWriterEx(ms, true, true))
                {
                    Assert.ThrowsException<ArgumentException>(() => {
                        writer.WriteShortString(tooLong);
                    });

                    //Make sure nothing was written to the stream
                    Assert.AreEqual(0, ms.Length);
                }
            }
        }
        
        [DataTestMethod]
        [DataRow("")]
        [DataRow("H")]
        [DataRow("Hello World!")]
        [DataRow("猫")]
        [DataRow("Hello World! 猫")]
        public void WriteShortStringWorks(string value)
        {
            for (int i = 0; i < 2; i++)
            {
                bool littleEndian = i != 0;

                using (MemoryStream ms = new MemoryStream())
                {
                    using (BinaryWriterEx writer = new BinaryWriterEx(ms, true, littleEndian))
                    {
                        writer.WriteShortString(value);
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
}
