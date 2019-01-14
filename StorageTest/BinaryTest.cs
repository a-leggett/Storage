using Microsoft.VisualStudio.TestTools.UnitTesting;
using Storage.Data;
using System;

namespace StorageTest
{
    [TestClass]
    public class BinaryTest
    {
        [TestMethod]
        public void ReadWriteInt16Works()
        {
            Int16 a = Int16.MinValue;
            Int16 b = Int16.MaxValue;
            Int16 c = 0;
            Int16 d = (Int16)(a / 2);
            Int16 e = (Int16)(b / 2);

            byte[] aBytesLE = Binary.GetInt16Bytes(a, true);
            byte[] bBytesLE = Binary.GetInt16Bytes(b, true);
            byte[] cBytesLE = Binary.GetInt16Bytes(c, true);
            byte[] dBytesLE = Binary.GetInt16Bytes(d, true);
            byte[] eBytesLE = Binary.GetInt16Bytes(e, true);

            byte[] aBytesBE = Binary.GetInt16Bytes(a, false);
            byte[] bBytesBE = Binary.GetInt16Bytes(b, false);
            byte[] cBytesBE = Binary.GetInt16Bytes(c, false);
            byte[] dBytesBE = Binary.GetInt16Bytes(d, false);
            byte[] eBytesBE = Binary.GetInt16Bytes(e, false);

            byte[] aBytesLERead = new byte[aBytesLE.Length + 3];
            Array.Copy(aBytesLE, 0, aBytesLERead, 3, aBytesLE.Length);
            byte[] bBytesLERead = new byte[bBytesLE.Length + 3];
            Array.Copy(bBytesLE, 0, bBytesLERead, 3, bBytesLE.Length);
            byte[] cBytesLERead = new byte[cBytesLE.Length + 3];
            Array.Copy(cBytesLE, 0, cBytesLERead, 3, cBytesLE.Length);
            byte[] dBytesLERead = new byte[dBytesLE.Length + 3];
            Array.Copy(dBytesLE, 0, dBytesLERead, 3, dBytesLE.Length);
            byte[] eBytesLERead = new byte[eBytesLE.Length + 3];
            Array.Copy(eBytesLE, 0, eBytesLERead, 3, eBytesLE.Length);

            byte[] aBytesBERead = new byte[aBytesBE.Length + 3];
            Array.Copy(aBytesBE, 0, aBytesBERead, 3, aBytesBE.Length);
            byte[] bBytesBERead = new byte[bBytesBE.Length + 3];
            Array.Copy(bBytesBE, 0, bBytesBERead, 3, bBytesBE.Length);
            byte[] cBytesBERead = new byte[cBytesBE.Length + 3];
            Array.Copy(cBytesBE, 0, cBytesBERead, 3, cBytesBE.Length);
            byte[] dBytesBERead = new byte[dBytesBE.Length + 3];
            Array.Copy(dBytesBE, 0, dBytesBERead, 3, dBytesBE.Length);
            byte[] eBytesBERead = new byte[eBytesBE.Length + 3];
            Array.Copy(eBytesBE, 0, eBytesBERead, 3, eBytesBE.Length);

            Assert.AreEqual(a, Binary.ReadInt16(aBytesLERead, 3, true));
            Assert.AreEqual(b, Binary.ReadInt16(bBytesLERead, 3, true));
            Assert.AreEqual(c, Binary.ReadInt16(cBytesLERead, 3, true));
            Assert.AreEqual(d, Binary.ReadInt16(dBytesLERead, 3, true));
            Assert.AreEqual(e, Binary.ReadInt16(eBytesLERead, 3, true));

            Assert.AreEqual(a, Binary.ReadInt16(aBytesBERead, 3, false));
            Assert.AreEqual(b, Binary.ReadInt16(bBytesBERead, 3, false));
            Assert.AreEqual(c, Binary.ReadInt16(cBytesBERead, 3, false));
            Assert.AreEqual(d, Binary.ReadInt16(dBytesBERead, 3, false));
            Assert.AreEqual(e, Binary.ReadInt16(eBytesBERead, 3, false));
        }

        [TestMethod]
        public void ReadInt16CannotHaveNullArg()
        {
            Assert.ThrowsException<ArgumentNullException>(() =>
            {
                Binary.ReadInt16(null, 0, false);
            });

            Assert.ThrowsException<ArgumentNullException>(() => {
                Binary.ReadInt16(null, 0, true);
            });
        }

        [TestMethod]
        public void ReadInt16ArgsMustBeInRange()
        {
            byte[] buffer = new byte[sizeof(Int16)];
            Assert.ThrowsException<ArgumentOutOfRangeException>(() => {
                Binary.ReadInt16(buffer, -1, false);
            });

            Assert.ThrowsException<ArgumentOutOfRangeException>(() => {
                Binary.ReadInt16(buffer, 1, false);
            });

            Assert.ThrowsException<ArgumentOutOfRangeException>(() => {
                Binary.ReadInt16(buffer, -1, true);
            });

            Assert.ThrowsException<ArgumentOutOfRangeException>(() => {
                Binary.ReadInt16(buffer, 1, true);
            });
        }

        [TestMethod]
        public void ReadInt16DoesNotCorruptBuffer()
        {
            byte[] buffer = Binary.GetInt16Bytes(121, false);
            byte[] original = new byte[buffer.Length];
            Array.Copy(buffer, original, buffer.Length);
            Binary.ReadInt16(buffer, 0, false);
            Binary.ReadInt16(buffer, 0, true);
            CollectionAssert.AreEquivalent(original, buffer);
        }

        [TestMethod]
        public void ReadWriteUInt16Works()
        {
            UInt16 a = UInt16.MinValue;
            UInt16 b = UInt16.MaxValue;
            UInt16 c = 0;
            UInt16 d = (UInt16)(a / 2);
            UInt16 e = (UInt16)(b / 2);

            byte[] aBytesLE = Binary.GetUInt16Bytes(a, true);
            byte[] bBytesLE = Binary.GetUInt16Bytes(b, true);
            byte[] cBytesLE = Binary.GetUInt16Bytes(c, true);
            byte[] dBytesLE = Binary.GetUInt16Bytes(d, true);
            byte[] eBytesLE = Binary.GetUInt16Bytes(e, true);

            byte[] aBytesBE = Binary.GetUInt16Bytes(a, false);
            byte[] bBytesBE = Binary.GetUInt16Bytes(b, false);
            byte[] cBytesBE = Binary.GetUInt16Bytes(c, false);
            byte[] dBytesBE = Binary.GetUInt16Bytes(d, false);
            byte[] eBytesBE = Binary.GetUInt16Bytes(e, false);

            byte[] aBytesLERead = new byte[aBytesLE.Length + 3];
            Array.Copy(aBytesLE, 0, aBytesLERead, 3, aBytesLE.Length);
            byte[] bBytesLERead = new byte[bBytesLE.Length + 3];
            Array.Copy(bBytesLE, 0, bBytesLERead, 3, bBytesLE.Length);
            byte[] cBytesLERead = new byte[cBytesLE.Length + 3];
            Array.Copy(cBytesLE, 0, cBytesLERead, 3, cBytesLE.Length);
            byte[] dBytesLERead = new byte[dBytesLE.Length + 3];
            Array.Copy(dBytesLE, 0, dBytesLERead, 3, dBytesLE.Length);
            byte[] eBytesLERead = new byte[eBytesLE.Length + 3];
            Array.Copy(eBytesLE, 0, eBytesLERead, 3, eBytesLE.Length);

            byte[] aBytesBERead = new byte[aBytesBE.Length + 3];
            Array.Copy(aBytesBE, 0, aBytesBERead, 3, aBytesBE.Length);
            byte[] bBytesBERead = new byte[bBytesBE.Length + 3];
            Array.Copy(bBytesBE, 0, bBytesBERead, 3, bBytesBE.Length);
            byte[] cBytesBERead = new byte[cBytesBE.Length + 3];
            Array.Copy(cBytesBE, 0, cBytesBERead, 3, cBytesBE.Length);
            byte[] dBytesBERead = new byte[dBytesBE.Length + 3];
            Array.Copy(dBytesBE, 0, dBytesBERead, 3, dBytesBE.Length);
            byte[] eBytesBERead = new byte[eBytesBE.Length + 3];
            Array.Copy(eBytesBE, 0, eBytesBERead, 3, eBytesBE.Length);

            Assert.AreEqual(a, Binary.ReadUInt16(aBytesLERead, 3, true));
            Assert.AreEqual(b, Binary.ReadUInt16(bBytesLERead, 3, true));
            Assert.AreEqual(c, Binary.ReadUInt16(cBytesLERead, 3, true));
            Assert.AreEqual(d, Binary.ReadUInt16(dBytesLERead, 3, true));
            Assert.AreEqual(e, Binary.ReadUInt16(eBytesLERead, 3, true));

            Assert.AreEqual(a, Binary.ReadUInt16(aBytesBERead, 3, false));
            Assert.AreEqual(b, Binary.ReadUInt16(bBytesBERead, 3, false));
            Assert.AreEqual(c, Binary.ReadUInt16(cBytesBERead, 3, false));
            Assert.AreEqual(d, Binary.ReadUInt16(dBytesBERead, 3, false));
            Assert.AreEqual(e, Binary.ReadUInt16(eBytesBERead, 3, false));
        }

        [TestMethod]
        public void ReadUInt16CannotHaveNullArg()
        {
            Assert.ThrowsException<ArgumentNullException>(() =>
            {
                Binary.ReadUInt16(null, 0, false);
            });

            Assert.ThrowsException<ArgumentNullException>(() => {
                Binary.ReadUInt16(null, 0, true);
            });
        }

        [TestMethod]
        public void ReadUInt16ArgsMustBeInRange()
        {
            byte[] buffer = new byte[sizeof(UInt16)];
            Assert.ThrowsException<ArgumentOutOfRangeException>(() => {
                Binary.ReadUInt16(buffer, -1, false);
            });

            Assert.ThrowsException<ArgumentOutOfRangeException>(() => {
                Binary.ReadUInt16(buffer, 1, false);
            });

            Assert.ThrowsException<ArgumentOutOfRangeException>(() => {
                Binary.ReadUInt16(buffer, -1, true);
            });

            Assert.ThrowsException<ArgumentOutOfRangeException>(() => {
                Binary.ReadUInt16(buffer, 1, true);
            });
        }

        [TestMethod]
        public void ReadUInt16DoesNotCorruptBuffer()
        {
            byte[] buffer = Binary.GetUInt16Bytes(121, false);
            byte[] original = new byte[buffer.Length];
            Array.Copy(buffer, original, buffer.Length);
            Binary.ReadUInt16(buffer, 0, false);
            Binary.ReadUInt16(buffer, 0, true);
            CollectionAssert.AreEquivalent(original, buffer);
        }

        [TestMethod]
        public void ReadWriteInt32Works()
        {
            Int32 a = Int32.MinValue;
            Int32 b = Int32.MaxValue;
            Int32 c = 0;
            Int32 d = (Int32)(a / 2);
            Int32 e = (Int32)(b / 2);

            byte[] aBytesLE = Binary.GetInt32Bytes(a, true);
            byte[] bBytesLE = Binary.GetInt32Bytes(b, true);
            byte[] cBytesLE = Binary.GetInt32Bytes(c, true);
            byte[] dBytesLE = Binary.GetInt32Bytes(d, true);
            byte[] eBytesLE = Binary.GetInt32Bytes(e, true);

            byte[] aBytesBE = Binary.GetInt32Bytes(a, false);
            byte[] bBytesBE = Binary.GetInt32Bytes(b, false);
            byte[] cBytesBE = Binary.GetInt32Bytes(c, false);
            byte[] dBytesBE = Binary.GetInt32Bytes(d, false);
            byte[] eBytesBE = Binary.GetInt32Bytes(e, false);

            byte[] aBytesLERead = new byte[aBytesLE.Length + 3];
            Array.Copy(aBytesLE, 0, aBytesLERead, 3, aBytesLE.Length);
            byte[] bBytesLERead = new byte[bBytesLE.Length + 3];
            Array.Copy(bBytesLE, 0, bBytesLERead, 3, bBytesLE.Length);
            byte[] cBytesLERead = new byte[cBytesLE.Length + 3];
            Array.Copy(cBytesLE, 0, cBytesLERead, 3, cBytesLE.Length);
            byte[] dBytesLERead = new byte[dBytesLE.Length + 3];
            Array.Copy(dBytesLE, 0, dBytesLERead, 3, dBytesLE.Length);
            byte[] eBytesLERead = new byte[eBytesLE.Length + 3];
            Array.Copy(eBytesLE, 0, eBytesLERead, 3, eBytesLE.Length);

            byte[] aBytesBERead = new byte[aBytesBE.Length + 3];
            Array.Copy(aBytesBE, 0, aBytesBERead, 3, aBytesBE.Length);
            byte[] bBytesBERead = new byte[bBytesBE.Length + 3];
            Array.Copy(bBytesBE, 0, bBytesBERead, 3, bBytesBE.Length);
            byte[] cBytesBERead = new byte[cBytesBE.Length + 3];
            Array.Copy(cBytesBE, 0, cBytesBERead, 3, cBytesBE.Length);
            byte[] dBytesBERead = new byte[dBytesBE.Length + 3];
            Array.Copy(dBytesBE, 0, dBytesBERead, 3, dBytesBE.Length);
            byte[] eBytesBERead = new byte[eBytesBE.Length + 3];
            Array.Copy(eBytesBE, 0, eBytesBERead, 3, eBytesBE.Length);

            Assert.AreEqual(a, Binary.ReadInt32(aBytesLERead, 3, true));
            Assert.AreEqual(b, Binary.ReadInt32(bBytesLERead, 3, true));
            Assert.AreEqual(c, Binary.ReadInt32(cBytesLERead, 3, true));
            Assert.AreEqual(d, Binary.ReadInt32(dBytesLERead, 3, true));
            Assert.AreEqual(e, Binary.ReadInt32(eBytesLERead, 3, true));

            Assert.AreEqual(a, Binary.ReadInt32(aBytesBERead, 3, false));
            Assert.AreEqual(b, Binary.ReadInt32(bBytesBERead, 3, false));
            Assert.AreEqual(c, Binary.ReadInt32(cBytesBERead, 3, false));
            Assert.AreEqual(d, Binary.ReadInt32(dBytesBERead, 3, false));
            Assert.AreEqual(e, Binary.ReadInt32(eBytesBERead, 3, false));
        }

        [TestMethod]
        public void ReadInt32CannotHaveNullArg()
        {
            Assert.ThrowsException<ArgumentNullException>(() =>
            {
                Binary.ReadInt32(null, 0, false);
            });

            Assert.ThrowsException<ArgumentNullException>(() => {
                Binary.ReadInt32(null, 0, true);
            });
        }

        [TestMethod]
        public void ReadInt32ArgsMustBeInRange()
        {
            byte[] buffer = new byte[sizeof(Int32)];
            Assert.ThrowsException<ArgumentOutOfRangeException>(() => {
                Binary.ReadInt32(buffer, -1, false);
            });

            Assert.ThrowsException<ArgumentOutOfRangeException>(() => {
                Binary.ReadInt32(buffer, 1, false);
            });

            Assert.ThrowsException<ArgumentOutOfRangeException>(() => {
                Binary.ReadInt32(buffer, -1, true);
            });

            Assert.ThrowsException<ArgumentOutOfRangeException>(() => {
                Binary.ReadInt32(buffer, 1, true);
            });
        }

        [TestMethod]
        public void ReadInt32DoesNotCorruptBuffer()
        {
            byte[] buffer = Binary.GetInt32Bytes(121, false);
            byte[] original = new byte[buffer.Length];
            Array.Copy(buffer, original, buffer.Length);
            Binary.ReadInt32(buffer, 0, false);
            Binary.ReadInt32(buffer, 0, true);
            CollectionAssert.AreEquivalent(original, buffer);
        }

        [TestMethod]
        public void ReadWriteUInt32Works()
        {
            UInt32 a = UInt32.MinValue;
            UInt32 b = UInt32.MaxValue;
            UInt32 c = 0;
            UInt32 d = (UInt32)(a / 2);
            UInt32 e = (UInt32)(b / 2);

            byte[] aBytesLE = Binary.GetUInt32Bytes(a, true);
            byte[] bBytesLE = Binary.GetUInt32Bytes(b, true);
            byte[] cBytesLE = Binary.GetUInt32Bytes(c, true);
            byte[] dBytesLE = Binary.GetUInt32Bytes(d, true);
            byte[] eBytesLE = Binary.GetUInt32Bytes(e, true);

            byte[] aBytesBE = Binary.GetUInt32Bytes(a, false);
            byte[] bBytesBE = Binary.GetUInt32Bytes(b, false);
            byte[] cBytesBE = Binary.GetUInt32Bytes(c, false);
            byte[] dBytesBE = Binary.GetUInt32Bytes(d, false);
            byte[] eBytesBE = Binary.GetUInt32Bytes(e, false);

            byte[] aBytesLERead = new byte[aBytesLE.Length + 3];
            Array.Copy(aBytesLE, 0, aBytesLERead, 3, aBytesLE.Length);
            byte[] bBytesLERead = new byte[bBytesLE.Length + 3];
            Array.Copy(bBytesLE, 0, bBytesLERead, 3, bBytesLE.Length);
            byte[] cBytesLERead = new byte[cBytesLE.Length + 3];
            Array.Copy(cBytesLE, 0, cBytesLERead, 3, cBytesLE.Length);
            byte[] dBytesLERead = new byte[dBytesLE.Length + 3];
            Array.Copy(dBytesLE, 0, dBytesLERead, 3, dBytesLE.Length);
            byte[] eBytesLERead = new byte[eBytesLE.Length + 3];
            Array.Copy(eBytesLE, 0, eBytesLERead, 3, eBytesLE.Length);

            byte[] aBytesBERead = new byte[aBytesBE.Length + 3];
            Array.Copy(aBytesBE, 0, aBytesBERead, 3, aBytesBE.Length);
            byte[] bBytesBERead = new byte[bBytesBE.Length + 3];
            Array.Copy(bBytesBE, 0, bBytesBERead, 3, bBytesBE.Length);
            byte[] cBytesBERead = new byte[cBytesBE.Length + 3];
            Array.Copy(cBytesBE, 0, cBytesBERead, 3, cBytesBE.Length);
            byte[] dBytesBERead = new byte[dBytesBE.Length + 3];
            Array.Copy(dBytesBE, 0, dBytesBERead, 3, dBytesBE.Length);
            byte[] eBytesBERead = new byte[eBytesBE.Length + 3];
            Array.Copy(eBytesBE, 0, eBytesBERead, 3, eBytesBE.Length);

            Assert.AreEqual(a, Binary.ReadUInt32(aBytesLERead, 3, true));
            Assert.AreEqual(b, Binary.ReadUInt32(bBytesLERead, 3, true));
            Assert.AreEqual(c, Binary.ReadUInt32(cBytesLERead, 3, true));
            Assert.AreEqual(d, Binary.ReadUInt32(dBytesLERead, 3, true));
            Assert.AreEqual(e, Binary.ReadUInt32(eBytesLERead, 3, true));

            Assert.AreEqual(a, Binary.ReadUInt32(aBytesBERead, 3, false));
            Assert.AreEqual(b, Binary.ReadUInt32(bBytesBERead, 3, false));
            Assert.AreEqual(c, Binary.ReadUInt32(cBytesBERead, 3, false));
            Assert.AreEqual(d, Binary.ReadUInt32(dBytesBERead, 3, false));
            Assert.AreEqual(e, Binary.ReadUInt32(eBytesBERead, 3, false));
        }

        [TestMethod]
        public void ReadUInt32CannotHaveNullArg()
        {
            Assert.ThrowsException<ArgumentNullException>(() =>
            {
                Binary.ReadUInt32(null, 0, false);
            });

            Assert.ThrowsException<ArgumentNullException>(() => {
                Binary.ReadUInt32(null, 0, true);
            });
        }

        [TestMethod]
        public void ReadUInt32ArgsMustBeInRange()
        {
            byte[] buffer = new byte[sizeof(UInt32)];
            Assert.ThrowsException<ArgumentOutOfRangeException>(() => {
                Binary.ReadUInt32(buffer, -1, false);
            });

            Assert.ThrowsException<ArgumentOutOfRangeException>(() => {
                Binary.ReadUInt32(buffer, 1, false);
            });

            Assert.ThrowsException<ArgumentOutOfRangeException>(() => {
                Binary.ReadUInt32(buffer, -1, true);
            });

            Assert.ThrowsException<ArgumentOutOfRangeException>(() => {
                Binary.ReadUInt32(buffer, 1, true);
            });
        }

        [TestMethod]
        public void ReadUInt32DoesNotCorruptBuffer()
        {
            byte[] buffer = Binary.GetUInt32Bytes(121, false);
            byte[] original = new byte[buffer.Length];
            Array.Copy(buffer, original, buffer.Length);
            Binary.ReadUInt32(buffer, 0, false);
            Binary.ReadUInt32(buffer, 0, true);
            CollectionAssert.AreEquivalent(original, buffer);
        }

        [TestMethod]
        public void ReadWriteInt64Works()
        {
            Int64 a = Int64.MinValue;
            Int64 b = Int64.MaxValue;
            Int64 c = 0;
            Int64 d = (Int64)(a / 2);
            Int64 e = (Int64)(b / 2);

            byte[] aBytesLE = Binary.GetInt64Bytes(a, true);
            byte[] bBytesLE = Binary.GetInt64Bytes(b, true);
            byte[] cBytesLE = Binary.GetInt64Bytes(c, true);
            byte[] dBytesLE = Binary.GetInt64Bytes(d, true);
            byte[] eBytesLE = Binary.GetInt64Bytes(e, true);

            byte[] aBytesBE = Binary.GetInt64Bytes(a, false);
            byte[] bBytesBE = Binary.GetInt64Bytes(b, false);
            byte[] cBytesBE = Binary.GetInt64Bytes(c, false);
            byte[] dBytesBE = Binary.GetInt64Bytes(d, false);
            byte[] eBytesBE = Binary.GetInt64Bytes(e, false);

            byte[] aBytesLERead = new byte[aBytesLE.Length + 3];
            Array.Copy(aBytesLE, 0, aBytesLERead, 3, aBytesLE.Length);
            byte[] bBytesLERead = new byte[bBytesLE.Length + 3];
            Array.Copy(bBytesLE, 0, bBytesLERead, 3, bBytesLE.Length);
            byte[] cBytesLERead = new byte[cBytesLE.Length + 3];
            Array.Copy(cBytesLE, 0, cBytesLERead, 3, cBytesLE.Length);
            byte[] dBytesLERead = new byte[dBytesLE.Length + 3];
            Array.Copy(dBytesLE, 0, dBytesLERead, 3, dBytesLE.Length);
            byte[] eBytesLERead = new byte[eBytesLE.Length + 3];
            Array.Copy(eBytesLE, 0, eBytesLERead, 3, eBytesLE.Length);

            byte[] aBytesBERead = new byte[aBytesBE.Length + 3];
            Array.Copy(aBytesBE, 0, aBytesBERead, 3, aBytesBE.Length);
            byte[] bBytesBERead = new byte[bBytesBE.Length + 3];
            Array.Copy(bBytesBE, 0, bBytesBERead, 3, bBytesBE.Length);
            byte[] cBytesBERead = new byte[cBytesBE.Length + 3];
            Array.Copy(cBytesBE, 0, cBytesBERead, 3, cBytesBE.Length);
            byte[] dBytesBERead = new byte[dBytesBE.Length + 3];
            Array.Copy(dBytesBE, 0, dBytesBERead, 3, dBytesBE.Length);
            byte[] eBytesBERead = new byte[eBytesBE.Length + 3];
            Array.Copy(eBytesBE, 0, eBytesBERead, 3, eBytesBE.Length);

            Assert.AreEqual(a, Binary.ReadInt64(aBytesLERead, 3, true));
            Assert.AreEqual(b, Binary.ReadInt64(bBytesLERead, 3, true));
            Assert.AreEqual(c, Binary.ReadInt64(cBytesLERead, 3, true));
            Assert.AreEqual(d, Binary.ReadInt64(dBytesLERead, 3, true));
            Assert.AreEqual(e, Binary.ReadInt64(eBytesLERead, 3, true));

            Assert.AreEqual(a, Binary.ReadInt64(aBytesBERead, 3, false));
            Assert.AreEqual(b, Binary.ReadInt64(bBytesBERead, 3, false));
            Assert.AreEqual(c, Binary.ReadInt64(cBytesBERead, 3, false));
            Assert.AreEqual(d, Binary.ReadInt64(dBytesBERead, 3, false));
            Assert.AreEqual(e, Binary.ReadInt64(eBytesBERead, 3, false));
        }

        [TestMethod]
        public void ReadInt64CannotHaveNullArg()
        {
            Assert.ThrowsException<ArgumentNullException>(() =>
            {
                Binary.ReadInt64(null, 0, false);
            });

            Assert.ThrowsException<ArgumentNullException>(() => {
                Binary.ReadInt64(null, 0, true);
            });
        }

        [TestMethod]
        public void ReadInt64ArgsMustBeInRange()
        {
            byte[] buffer = new byte[sizeof(Int64)];
            Assert.ThrowsException<ArgumentOutOfRangeException>(() => {
                Binary.ReadInt64(buffer, -1, false);
            });

            Assert.ThrowsException<ArgumentOutOfRangeException>(() => {
                Binary.ReadInt64(buffer, 1, false);
            });

            Assert.ThrowsException<ArgumentOutOfRangeException>(() => {
                Binary.ReadInt64(buffer, -1, true);
            });

            Assert.ThrowsException<ArgumentOutOfRangeException>(() => {
                Binary.ReadInt64(buffer, 1, true);
            });
        }

        [TestMethod]
        public void ReadInt64DoesNotCorruptBuffer()
        {
            byte[] buffer = Binary.GetInt64Bytes(121, false);
            byte[] original = new byte[buffer.Length];
            Array.Copy(buffer, original, buffer.Length);
            Binary.ReadInt64(buffer, 0, false);
            Binary.ReadInt64(buffer, 0, true);
            CollectionAssert.AreEquivalent(original, buffer);
        }

        [TestMethod]
        public void ReadWriteUInt64Works()
        {
            UInt64 a = UInt64.MinValue;
            UInt64 b = UInt64.MaxValue;
            UInt64 c = 0;
            UInt64 d = (UInt64)(a / 2);
            UInt64 e = (UInt64)(b / 2);

            byte[] aBytesLE = Binary.GetUInt64Bytes(a, true);
            byte[] bBytesLE = Binary.GetUInt64Bytes(b, true);
            byte[] cBytesLE = Binary.GetUInt64Bytes(c, true);
            byte[] dBytesLE = Binary.GetUInt64Bytes(d, true);
            byte[] eBytesLE = Binary.GetUInt64Bytes(e, true);

            byte[] aBytesBE = Binary.GetUInt64Bytes(a, false);
            byte[] bBytesBE = Binary.GetUInt64Bytes(b, false);
            byte[] cBytesBE = Binary.GetUInt64Bytes(c, false);
            byte[] dBytesBE = Binary.GetUInt64Bytes(d, false);
            byte[] eBytesBE = Binary.GetUInt64Bytes(e, false);

            byte[] aBytesLERead = new byte[aBytesLE.Length + 3];
            Array.Copy(aBytesLE, 0, aBytesLERead, 3, aBytesLE.Length);
            byte[] bBytesLERead = new byte[bBytesLE.Length + 3];
            Array.Copy(bBytesLE, 0, bBytesLERead, 3, bBytesLE.Length);
            byte[] cBytesLERead = new byte[cBytesLE.Length + 3];
            Array.Copy(cBytesLE, 0, cBytesLERead, 3, cBytesLE.Length);
            byte[] dBytesLERead = new byte[dBytesLE.Length + 3];
            Array.Copy(dBytesLE, 0, dBytesLERead, 3, dBytesLE.Length);
            byte[] eBytesLERead = new byte[eBytesLE.Length + 3];
            Array.Copy(eBytesLE, 0, eBytesLERead, 3, eBytesLE.Length);

            byte[] aBytesBERead = new byte[aBytesBE.Length + 3];
            Array.Copy(aBytesBE, 0, aBytesBERead, 3, aBytesBE.Length);
            byte[] bBytesBERead = new byte[bBytesBE.Length + 3];
            Array.Copy(bBytesBE, 0, bBytesBERead, 3, bBytesBE.Length);
            byte[] cBytesBERead = new byte[cBytesBE.Length + 3];
            Array.Copy(cBytesBE, 0, cBytesBERead, 3, cBytesBE.Length);
            byte[] dBytesBERead = new byte[dBytesBE.Length + 3];
            Array.Copy(dBytesBE, 0, dBytesBERead, 3, dBytesBE.Length);
            byte[] eBytesBERead = new byte[eBytesBE.Length + 3];
            Array.Copy(eBytesBE, 0, eBytesBERead, 3, eBytesBE.Length);

            Assert.AreEqual(a, Binary.ReadUInt64(aBytesLERead, 3, true));
            Assert.AreEqual(b, Binary.ReadUInt64(bBytesLERead, 3, true));
            Assert.AreEqual(c, Binary.ReadUInt64(cBytesLERead, 3, true));
            Assert.AreEqual(d, Binary.ReadUInt64(dBytesLERead, 3, true));
            Assert.AreEqual(e, Binary.ReadUInt64(eBytesLERead, 3, true));

            Assert.AreEqual(a, Binary.ReadUInt64(aBytesBERead, 3, false));
            Assert.AreEqual(b, Binary.ReadUInt64(bBytesBERead, 3, false));
            Assert.AreEqual(c, Binary.ReadUInt64(cBytesBERead, 3, false));
            Assert.AreEqual(d, Binary.ReadUInt64(dBytesBERead, 3, false));
            Assert.AreEqual(e, Binary.ReadUInt64(eBytesBERead, 3, false));
        }

        [TestMethod]
        public void ReadUInt64CannotHaveNullArg()
        {
            Assert.ThrowsException<ArgumentNullException>(() =>
            {
                Binary.ReadUInt64(null, 0, false);
            });

            Assert.ThrowsException<ArgumentNullException>(() => {
                Binary.ReadUInt64(null, 0, true);
            });
        }

        [TestMethod]
        public void ReadUInt64ArgsMustBeInRange()
        {
            byte[] buffer = new byte[sizeof(UInt64)];
            Assert.ThrowsException<ArgumentOutOfRangeException>(() => {
                Binary.ReadUInt64(buffer, -1, false);
            });

            Assert.ThrowsException<ArgumentOutOfRangeException>(() => {
                Binary.ReadUInt64(buffer, 1, false);
            });

            Assert.ThrowsException<ArgumentOutOfRangeException>(() => {
                Binary.ReadUInt64(buffer, -1, true);
            });

            Assert.ThrowsException<ArgumentOutOfRangeException>(() => {
                Binary.ReadUInt64(buffer, 1, true);
            });
        }

        [TestMethod]
        public void ReadUInt64DoesNotCorruptBuffer()
        {
            byte[] buffer = Binary.GetUInt64Bytes(121, false);
            byte[] original = new byte[buffer.Length];
            Array.Copy(buffer, original, buffer.Length);
            Binary.ReadUInt64(buffer, 0, false);
            Binary.ReadUInt64(buffer, 0, true);
            CollectionAssert.AreEquivalent(original, buffer);
        }

        [TestMethod]
        public void ReadWriteSingleWorks()
        {
            Single a = Single.MinValue;
            Single b = Single.MaxValue;
            Single c = 0;
            Single d = (Single)(a / 2);
            Single e = (Single)(b / 2);

            byte[] aBytesLE = Binary.GetSingleBytes(a, true);
            byte[] bBytesLE = Binary.GetSingleBytes(b, true);
            byte[] cBytesLE = Binary.GetSingleBytes(c, true);
            byte[] dBytesLE = Binary.GetSingleBytes(d, true);
            byte[] eBytesLE = Binary.GetSingleBytes(e, true);

            byte[] aBytesBE = Binary.GetSingleBytes(a, false);
            byte[] bBytesBE = Binary.GetSingleBytes(b, false);
            byte[] cBytesBE = Binary.GetSingleBytes(c, false);
            byte[] dBytesBE = Binary.GetSingleBytes(d, false);
            byte[] eBytesBE = Binary.GetSingleBytes(e, false);

            byte[] aBytesLERead = new byte[aBytesLE.Length + 3];
            Array.Copy(aBytesLE, 0, aBytesLERead, 3, aBytesLE.Length);
            byte[] bBytesLERead = new byte[bBytesLE.Length + 3];
            Array.Copy(bBytesLE, 0, bBytesLERead, 3, bBytesLE.Length);
            byte[] cBytesLERead = new byte[cBytesLE.Length + 3];
            Array.Copy(cBytesLE, 0, cBytesLERead, 3, cBytesLE.Length);
            byte[] dBytesLERead = new byte[dBytesLE.Length + 3];
            Array.Copy(dBytesLE, 0, dBytesLERead, 3, dBytesLE.Length);
            byte[] eBytesLERead = new byte[eBytesLE.Length + 3];
            Array.Copy(eBytesLE, 0, eBytesLERead, 3, eBytesLE.Length);

            byte[] aBytesBERead = new byte[aBytesBE.Length + 3];
            Array.Copy(aBytesBE, 0, aBytesBERead, 3, aBytesBE.Length);
            byte[] bBytesBERead = new byte[bBytesBE.Length + 3];
            Array.Copy(bBytesBE, 0, bBytesBERead, 3, bBytesBE.Length);
            byte[] cBytesBERead = new byte[cBytesBE.Length + 3];
            Array.Copy(cBytesBE, 0, cBytesBERead, 3, cBytesBE.Length);
            byte[] dBytesBERead = new byte[dBytesBE.Length + 3];
            Array.Copy(dBytesBE, 0, dBytesBERead, 3, dBytesBE.Length);
            byte[] eBytesBERead = new byte[eBytesBE.Length + 3];
            Array.Copy(eBytesBE, 0, eBytesBERead, 3, eBytesBE.Length);

            Assert.AreEqual(a, Binary.ReadSingle(aBytesLERead, 3, true));
            Assert.AreEqual(b, Binary.ReadSingle(bBytesLERead, 3, true));
            Assert.AreEqual(c, Binary.ReadSingle(cBytesLERead, 3, true));
            Assert.AreEqual(d, Binary.ReadSingle(dBytesLERead, 3, true));
            Assert.AreEqual(e, Binary.ReadSingle(eBytesLERead, 3, true));

            Assert.AreEqual(a, Binary.ReadSingle(aBytesBERead, 3, false));
            Assert.AreEqual(b, Binary.ReadSingle(bBytesBERead, 3, false));
            Assert.AreEqual(c, Binary.ReadSingle(cBytesBERead, 3, false));
            Assert.AreEqual(d, Binary.ReadSingle(dBytesBERead, 3, false));
            Assert.AreEqual(e, Binary.ReadSingle(eBytesBERead, 3, false));
        }

        [TestMethod]
        public void ReadSingleCannotHaveNullArg()
        {
            Assert.ThrowsException<ArgumentNullException>(() =>
            {
                Binary.ReadSingle(null, 0, false);
            });

            Assert.ThrowsException<ArgumentNullException>(() => {
                Binary.ReadSingle(null, 0, true);
            });
        }

        [TestMethod]
        public void ReadSingleArgsMustBeInRange()
        {
            byte[] buffer = new byte[sizeof(Single)];
            Assert.ThrowsException<ArgumentOutOfRangeException>(() => {
                Binary.ReadSingle(buffer, -1, false);
            });

            Assert.ThrowsException<ArgumentOutOfRangeException>(() => {
                Binary.ReadSingle(buffer, 1, false);
            });

            Assert.ThrowsException<ArgumentOutOfRangeException>(() => {
                Binary.ReadSingle(buffer, -1, true);
            });

            Assert.ThrowsException<ArgumentOutOfRangeException>(() => {
                Binary.ReadSingle(buffer, 1, true);
            });
        }

        [TestMethod]
        public void ReadSingleDoesNotCorruptBuffer()
        {
            byte[] buffer = Binary.GetSingleBytes(121, false);
            byte[] original = new byte[buffer.Length];
            Array.Copy(buffer, original, buffer.Length);
            Binary.ReadSingle(buffer, 0, false);
            Binary.ReadSingle(buffer, 0, true);
            CollectionAssert.AreEquivalent(original, buffer);
        }

        [TestMethod]
        public void ReadWriteDoubleWorks()
        {
            Double a = Double.MinValue;
            Double b = Double.MaxValue;
            Double c = 0;
            Double d = (Double)(a / 2);
            Double e = (Double)(b / 2);

            byte[] aBytesLE = Binary.GetDoubleBytes(a, true);
            byte[] bBytesLE = Binary.GetDoubleBytes(b, true);
            byte[] cBytesLE = Binary.GetDoubleBytes(c, true);
            byte[] dBytesLE = Binary.GetDoubleBytes(d, true);
            byte[] eBytesLE = Binary.GetDoubleBytes(e, true);

            byte[] aBytesBE = Binary.GetDoubleBytes(a, false);
            byte[] bBytesBE = Binary.GetDoubleBytes(b, false);
            byte[] cBytesBE = Binary.GetDoubleBytes(c, false);
            byte[] dBytesBE = Binary.GetDoubleBytes(d, false);
            byte[] eBytesBE = Binary.GetDoubleBytes(e, false);

            byte[] aBytesLERead = new byte[aBytesLE.Length + 3];
            Array.Copy(aBytesLE, 0, aBytesLERead, 3, aBytesLE.Length);
            byte[] bBytesLERead = new byte[bBytesLE.Length + 3];
            Array.Copy(bBytesLE, 0, bBytesLERead, 3, bBytesLE.Length);
            byte[] cBytesLERead = new byte[cBytesLE.Length + 3];
            Array.Copy(cBytesLE, 0, cBytesLERead, 3, cBytesLE.Length);
            byte[] dBytesLERead = new byte[dBytesLE.Length + 3];
            Array.Copy(dBytesLE, 0, dBytesLERead, 3, dBytesLE.Length);
            byte[] eBytesLERead = new byte[eBytesLE.Length + 3];
            Array.Copy(eBytesLE, 0, eBytesLERead, 3, eBytesLE.Length);

            byte[] aBytesBERead = new byte[aBytesBE.Length + 3];
            Array.Copy(aBytesBE, 0, aBytesBERead, 3, aBytesBE.Length);
            byte[] bBytesBERead = new byte[bBytesBE.Length + 3];
            Array.Copy(bBytesBE, 0, bBytesBERead, 3, bBytesBE.Length);
            byte[] cBytesBERead = new byte[cBytesBE.Length + 3];
            Array.Copy(cBytesBE, 0, cBytesBERead, 3, cBytesBE.Length);
            byte[] dBytesBERead = new byte[dBytesBE.Length + 3];
            Array.Copy(dBytesBE, 0, dBytesBERead, 3, dBytesBE.Length);
            byte[] eBytesBERead = new byte[eBytesBE.Length + 3];
            Array.Copy(eBytesBE, 0, eBytesBERead, 3, eBytesBE.Length);

            Assert.AreEqual(a, Binary.ReadDouble(aBytesLERead, 3, true));
            Assert.AreEqual(b, Binary.ReadDouble(bBytesLERead, 3, true));
            Assert.AreEqual(c, Binary.ReadDouble(cBytesLERead, 3, true));
            Assert.AreEqual(d, Binary.ReadDouble(dBytesLERead, 3, true));
            Assert.AreEqual(e, Binary.ReadDouble(eBytesLERead, 3, true));

            Assert.AreEqual(a, Binary.ReadDouble(aBytesBERead, 3, false));
            Assert.AreEqual(b, Binary.ReadDouble(bBytesBERead, 3, false));
            Assert.AreEqual(c, Binary.ReadDouble(cBytesBERead, 3, false));
            Assert.AreEqual(d, Binary.ReadDouble(dBytesBERead, 3, false));
            Assert.AreEqual(e, Binary.ReadDouble(eBytesBERead, 3, false));
        }

        [TestMethod]
        public void ReadDoubleCannotHaveNullArg()
        {
            Assert.ThrowsException<ArgumentNullException>(() =>
            {
                Binary.ReadDouble(null, 0, false);
            });

            Assert.ThrowsException<ArgumentNullException>(() => {
                Binary.ReadDouble(null, 0, true);
            });
        }

        [TestMethod]
        public void ReadDoubleArgsMustBeInRange()
        {
            byte[] buffer = new byte[sizeof(Double)];
            Assert.ThrowsException<ArgumentOutOfRangeException>(() => {
                Binary.ReadDouble(buffer, -1, false);
            });

            Assert.ThrowsException<ArgumentOutOfRangeException>(() => {
                Binary.ReadDouble(buffer, 1, false);
            });

            Assert.ThrowsException<ArgumentOutOfRangeException>(() => {
                Binary.ReadDouble(buffer, -1, true);
            });

            Assert.ThrowsException<ArgumentOutOfRangeException>(() => {
                Binary.ReadDouble(buffer, 1, true);
            });
        }

        [TestMethod]
        public void ReadDoubleDoesNotCorruptBuffer()
        {
            byte[] buffer = Binary.GetDoubleBytes(121, false);
            byte[] original = new byte[buffer.Length];
            Array.Copy(buffer, original, buffer.Length);
            Binary.ReadDouble(buffer, 0, false);
            Binary.ReadDouble(buffer, 0, true);
            CollectionAssert.AreEquivalent(original, buffer);
        }
    }
}
