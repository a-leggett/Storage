using System;
using System.IO;
using System.Text;

namespace Storage.Data
{
    /// <summary>
    /// Similar to <see cref="System.IO.BinaryReader"/>, but uses a specific endianness.
    /// </summary>
    public sealed class BinaryReaderEx : IDisposable
    {
        /// <summary>
        /// The base <see cref="Stream"/> from which this <see cref="BinaryReaderEx"/>
        /// reads data.
        /// </summary>
        public Stream BaseStream { get; private set; }

        private Encoding encoding;

        /// <summary>
        /// Will the <see cref="BaseStream"/> remain open after this <see cref="BinaryReaderEx"/>
        /// is disposed?
        /// </summary>
        public bool WillLeaveStreamOpen { get; private set; }

        /// <summary>
        /// Is data stored in little-endian format on the <see cref="BaseStream"/>?
        /// </summary>
        public bool ReadsLittleEndian { get; private set; }

        /// <summary>
        /// Constructs a <see cref="BinaryReaderEx"/> that uses UTF-8 encoding.
        /// </summary>
        /// <param name="baseStream">The base <see cref="Stream"/> from which data will be read.</param>
        /// <param name="leaveStreamOpen">Should the <paramref name="baseStream"/> remain open after
        /// this <see cref="BinaryReaderEx"/> is disposed? If false, then <paramref name="baseStream"/>
        /// will be disposed when this <see cref="BinaryReaderEx"/> is disposed.</param>
        /// <param name="littleEndian">Is the data stored in little-endian format?</param>
        /// <exception cref="ArgumentNullException">Thrown if <paramref name="baseStream"/> is null.</exception>
        public BinaryReaderEx(Stream baseStream, bool leaveStreamOpen, bool littleEndian) : this(baseStream, Encoding.UTF8, leaveStreamOpen, littleEndian) { }

        /// <summary>
        /// Constructs a <see cref="BinaryReaderEx"/>.
        /// </summary>
        /// <param name="baseStream">The base <see cref="Stream"/> from which data will be read.</param>
        /// <param name="encoding">The <see cref="Encoding"/> to use when reading <see cref="string"/>s.</param>
        /// <param name="leaveStreamOpen">Should the <paramref name="baseStream"/> remain open after
        /// this <see cref="BinaryReaderEx"/> is disposed? If false, then <paramref name="baseStream"/>
        /// will be disposed when this <see cref="BinaryReaderEx"/> is disposed.</param>
        /// <param name="littleEndian">Is the data stored in little-endian format?</param>
        /// <exception cref="ArgumentNullException">Thrown if <paramref name="baseStream"/> or <paramref name="encoding"/> is null.</exception>
        public BinaryReaderEx(Stream baseStream, Encoding encoding, bool leaveStreamOpen, bool littleEndian)
        {
            this.BaseStream = baseStream ?? throw new ArgumentNullException(nameof(baseStream));
            this.encoding = encoding ?? throw new ArgumentNullException(nameof(encoding));
            this.ReadsLittleEndian = littleEndian;
            this.WillLeaveStreamOpen = leaveStreamOpen;
        }

        /// <summary>
        /// Reads a single byte and determines whether it
        /// represents a true value or a false value.
        /// </summary>
        /// <returns>True if the byte is non-zero, otherwise false.</returns>
        public bool ReadBoolean()
        {
            byte[] buffer = new byte[1];
            BaseStream.Read(buffer, 0, buffer.Length);
            return buffer[0] != 0x00;
        }

        /// <summary>
        /// Reads an <see cref="Int16"/> from the <see cref="BaseStream"/>.
        /// </summary>
        /// <returns>The <see cref="Int16"/> value.</returns>
        public Int16 ReadInt16()
        {
            byte[] buffer = new byte[sizeof(Int16)];
            BaseStream.Read(buffer, 0, buffer.Length);
            return Binary.ReadInt16(buffer, 0, ReadsLittleEndian);
        }

        /// <summary>
        /// Reads an <see cref="UInt16"/> from the <see cref="BaseStream"/>.
        /// </summary>
        /// <returns>The <see cref="UInt16"/> value.</returns>
        public UInt16 ReadUInt16()
        {
            byte[] buffer = new byte[sizeof(Int16)];
            BaseStream.Read(buffer, 0, buffer.Length);
            return Binary.ReadUInt16(buffer, 0, ReadsLittleEndian);
        }

        /// <summary>
        /// Reads an <see cref="Int32"/> from the <see cref="BaseStream"/>.
        /// </summary>
        /// <returns>The <see cref="Int32"/> value.</returns>
        public Int32 ReadInt32()
        {
            byte[] buffer = new byte[sizeof(Int32)];
            BaseStream.Read(buffer, 0, buffer.Length);
            return Binary.ReadInt32(buffer, 0, ReadsLittleEndian);
        }

        /// <summary>
        /// Reads an <see cref="UInt32"/> from the <see cref="BaseStream"/>.
        /// </summary>
        /// <returns>The <see cref="UInt32"/> value.</returns>
        public UInt32 ReadUInt32()
        {
            byte[] buffer = new byte[sizeof(Int32)];
            BaseStream.Read(buffer, 0, buffer.Length);
            return Binary.ReadUInt32(buffer, 0, ReadsLittleEndian);
        }

        /// <summary>
        /// Reads an <see cref="Int64"/> from the <see cref="BaseStream"/>.
        /// </summary>
        /// <returns>The <see cref="Int64"/> value.</returns>
        public Int64 ReadInt64()
        {
            byte[] buffer = new byte[sizeof(Int64)];
            BaseStream.Read(buffer, 0, buffer.Length);
            return Binary.ReadInt64(buffer, 0, ReadsLittleEndian);
        }

        /// <summary>
        /// Reads an <see cref="UInt64"/> from the <see cref="BaseStream"/>.
        /// </summary>
        /// <returns>The <see cref="UInt64"/> value.</returns>
        public UInt64 ReadUInt64()
        {
            byte[] buffer = new byte[sizeof(Int64)];
            BaseStream.Read(buffer, 0, buffer.Length);
            return Binary.ReadUInt64(buffer, 0, ReadsLittleEndian);
        }

        /// <summary>
        /// Reads a <see cref="UInt16"/>-prefixed <see cref="string"/>.
        /// </summary>
        /// <returns>The <see cref="string"/> value.</returns>
        /// <remarks>
        /// This method will call <see cref="ReadUInt16"/> to determine how
        /// many bytes to read for the <see cref="string"/>, and then will
        /// read that many bytes, and decode the <see cref="string"/> using
        /// the <see cref="Encoding"/> that was specified in the constructor.
        /// </remarks>
        public string ReadShortString()
        {
            //First read the number of bytes
            UInt16 byteCount = ReadUInt16();

            //Now read the string's bytes
            byte[] buffer = new byte[byteCount];
            BaseStream.Read(buffer, 0, buffer.Length);
            return encoding.GetString(buffer, 0, buffer.Length);
        }

        /// <summary>
        /// Reads a <see cref="Single"/> from the <see cref="BaseStream"/>.
        /// </summary>
        /// <returns>The <see cref="Single"/> value.</returns>
        public Single ReadSingle()
        {
            byte[] buffer = new byte[sizeof(Single)];
            BaseStream.Read(buffer, 0, buffer.Length);
            return Binary.ReadSingle(buffer, 0, ReadsLittleEndian);
        }

        /// <summary>
        /// Reads a <see cref="Double"/> from the <see cref="BaseStream"/>.
        /// </summary>
        /// <returns>The <see cref="Double"/> value.</returns>
        public Double ReadDouble()
        {
            byte[] buffer = new byte[sizeof(Double)];
            BaseStream.Read(buffer, 0, buffer.Length);
            return Binary.ReadDouble(buffer, 0, ReadsLittleEndian);
        }

        /// <summary>
        /// Disposes this <see cref="BinaryReaderEx"/>.
        /// </summary>
        /// <remarks>
        /// The <see cref="BaseStream"/> will be disposed unless <see cref="WillLeaveStreamOpen"/> is true.
        /// </remarks>
        public void Dispose()
        {
            if (!WillLeaveStreamOpen)
                BaseStream.Dispose();
        }
    }
}
