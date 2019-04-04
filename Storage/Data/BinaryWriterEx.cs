using System;
using System.IO;
using System.Text;

namespace Storage.Data
{
    /// <summary>
    /// Similar to <see cref="System.IO.BinaryWriter"/>, but uses a specific endianness.
    /// </summary>
    public sealed class BinaryWriterEx : IDisposable
    {
        /// <summary>
        /// The base <see cref="Stream"/> to which this <see cref="BinaryWriterEx"/>
        /// writes data.
        /// </summary>
        public Stream BaseStream { get; private set; }

        private Encoding encoding;

        /// <summary>
        /// Will the <see cref="BaseStream"/> remain open after this <see cref="BinaryWriterEx"/>
        /// is disposed?
        /// </summary>
        public bool WillLeaveStreamOpen { get; private set; }

        /// <summary>
        /// Will data be written to the <see cref="BaseStream"/> in little-endian format?
        /// </summary>
        public bool WritesLittleEndian { get; private set; }

        /// <summary>
        /// Constructs a <see cref="BinaryWriterEx"/> that uses UTF-8 encoding.
        /// </summary>
        /// <param name="baseStream">The base <see cref="Stream"/> to which data will be written.</param>
        /// <param name="leaveStreamOpen">Should the <paramref name="baseStream"/> remain open after
        /// this <see cref="BinaryWriterEx"/> is disposed? If false, then <paramref name="baseStream"/>
        /// will be disposed when this <see cref="BinaryWriterEx"/> is disposed.</param>
        /// <param name="littleEndian">Should data be written in little-endian format? If false, big-endian
        /// is used.</param>
        /// <exception cref="ArgumentNullException">Thrown if <paramref name="baseStream"/> is null.</exception>
        public BinaryWriterEx(Stream baseStream, bool leaveStreamOpen, bool littleEndian) : this(baseStream, Encoding.UTF8, leaveStreamOpen, littleEndian) { }

        /// <summary>
        /// Constructs a <see cref="BinaryWriterEx"/>.
        /// </summary>
        /// <param name="baseStream">The base <see cref="Stream"/> to which data will be written.</param>
        /// <param name="encoding">The <see cref="Encoding"/> to use when writing <see cref="string"/>s.</param>
        /// <param name="leaveStreamOpen">Should the <paramref name="baseStream"/> remain open after
        /// this <see cref="BinaryWriterEx"/> is disposed? If false, then <paramref name="baseStream"/>
        /// will be disposed when this <see cref="BinaryWriterEx"/> is disposed.</param>
        /// <param name="littleEndian">Should data be written in little-endian format? If false, big-endian
        /// is used.</param>
        /// <exception cref="ArgumentNullException">Thrown if <paramref name="baseStream"/> or
        /// <paramref name="encoding"/> is null.</exception>
        public BinaryWriterEx(Stream baseStream, Encoding encoding, bool leaveStreamOpen, bool littleEndian)
        {
            this.BaseStream = baseStream ?? throw new ArgumentNullException(nameof(baseStream));
            this.encoding = encoding ?? throw new ArgumentNullException(nameof(encoding));
            this.WritesLittleEndian = littleEndian;
            this.WillLeaveStreamOpen = leaveStreamOpen;
        }

        /// <summary>
        /// Writes a <see cref="Boolean"/> to the <see cref="BaseStream"/>.
        /// </summary>
        /// <param name="value">The value to write.</param>
        /// <remarks>
        /// If <paramref name="value"/> is false, a zero byte will be written.
        /// Otherwise the application should assume that any arbitrary non-zero
        /// byte will be written.
        /// </remarks>
        public void WriteBoolean(Boolean value)
        {
            byte[] buffer = new byte[] { value ? (byte)0xFF : (byte)0x00 };
            BaseStream.Write(buffer, 0, buffer.Length);
        }

        /// <summary>
        /// Writes a <see cref="Int16"/> to the <see cref="BaseStream"/>.
        /// </summary>
        /// <param name="value">The value to write.</param>
        public void WriteInt16(Int16 value)
        {
            byte[] buffer = Binary.GetInt16Bytes(value, WritesLittleEndian);
            BaseStream.Write(buffer, 0, buffer.Length);
        }

        /// <summary>
        /// Writes a <see cref="UInt16"/> to the <see cref="BaseStream"/>.
        /// </summary>
        /// <param name="value">The value to write.</param>
        public void WriteUInt16(UInt16 value)
        {
            byte[] buffer = Binary.GetUInt16Bytes(value, WritesLittleEndian);
            BaseStream.Write(buffer, 0, buffer.Length);
        }

        /// <summary>
        /// Writes a <see cref="Int32"/> to the <see cref="BaseStream"/>.
        /// </summary>
        /// <param name="value">The value to write.</param>
        public void WriteInt32(Int32 value)
        {
            byte[] buffer = Binary.GetInt32Bytes(value, WritesLittleEndian);
            BaseStream.Write(buffer, 0, buffer.Length);
        }

        /// <summary>
        /// Writes a <see cref="UInt32"/> to the <see cref="BaseStream"/>.
        /// </summary>
        /// <param name="value">The value to write.</param>
        public void WriteUInt32(UInt32 value)
        {
            byte[] buffer = Binary.GetUInt32Bytes(value, WritesLittleEndian);
            BaseStream.Write(buffer, 0, buffer.Length);
        }

        /// <summary>
        /// Writes a <see cref="Int64"/> to the <see cref="BaseStream"/>.
        /// </summary>
        /// <param name="value">The value to write.</param>
        public void WriteInt64(Int64 value)
        {
            byte[] buffer = Binary.GetInt64Bytes(value, WritesLittleEndian);
            BaseStream.Write(buffer, 0, buffer.Length);
        }

        /// <summary>
        /// Writes a <see cref="UInt64"/> to the <see cref="BaseStream"/>.
        /// </summary>
        /// <param name="value">The value to write.</param>
        public void WriteUInt64(UInt64 value)
        {
            byte[] buffer = Binary.GetUInt64Bytes(value, WritesLittleEndian);
            BaseStream.Write(buffer, 0, buffer.Length);
        }

        /// <summary>
        /// Writes a <see cref="UInt16"/>-prefixed <see cref="string"/>.
        /// </summary>
        /// <param name="value">The value to write.</param>
        /// <exception cref="ArgumentNullException">Thrown if <paramref name="value"/> is null.</exception>
        /// <exception cref="ArgumentException">Thrown if <paramref name="value"/>, when encoded
        /// using the constructor-specified <see cref="Encoding"/>, requires more than
        /// <see cref="UInt16.MaxValue"/> bytes.</exception>
        /// <remarks>
        /// <para>
        /// Note that the <paramref name="value"/> cannot have more than <see cref="UInt16.MaxValue"/>
        /// bytes (when encoded using the constructor-specified <see cref="Encoding"/>, see
        /// <see cref="Encoding.GetByteCount(string)"/>).
        /// </para>
        /// <para>
        /// A <see cref="UInt16"/> header will be written before the <see cref="string"/>'s
        /// bytes to define how many bytes are used. This may be different from the
        /// <see cref="string.Length"/> of the <paramref name="value"/>.
        /// </para>
        /// </remarks>
        public void WriteShortString(string value)
        {
            if (value == null)
                throw new ArgumentNullException(nameof(value));

            byte[] bytes = encoding.GetBytes(value);
            if (bytes.Length > UInt16.MaxValue)
                throw new ArgumentException("Cannot write a string which, when encoded using the specified " + nameof(Encoding) + ", requires more than " + nameof(UInt16) + "." + nameof(UInt16.MaxValue) + " bytes.", nameof(value));

            WriteUInt16((UInt16)bytes.Length);
            BaseStream.Write(bytes, 0, bytes.Length);
        }

        /// <summary>
        /// Writes a <see cref="Single"/> to the <see cref="BaseStream"/>.
        /// </summary>
        /// <param name="value">The value to write.</param>
        public void WriteSingle(Single value)
        {
            byte[] buffer = Binary.GetSingleBytes(value, WritesLittleEndian);
            BaseStream.Write(buffer, 0, buffer.Length);
        }

        /// <summary>
        /// Writes a <see cref="Double"/> to the <see cref="BaseStream"/>.
        /// </summary>
        /// <param name="value">The value to write.</param>
        public void WriteDouble(Double value)
        {
            byte[] buffer = Binary.GetDoubleBytes(value, WritesLittleEndian);
            BaseStream.Write(buffer, 0, buffer.Length);
        }

        /// <summary>
        /// Disposes this <see cref="BinaryWriterEx"/>.
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
