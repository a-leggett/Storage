using System;

namespace Storage.Data
{
    /// <summary>
    /// Static class for binary storage methods.
    /// </summary>
    public static class Binary
    {
        /// <summary>
        /// Converts an <see cref="Int16"/> to a byte array.
        /// </summary>
        /// <param name="value">The <see cref="Int16"/> value.</param>
        /// <param name="littleEndian">Should the bytes be ordered in little-endian? If false, big endian is used.</param>
        /// <returns>The two bytes.</returns>
        public static byte[] GetInt16Bytes(Int16 value, bool littleEndian)
        {
            byte[] ret = BitConverter.GetBytes(value);
            if (BitConverter.IsLittleEndian != littleEndian)
                Array.Reverse(ret);

            return ret;
        }

        /// <summary>
        /// Converts an <see cref="UInt16"/> to a byte array.
        /// </summary>
        /// <param name="value">The <see cref="UInt16"/> value.</param>
        /// <param name="littleEndian">Should the bytes be ordered in little-endian? If false, big endian is used.</param>
        /// <returns>The two bytes.</returns>
        public static byte[] GetUInt16Bytes(UInt16 value, bool littleEndian)
        {
            byte[] ret = BitConverter.GetBytes(value);
            if (BitConverter.IsLittleEndian != littleEndian)
                Array.Reverse(ret);

            return ret;
        }

        /// <summary>
        /// Reads an <see cref="Int16"/> from a byte array.
        /// </summary>
        /// <param name="buffer">The buffer containing the value.</param>
        /// <param name="offset">The position in the <paramref name="buffer"/> of the first byte to read.</param>
        /// <param name="littleEndian">Should the bytes be interpreted as little-endian? If false, big endian is used.</param>
        /// <returns>The resulting value.</returns>
        /// <exception cref="ArgumentNullException">Thrown if <paramref name="buffer"/> is null.</exception>
        /// <exception cref="ArgumentOutOfRangeException">Thrown if <paramref name="offset"/> is invalid (accounting for
        /// the size of an <see cref="Int16"/>).</exception>
        public static Int16 ReadInt16(byte[] buffer, int offset, bool littleEndian)
        {
            if (buffer == null)
                throw new ArgumentNullException(nameof(buffer));
            if (offset < 0 || offset + sizeof(Int16) > buffer.Length)
                throw new ArgumentOutOfRangeException(nameof(offset));

            if (BitConverter.IsLittleEndian == littleEndian)
            {
                return BitConverter.ToInt16(buffer, offset);
            }
            else
            {
                byte[] copy = new byte[sizeof(Int16)];
                Array.Copy(buffer, offset, copy, 0, copy.Length);
                Array.Reverse(copy);
                return BitConverter.ToInt16(copy, 0);
            }
        }

        /// <summary>
        /// Reads an <see cref="UInt16"/> from a byte array.
        /// </summary>
        /// <param name="buffer">The buffer containing the value.</param>
        /// <param name="offset">The position in the <paramref name="buffer"/> of the first byte to read.</param>
        /// <param name="littleEndian">Should the bytes be interpreted as little-endian? If false, big endian is used.</param>
        /// <returns>The resulting value.</returns>
        /// <exception cref="ArgumentNullException">Thrown if <paramref name="buffer"/> is null.</exception>
        /// <exception cref="ArgumentOutOfRangeException">Thrown if <paramref name="offset"/> is invalid (accounting for
        /// the size of an <see cref="UInt16"/>).</exception>
        public static UInt16 ReadUInt16(byte[] buffer, int offset, bool littleEndian)
        {
            if (buffer == null)
                throw new ArgumentNullException(nameof(buffer));
            if (offset < 0 || offset + sizeof(UInt16) > buffer.Length)
                throw new ArgumentOutOfRangeException(nameof(offset));

            if (BitConverter.IsLittleEndian == littleEndian)
            {
                return BitConverter.ToUInt16(buffer, offset);
            }
            else
            {
                byte[] copy = new byte[sizeof(UInt16)];
                Array.Copy(buffer, offset, copy, 0, copy.Length);
                Array.Reverse(copy);
                return BitConverter.ToUInt16(copy, 0);
            }
        }

        /// <summary>
        /// Converts an <see cref="Int32"/> to a byte array.
        /// </summary>
        /// <param name="value">The <see cref="Int32"/> value.</param>
        /// <param name="littleEndian">Should the bytes be ordered in little-endian? If false, big endian is used.</param>
        /// <returns>The four bytes.</returns>
        public static byte[] GetInt32Bytes(Int32 value, bool littleEndian)
        {
            byte[] ret = BitConverter.GetBytes(value);
            if (BitConverter.IsLittleEndian != littleEndian)
                Array.Reverse(ret);

            return ret;
        }

        /// <summary>
        /// Converts an <see cref="UInt32"/> to a byte array.
        /// </summary>
        /// <param name="value">The <see cref="UInt32"/> value.</param>
        /// <param name="littleEndian">Should the bytes be ordered in little-endian? If false, big endian is used.</param>
        /// <returns>The four bytes.</returns>
        public static byte[] GetUInt32Bytes(UInt32 value, bool littleEndian)
        {
            byte[] ret = BitConverter.GetBytes(value);
            if (BitConverter.IsLittleEndian != littleEndian)
                Array.Reverse(ret);

            return ret;
        }

        /// <summary>
        /// Reads an <see cref="Int32"/> from a byte array.
        /// </summary>
        /// <param name="buffer">The buffer containing the value.</param>
        /// <param name="offset">The position in the <paramref name="buffer"/> of the first byte to read.</param>
        /// <param name="littleEndian">Should the bytes be interpreted as little-endian? If false, big endian is used.</param>
        /// <returns>The resulting value.</returns>
        /// <exception cref="ArgumentNullException">Thrown if <paramref name="buffer"/> is null.</exception>
        /// <exception cref="ArgumentOutOfRangeException">Thrown if <paramref name="offset"/> is invalid (accounting for
        /// the size of an <see cref="Int16"/>).</exception>
        public static Int32 ReadInt32(byte[] buffer, int offset, bool littleEndian)
        {
            if (buffer == null)
                throw new ArgumentNullException(nameof(buffer));
            if (offset < 0 || offset + sizeof(Int32) > buffer.Length)
                throw new ArgumentOutOfRangeException(nameof(offset));

            if (BitConverter.IsLittleEndian == littleEndian)
            {
                return BitConverter.ToInt32(buffer, offset);
            }
            else
            {
                byte[] copy = new byte[sizeof(Int32)];
                Array.Copy(buffer, offset, copy, 0, copy.Length);
                Array.Reverse(copy);
                return BitConverter.ToInt32(copy, 0);
            }
        }

        /// <summary>
        /// Reads an <see cref="UInt32"/> from a byte array.
        /// </summary>
        /// <param name="buffer">The buffer containing the value.</param>
        /// <param name="offset">The position in the <paramref name="buffer"/> of the first byte to read.</param>
        /// <param name="littleEndian">Should the bytes be interpreted as little-endian? If false, big endian is used.</param>
        /// <returns>The resulting value.</returns>
        /// <exception cref="ArgumentNullException">Thrown if <paramref name="buffer"/> is null.</exception>
        /// <exception cref="ArgumentOutOfRangeException">Thrown if <paramref name="offset"/> is invalid (accounting for
        /// the size of an <see cref="Int16"/>).</exception>
        public static UInt32 ReadUInt32(byte[] buffer, int offset, bool littleEndian)
        {
            if (buffer == null)
                throw new ArgumentNullException(nameof(buffer));
            if (offset < 0 || offset + sizeof(UInt32) > buffer.Length)
                throw new ArgumentOutOfRangeException(nameof(offset));

            if (BitConverter.IsLittleEndian == littleEndian)
            {
                return BitConverter.ToUInt32(buffer, offset);
            }
            else
            {
                byte[] copy = new byte[sizeof(UInt32)];
                Array.Copy(buffer, offset, copy, 0, copy.Length);
                Array.Reverse(copy);
                return BitConverter.ToUInt32(copy, 0);
            }
        }

        /// <summary>
        /// Converts an <see cref="Int64"/> to a byte array.
        /// </summary>
        /// <param name="value">The <see cref="Int64"/> value.</param>
        /// <param name="littleEndian">Should the bytes be ordered in little-endian? If false, big endian is used.</param>
        /// <returns>The eight bytes.</returns>
        public static byte[] GetInt64Bytes(Int64 value, bool littleEndian)
        {
            byte[] ret = BitConverter.GetBytes(value);
            if (BitConverter.IsLittleEndian != littleEndian)
                Array.Reverse(ret);

            return ret;
        }

        /// <summary>
        /// Converts an <see cref="UInt64"/> to a byte array.
        /// </summary>
        /// <param name="value">The <see cref="UInt64"/> value.</param>
        /// <param name="littleEndian">Should the bytes be ordered in little-endian? If false, big endian is used.</param>
        /// <returns>The eight bytes.</returns>
        public static byte[] GetUInt64Bytes(UInt64 value, bool littleEndian)
        {
            byte[] ret = BitConverter.GetBytes(value);
            if (BitConverter.IsLittleEndian != littleEndian)
                Array.Reverse(ret);

            return ret;
        }

        /// <summary>
        /// Reads an <see cref="Int64"/> from a byte array.
        /// </summary>
        /// <param name="buffer">The buffer containing the value.</param>
        /// <param name="offset">The position in the <paramref name="buffer"/> of the first byte to read.</param>
        /// <param name="littleEndian">Should the bytes be interpreted as little-endian? If false, big endian is used.</param>
        /// <returns>The resulting value.</returns>
        /// <exception cref="ArgumentNullException">Thrown if <paramref name="buffer"/> is null.</exception>
        /// <exception cref="ArgumentOutOfRangeException">Thrown if <paramref name="offset"/> is invalid (accounting for
        /// the size of an <see cref="Int16"/>).</exception>
        public static Int64 ReadInt64(byte[] buffer, int offset, bool littleEndian)
        {
            if (buffer == null)
                throw new ArgumentNullException(nameof(buffer));
            if (offset < 0 || offset + sizeof(Int64) > buffer.Length)
                throw new ArgumentOutOfRangeException(nameof(offset));

            if (BitConverter.IsLittleEndian == littleEndian)
            {
                return BitConverter.ToInt64(buffer, offset);
            }
            else
            {
                byte[] copy = new byte[sizeof(Int64)];
                Array.Copy(buffer, offset, copy, 0, copy.Length);
                Array.Reverse(copy);
                return BitConverter.ToInt64(copy, 0);
            }
        }

        /// <summary>
        /// Reads an <see cref="UInt64"/> from a byte array.
        /// </summary>
        /// <param name="buffer">The buffer containing the value.</param>
        /// <param name="offset">The position in the <paramref name="buffer"/> of the first byte to read.</param>
        /// <param name="littleEndian">Should the bytes be interpreted as little-endian? If false, big endian is used.</param>
        /// <returns>The resulting value.</returns>
        /// <exception cref="ArgumentNullException">Thrown if <paramref name="buffer"/> is null.</exception>
        /// <exception cref="ArgumentOutOfRangeException">Thrown if <paramref name="offset"/> is invalid (accounting for
        /// the size of an <see cref="Int16"/>).</exception>
        public static UInt64 ReadUInt64(byte[] buffer, int offset, bool littleEndian)
        {
            if (buffer == null)
                throw new ArgumentNullException(nameof(buffer));
            if (offset < 0 || offset + sizeof(UInt64) > buffer.Length)
                throw new ArgumentOutOfRangeException(nameof(offset));

            if (BitConverter.IsLittleEndian == littleEndian)
            {
                return BitConverter.ToUInt64(buffer, offset);
            }
            else
            {
                byte[] copy = new byte[sizeof(UInt64)];
                Array.Copy(buffer, offset, copy, 0, copy.Length);
                Array.Reverse(copy);
                return BitConverter.ToUInt64(copy, 0);
            }
        }

        /// <summary>
        /// Converts a <see cref="Single"/> to a byte array.
        /// </summary>
        /// <param name="value">The <see cref="Single"/> value.</param>
        /// <param name="littleEndian">Should the bytes be ordered in little-endian? If false, big endian is used.</param>
        /// <returns>The four bytes.</returns>
        public static byte[] GetSingleBytes(Single value, bool littleEndian)
        {
            byte[] ret = BitConverter.GetBytes(value);
            if (BitConverter.IsLittleEndian != littleEndian)
                Array.Reverse(ret);

            return ret;
        }

        /// <summary>
        /// Reads a <see cref="Single"/> from a byte array.
        /// </summary>
        /// <param name="buffer">The buffer containing the value.</param>
        /// <param name="offset">The position in the <paramref name="buffer"/> of the first byte to read.</param>
        /// <param name="littleEndian">Should the bytes be interpreted as little-endian? If false, big endian is used.</param>
        /// <returns>The resulting value.</returns>
        /// <exception cref="ArgumentNullException">Thrown if <paramref name="buffer"/> is null.</exception>
        /// <exception cref="ArgumentOutOfRangeException">Thrown if <paramref name="offset"/> is invalid (accounting for
        /// the size of an <see cref="Int16"/>).</exception>
        public static Single ReadSingle(byte[] buffer, int offset, bool littleEndian)
        {
            if (buffer == null)
                throw new ArgumentNullException(nameof(buffer));
            if (offset < 0 || offset + sizeof(Single) > buffer.Length)
                throw new ArgumentOutOfRangeException(nameof(offset));

            if (BitConverter.IsLittleEndian == littleEndian)
            {
                return BitConverter.ToSingle(buffer, offset);
            }
            else
            {
                byte[] copy = new byte[sizeof(Single)];
                Array.Copy(buffer, offset, copy, 0, copy.Length);
                Array.Reverse(copy);
                return BitConverter.ToSingle(copy, 0);
            }
        }

        /// <summary>
        /// Converts a <see cref="Double"/> to a byte array.
        /// </summary>
        /// <param name="value">The <see cref="Double"/> value.</param>
        /// <param name="littleEndian">Should the bytes be ordered in little-endian? If false, big endian is used.</param>
        /// <returns>The eight bytes.</returns>
        public static byte[] GetDoubleBytes(Double value, bool littleEndian)
        {
            byte[] ret = BitConverter.GetBytes(value);
            if (BitConverter.IsLittleEndian != littleEndian)
                Array.Reverse(ret);

            return ret;
        }

        /// <summary>
        /// Reads a <see cref="Double"/> from a byte array.
        /// </summary>
        /// <param name="buffer">The buffer containing the value.</param>
        /// <param name="offset">The position in the <paramref name="buffer"/> of the first byte to read.</param>
        /// <param name="littleEndian">Should the bytes be interpreted as little-endian? If false, big endian is used.</param>
        /// <returns>The resulting value.</returns>
        /// <exception cref="ArgumentNullException">Thrown if <paramref name="buffer"/> is null.</exception>
        /// <exception cref="ArgumentOutOfRangeException">Thrown if <paramref name="offset"/> is invalid (accounting for
        /// the size of an <see cref="Int16"/>).</exception>
        public static Double ReadDouble(byte[] buffer, int offset, bool littleEndian)
        {
            if (buffer == null)
                throw new ArgumentNullException(nameof(buffer));
            if (offset < 0 || offset + sizeof(Double) > buffer.Length)
                throw new ArgumentOutOfRangeException(nameof(offset));

            if (BitConverter.IsLittleEndian == littleEndian)
            {
                return BitConverter.ToDouble(buffer, offset);
            }
            else
            {
                byte[] copy = new byte[sizeof(Double)];
                Array.Copy(buffer, offset, copy, 0, copy.Length);
                Array.Reverse(copy);
                return BitConverter.ToDouble(copy, 0);
            }
        }
    }
}
