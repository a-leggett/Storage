using System;

namespace Storage.Data
{
    internal class StorageDictionaryBTree<TKey, TValue> : BTree<TKey, TValue> where TKey : IComparable<TKey>
    {
        public StorageDictionary<TKey, TValue> StorageDictionary { get; private set; }

        public long PageIndex
        {
            get
            {
                return StorageDictionary.PageIndex;
            }
        }

        private const long HeaderCountPosition = 0;
        private const long HeaderRootPageIndexPosition = HeaderCountPosition + sizeof(Int64);
        private const long HeaderSize = HeaderRootPageIndexPosition + sizeof(Int64);
        private const bool StoreAsLittleEndian = StreamingPageStorage.StoreAsLittleEndian;

        public void InitializeEmpty()
        {
            Count = 0;
            RootPageIndex = null;
        }

        private long? cachedCount_ = null;
        public  override long Count
        {
            get
            {
                if(!cachedCount_.HasValue)
                {
                    byte[] buffer = new byte[sizeof(Int64)];
                    PageStorage.ReadFrom(PageIndex, HeaderCountPosition, buffer, 0, buffer.Length);
                    cachedCount_ = Binary.ReadInt64(buffer, 0, StoreAsLittleEndian);
                    if (cachedCount_ < 0)
                        throw new CorruptDataException("The stored 'Count' field represents a negative integer.");
                }

                return cachedCount_.Value;
            }

            protected internal set
            {
                if (value < 0)
                    throw new ArgumentOutOfRangeException(nameof(value));

                cachedCount_ = value;
                byte[] buffer = Binary.GetInt64Bytes(value, StoreAsLittleEndian);
                PageStorage.WriteTo(PageIndex, HeaderCountPosition, buffer, 0, buffer.Length);
            }
        }

        private long? cachedRootPageIndex_ = null;
        private bool hasCachedRootPageIndex_ = false;
        protected internal override long? RootPageIndex
        {
            get
            {
                if(!hasCachedRootPageIndex_)
                {
                    byte[] buffer = new byte[sizeof(Int64)];
                    PageStorage.ReadFrom(PageIndex, HeaderRootPageIndexPosition, buffer, 0, buffer.Length);
                    long? got = Binary.ReadInt64(buffer, 0, StoreAsLittleEndian);
                    if (got == -1)//-1 indicates null
                        got = null;

                    cachedRootPageIndex_ = got;
                    hasCachedRootPageIndex_ = true;
                }

                return cachedRootPageIndex_;
            }

            set
            {
                cachedRootPageIndex_ = value;
                hasCachedRootPageIndex_ = true;

                long toWrite = value ?? -1;//-1 indicates null
                byte[] buffer = Binary.GetInt64Bytes(toWrite, StoreAsLittleEndian);
                PageStorage.WriteTo(PageIndex, HeaderRootPageIndexPosition, buffer, 0, buffer.Length);
            }
        }

        public static long GetAuxDataSize(long pageSize)
        {
            return pageSize - HeaderSize;
        }

        public long AuxDataSize { get { return GetAuxDataSize(PageStorage.PageSize); } }

        public void ReadAuxData(long srcOffset, byte[] buffer, long dstOffset, long length)
        {
            if (buffer == null)
                throw new ArgumentNullException(nameof(buffer));
            if (srcOffset < 0)
                throw new ArgumentOutOfRangeException(nameof(srcOffset));
            if (dstOffset < 0)
                throw new ArgumentOutOfRangeException(nameof(dstOffset));
            if (length < 0)
                throw new ArgumentOutOfRangeException(nameof(length));
            if (srcOffset + length > AuxDataSize)
                throw new ArgumentOutOfRangeException(nameof(srcOffset), "The sum of the source offset and length cannot be greater than " + nameof(AuxDataSize) + ".");
            if (dstOffset + length > buffer.Length)
                throw new ArgumentOutOfRangeException(nameof(dstOffset), "The sum of the destination offset and length cannot be greater than the size of the destination buffer.");

            PageStorage.ReadFrom(PageIndex, srcOffset + HeaderSize, buffer, dstOffset, length);
        }

        public void WriteAuxData(long dstOffset, byte[] buffer, long srcOffset, long length)
        {
            if (buffer == null)
                throw new ArgumentNullException(nameof(buffer));
            if (srcOffset < 0)
                throw new ArgumentOutOfRangeException(nameof(srcOffset));
            if (dstOffset < 0)
                throw new ArgumentOutOfRangeException(nameof(dstOffset));
            if (length < 0)
                throw new ArgumentOutOfRangeException(nameof(length));
            if (srcOffset + length > buffer.Length)
                throw new ArgumentOutOfRangeException(nameof(srcOffset), "The sum of the source offset and length cannot be greater than the size of the source buffer.");
            if (dstOffset + length > AuxDataSize)
                throw new ArgumentOutOfRangeException(nameof(dstOffset), "The sum of the destination offset and length cannot be greater than " + nameof(AuxDataSize) + ".");
            if (IsReadOnly)
                throw new InvalidOperationException("Cannot write auxiliary data to a read-only " + nameof(StorageDictionaryBTree<TKey, TValue>) + ".");

            PageStorage.WriteTo(PageIndex, dstOffset + HeaderSize, buffer, srcOffset, length);
        }

        public StorageDictionaryBTree(StorageDictionary<TKey, TValue> dictionary) : base(dictionary.CachedPageStorage, dictionary.KeySerializer, dictionary.ValueSerializer, dictionary.MaxMovePairCount)
        {
            this.StorageDictionary = dictionary ?? throw new ArgumentNullException(nameof(dictionary));
        }
    }
}
