using Storage;
using Storage.Data;
using Storage.Data.Serializers;
using System;

namespace StorageTest.Mocks
{
    class MockBTree : BTree<long, string>
    {
        public MockBTree(IPageStorage pageStorage, ISerializer<long> keySerializer, ISerializer<string> valueSerializer, long maxMovePairCount = 32) : base(pageStorage, keySerializer, valueSerializer, maxMovePairCount)
        {
            if (pageStorage.EntryPageIndex == null)
            {
                if (pageStorage.TryAllocatePage(out long index))
                {
                    pageStorage.EntryPageIndex = index;
                    this.Count = 0;
                    this.RootPageIndex = null;
                }
                else
                {
                    throw new ArgumentException("Failed to allocate the entry page index on the " + nameof(IPageStorage) + ".", nameof(pageStorage));
                }
            }
        }

        public override long Count
        {
            get
            {
                byte[] buffer = new byte[sizeof(Int64)];
                base.PageStorage.ReadFrom(base.PageStorage.EntryPageIndex.Value, 0, buffer, 0, buffer.Length);
                return Binary.ReadInt64(buffer, 0, true);
            }

            internal protected set
            {
                byte[] buffer = Binary.GetInt64Bytes(value, true);
                base.PageStorage.WriteTo(base.PageStorage.EntryPageIndex.Value, 0, buffer, 0, buffer.Length);
            }
        }

        protected internal override long? RootPageIndex
        {
            get
            {
                byte[] buffer = new byte[sizeof(Int64)];
                base.PageStorage.ReadFrom(base.PageStorage.EntryPageIndex.Value, sizeof(Int64)/*Skip the 'Count' data*/, buffer, 0, buffer.Length);
                long got = Binary.ReadInt64(buffer, 0, true);
                if (got == -1)
                    return null;
                else
                    return got;
            }

            set
            {
                long put = value ?? -1;
                byte[] buffer = Binary.GetInt64Bytes(put, true);
                base.PageStorage.WriteTo(base.PageStorage.EntryPageIndex.Value, sizeof(Int64), buffer, 0, buffer.Length);
            }
        }
    }
}
