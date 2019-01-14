using Storage;
using System;
using System.IO;
using System.Threading;

namespace StorageTest.Mocks
{
    public class MockPageStorage : IPageStorage
    {
        private StreamingPageStorage streamingPageStorage;

        public MockPageStorage(Stream baseStream, long pageSize, long initialCapacity, bool isReadOnly, bool isCapacityFixed)
        {
            StreamingPageStorage.Create(baseStream, pageSize, initialCapacity, null, new CancellationToken(false), true).Dispose();
            streamingPageStorage = StreamingPageStorage.Load(baseStream, isReadOnly, isCapacityFixed, false);
        }

        public bool IsReadOnly { get { return streamingPageStorage.IsReadOnly; } }

        public bool IsCapacityFixed { get { return streamingPageStorage.IsCapacityFixed; } }

        public long PageCapacity { get { return streamingPageStorage.PageCapacity; } }

        public long AllocatedPageCount { get { return streamingPageStorage.AllocatedPageCount; } }

        public long PageSize { get { return streamingPageStorage.PageSize; } }

        public long? EntryPageIndex
        {
            get
            {
                return streamingPageStorage.EntryPageIndex;
            }

            set
            {
                streamingPageStorage.EntryPageIndex = value;
            }
        }

        public event Action<MockPageStorage, long> OnFreePage;

        public bool FreePage(long index)
        {
            OnFreePage?.Invoke(this, index);
            return streamingPageStorage.FreePage(index);
        }

        public event Action<MockPageStorage, long> OnIsPageAllocated;

        public bool IsPageAllocated(long index)
        {
            OnIsPageAllocated?.Invoke(this, index);
            return streamingPageStorage.IsPageAllocated(index);
        }

        public bool IsPageOnStorage(long index)
        {
            return streamingPageStorage.IsPageOnStorage(index);
        }

        public event Action<MockPageStorage, long, long, byte[], long, long> OnRead;

        public void ReadFrom(long pageIndex, long srcOffset, byte[] buffer, long dstOffset, long length)
        {
            OnRead?.Invoke(this, pageIndex, srcOffset, buffer, dstOffset, length);
            streamingPageStorage.ReadFrom(pageIndex, srcOffset, buffer, dstOffset, length);
        }

        public event Action<MockPageStorage> OnTryAllocatePage;

        public bool TryAllocatePage(out long index)
        {
            OnTryAllocatePage?.Invoke(this);
            return streamingPageStorage.TryAllocatePage(out index);
        }

        public event Action<MockPageStorage, long> OnTryDeflate;

        public long TryDeflate(long removePageCount, IProgress<ProgressReport> progressReporter, CancellationToken cancellationToken)
        {
            OnTryDeflate?.Invoke(this, removePageCount);
            return streamingPageStorage.TryDeflate(removePageCount, progressReporter, cancellationToken);
        }

        public event Action<MockPageStorage, long> OnTryInflate;

        public long TryInflate(long additionalPageCount, IProgress<ProgressReport> progressReporter, CancellationToken cancellationToken)
        {
            OnTryInflate?.Invoke(this, additionalPageCount);
            return streamingPageStorage.TryInflate(additionalPageCount, progressReporter, cancellationToken);
        }

        public event Action<MockPageStorage, long, long, byte[], long, long> OnWriteTo;

        public void WriteTo(long pageIndex, long dstOffset, byte[] buffer, long srcOffset, long length)
        {
            OnWriteTo?.Invoke(this, pageIndex, dstOffset, buffer, srcOffset, length);
            streamingPageStorage.WriteTo(pageIndex, dstOffset, buffer, srcOffset, length);
        }

        public event Action<MockPageStorage> OnDispose;

        public void Dispose()
        {
            OnDispose?.Invoke(this);
        }
    }
}
