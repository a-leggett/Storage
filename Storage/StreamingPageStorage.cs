using Storage.Data;
using System;
using System.IO;
using System.Threading;

namespace Storage
{
    /// <summary>
    /// <see cref="IPageStorage"/> implementation where the base storage is a <see cref="Stream"/>.
    /// </summary>
    public sealed class StreamingPageStorage : IPageStorage
    {
        /// <summary>
        /// The size that is used to link together 'unallocated' pages.
        /// When a page is unallocated, its payload is used to form a linked
        /// list node of 'unallocated' pages. This means it must store the
        /// index of the previous unallocated page (or -1 if none), and the
        /// index of the next unallocated page (or -1 if none).
        /// </summary>
        private const long UnallocatedPageLinkSize = sizeof(Int64)/*Previous unallocated page index*/ + sizeof(Int64);/*Next unallocated page index*/

        private const long HeaderPageSizePosition = 0;
        private const long HeaderEntryPageIndexPosition = HeaderPageSizePosition + sizeof(Int64);
        private const long HeaderAllocatedPageCountPosition = HeaderEntryPageIndexPosition + sizeof(Int64);
        private const long HeaderFirstUnallocatedIndexPosition = HeaderAllocatedPageCountPosition + sizeof(Int64);
        private const long HeaderLastUnallocatedIndexPosition = HeaderFirstUnallocatedIndexPosition + sizeof(Int64);

        /// <summary>
        /// The size, in bytes, of the header.
        /// </summary>
        public const long HeaderSize = HeaderLastUnallocatedIndexPosition + sizeof(Int64);

        /// <summary>
        /// The minimum page payload size, measured in bytes.
        /// </summary>
        public const long MinPageSize = UnallocatedPageLinkSize;

        /// <summary>
        /// Gets the required size of a <see cref="Stream"/> to contain a <see cref="StreamingPageStorage"/> with
        /// a particular page size and capacity.
        /// </summary>
        /// <param name="pageSize">The size, in bytes, of each page's payload.</param>
        /// <param name="pageCapacity">The number of pages.</param>
        /// <returns>The required size, measured in bytes.</returns>
        /// <seealso cref="HeaderSize"/>
        public static long GetRequiredStreamSize(long pageSize, long pageCapacity)
        {
            return HeaderSize + ((pageSize + 1/*+1 for the 'is allocated' flag*/) * pageCapacity);
        }

        /// <summary>
        /// Determines the page capacity that can fit in a specific <see cref="Stream"/> size using
        /// a specific page size.
        /// </summary>
        /// <param name="streamSize">The size of the <see cref="Stream"/>, measured in bytes.</param>
        /// <param name="pageSize">The size of each page's payload, measured in bytes.</param>
        /// <returns>The maximum number of pages that can be stored in a <see cref="StreamingPageStorage"/>
        /// based on a <see cref="Stream"/> with the specified <paramref name="streamSize"/>.</returns>
        public static long GetPageCapacityForStreamSize(long streamSize, long pageSize)
        {
            long availableForPages = streamSize - HeaderSize;
            return availableForPages / (1/*Is Allocated flag*/ + pageSize);
        }

        private Stream stream_;
        private readonly object locker = new object();
        internal const bool StoreAsLittleEndian = true;

        private long ReadPageSize()
        {
            lock (locker)
            {
                byte[] buffer = new byte[sizeof(Int64)];
                stream_.Position = HeaderPageSizePosition;
                stream_.Read(buffer, 0, buffer.Length);
                cachedPageSize_ = Binary.ReadInt64(buffer, 0, StoreAsLittleEndian);//Update the cache
                return cachedPageSize_.Value;
            }
        }

        private void WritePageSize(long size)
        {
            if (IsReadOnly)
                throw new InvalidOperationException("Cannot write the 'Page Size' header for a read-only " + nameof(StreamingPageStorage) + ".");

            lock (locker)
            {
                byte[] buffer = Binary.GetInt64Bytes(size, StoreAsLittleEndian);
                stream_.Position = HeaderPageSizePosition;
                stream_.Write(buffer, 0, buffer.Length);
                stream_.Flush();

                //Update the cache
                cachedPageSize_ = size;
            }
        }

        private long? cachedPageSize_ = null;
        /// <summary>
        /// The size, in bytes, of each page's payload.
        /// </summary>
        /// <seealso cref="MinPageSize"/>
        public long PageSize
        {
            get
            {
                lock (locker)
                {
                    if (cachedPageSize_ == null)
                        cachedPageSize_ = ReadPageSize();

                    return cachedPageSize_.Value;
                }
            }
        }

        /// <summary>
        /// Is this <see cref="IPageStorage"/> readonly?
        /// </summary>
        public bool IsReadOnly { get; private set; }

        /// <summary>
        /// Is the <see cref="PageCapacity"/> a fixed constant?
        /// </summary>
        /// <remarks>
        /// This property defines whether the <see cref="PageCapacity"/> is a fixed constant,
        /// meaning it cannot be changed via <see cref="TryInflate(long, IProgress{ProgressReport}, CancellationToken)"/> or 
        /// <see cref="TryDeflate(long, IProgress{ProgressReport}, CancellationToken)"/>. It cannot be changed after the 
        /// <see cref="StreamingPageStorage"/> has been instantiated, however it may change across different instances for the same
        /// base <see cref="Stream"/>.
        /// </remarks>
        /// <seealso cref="IsReadOnly"/>
        public bool IsCapacityFixed { get; private set; }

        private long? ReadEntryPageIndex()
        {
            lock (locker)
            {
                byte[] buffer = new byte[sizeof(Int64)];
                stream_.Position = HeaderEntryPageIndexPosition;
                stream_.Read(buffer, 0, buffer.Length);

                //Update the cache
                long? got = Binary.ReadInt64(buffer, 0, StoreAsLittleEndian);
                if (got == -1)//-1 indicates null
                    got = null;
                cachedEntryPageIndex_ = got;
                hasCachedEntryPageIndex_ = true;

                return cachedEntryPageIndex_;
            }
        }

        private void WriteEntryPageIndex(long? index)
        {
            if (index.HasValue && index.Value < 0)
                throw new ArgumentOutOfRangeException(nameof(index), "The 'Entry Page Index' header cannot be assigned to a negative value.");
            if (IsReadOnly)
                throw new InvalidOperationException("Cannot write the 'Entry Page Index' header for a read-only " + nameof(StreamingPageStorage) + ".");

            lock (locker)
            {
                byte[] buffer = Binary.GetInt64Bytes(index ?? -1/*-1 indicates null*/, StoreAsLittleEndian);
                stream_.Position = HeaderEntryPageIndexPosition;
                stream_.Write(buffer, 0, buffer.Length);
                stream_.Flush();

                //Update the cache
                cachedEntryPageIndex_ = index;
                hasCachedEntryPageIndex_ = true;
            }
        }

        private bool hasCachedEntryPageIndex_ = false;
        private long? cachedEntryPageIndex_;
        /// <summary>
        /// The index of the application-defined 'entry page,' or null.
        /// </summary>
        /// <remarks>
        /// <para>
        /// This property is intended as a utility for an application. Consider that the application
        /// initially does not necessarily know where to start 'finding' data. It is not recommended
        /// that the application simply assumes the 'entry' page is at a constant index such as zero,
        /// since the interface technically allows pages to be allocated in any order. Instead, the
        /// application is expected to store the 'entry page' index in this property. Then, when the
        /// application first initializes, it will open the 'entry page' specified by this property,
        /// and that 'entry page' will contain whatever the application needs for initialization.
        /// </para>
        /// <para>
        /// For an example of how this property is useful, consider that the application is storing a
        /// tree in the storage, where each allocated page is a node. When the application is initialized,
        /// it will need to know where the root node's page is located. The application would use this property
        /// to store the index of the root node's page.
        /// </para>
        /// <para>
        /// This property can be used to store any non-negative value, even if it does not refer to an
        /// allocated page, or null. It is the application's responsibility to avoid assigning an invalid
        /// index.
        /// </para>
        /// </remarks>
        /// <exception cref="ArgumentOutOfRangeException">Thrown when assigning a negative value.</exception>
        /// <exception cref="InvalidOperationException">Thrown when assigning a value when <see cref="IsReadOnly"/>
        /// is true.</exception>
        public long? EntryPageIndex
        {
            get
            {
                lock (locker)
                {
                    if (!hasCachedEntryPageIndex_)
                    {
                        cachedEntryPageIndex_ = ReadEntryPageIndex();
                        hasCachedEntryPageIndex_ = true;
                    }

                    return cachedEntryPageIndex_;
                }
            }

            set
            {
                if (value.HasValue && value.Value < 0)
                    throw new ArgumentOutOfRangeException(nameof(value), "The " + nameof(EntryPageIndex) + " cannot be assigned to a negative value.");
                if (IsReadOnly)
                    throw new InvalidOperationException("Cannot assign the " + nameof(EntryPageIndex) + " of a read-only " + nameof(StreamingPageStorage) + ".");

                lock (locker)
                {
                    WriteEntryPageIndex(value);//This method will update the cache too
                }
            }
        }

        private long? cachedPageCapacity_ = null;
        /// <summary>
        /// The total capacity, measured in the number of pages.
        /// </summary>
        /// <remarks>
        /// <para>
        /// This property refers to the sum of all allocated and unallocated pages. If 
        /// <see cref="IsCapacityFixed"/> is false, then the page capacity can be 
        /// increased via <see cref="TryInflate(long, IProgress{ProgressReport}, CancellationToken)"/>
        /// and decreased via <see cref="TryDeflate(long, IProgress{ProgressReport}, CancellationToken)"/>.
        /// </para>
        /// </remarks>
        /// <seealso cref="IsCapacityFixed"/>
        /// <seealso cref="TryInflate(long, IProgress{ProgressReport}, CancellationToken)"/>
        /// <seealso cref="TryDeflate(long, IProgress{ProgressReport}, CancellationToken)"/>
        public long PageCapacity
        {
            get
            {
                lock (locker)
                {
                    if (cachedPageCapacity_ == null)
                        cachedPageCapacity_ = GetPageCapacityForStreamSize(stream_.Length, PageSize);

                    return cachedPageCapacity_.Value;
                }
            }
        }

        private long ReadAllocatedPageCount()
        {
            lock (locker)
            {
                byte[] buffer = new byte[sizeof(Int64)];
                stream_.Position = HeaderAllocatedPageCountPosition;
                stream_.Read(buffer, 0, buffer.Length);

                cachedAllocatedPageCount = Binary.ReadInt64(buffer, 0, StoreAsLittleEndian);//Update the cache

                return cachedAllocatedPageCount.Value;
            }
        }

        private void WriteAllocatedPageCount(long allocatedPageCount)
        {
            if (IsReadOnly)
                throw new InvalidOperationException("Cannot write the 'Allocated Page Count' header for a read-only " + nameof(StreamingPageStorage) + ".");

            lock (locker)
            {
                byte[] buffer = Binary.GetInt64Bytes(allocatedPageCount, StoreAsLittleEndian);
                stream_.Position = HeaderAllocatedPageCountPosition;
                stream_.Write(buffer, 0, buffer.Length);
                stream_.Flush();

                //Update the cache
                cachedAllocatedPageCount = allocatedPageCount;
            }
        }

        private long? cachedAllocatedPageCount = null;
        /// <summary>
        /// The total number of pages that are currently allocated by the application.
        /// </summary>
        /// <seealso cref="PageCapacity"/>
        /// <seealso cref="TryAllocatePage(out long)"/>
        /// <seealso cref="FreePage(long)"/>
        public long AllocatedPageCount
        {
            get
            {
                lock (locker)
                {
                    if (!cachedAllocatedPageCount.HasValue)
                        cachedAllocatedPageCount = ReadAllocatedPageCount();

                    return cachedAllocatedPageCount.Value;
                }
            }

            private set
            {
                lock (locker)
                {
                    if (value < 0)
                        throw new ArgumentOutOfRangeException(nameof(value));

                    WriteAllocatedPageCount(value);//This method will update cache too
                }
            }
        }

        private long? ReadFirstUnallocatedPageIndex()
        {
            lock (locker)
            {
                byte[] buffer = new byte[sizeof(Int64)];
                stream_.Position = HeaderFirstUnallocatedIndexPosition;
                stream_.Read(buffer, 0, buffer.Length);

                //Update the cache
                long? got = Binary.ReadInt64(buffer, 0, StoreAsLittleEndian);
                if (got == -1)//-1 indicates null
                    got = null;
                cachedFirstUnallocatedPageIndex_ = got;
                hasCachedFirstUnallocatedPageIndex_ = true;

                return cachedFirstUnallocatedPageIndex_;
            }
        }

        private void WriteFirstUnallocatedPageIndex(long? index)
        {
            if (IsReadOnly)
                throw new InvalidOperationException("Cannot write the 'First Unallocated Page Index' header for a read-only " + nameof(StreamingPageStorage) + ".");

            lock (locker)
            {
                byte[] buffer = Binary.GetInt64Bytes(index ?? -1, StoreAsLittleEndian);
                stream_.Position = HeaderFirstUnallocatedIndexPosition;
                stream_.Write(buffer, 0, buffer.Length);
                stream_.Flush();

                //Update the cache
                cachedFirstUnallocatedPageIndex_ = index;
                hasCachedFirstUnallocatedPageIndex_ = true;
            }
        }

        private long? cachedFirstUnallocatedPageIndex_ = null;
        private bool hasCachedFirstUnallocatedPageIndex_ = false;
        internal long? FirstUnallocatedPageIndex
        {
            get
            {
                lock (locker)
                {
                    if (!hasCachedFirstUnallocatedPageIndex_)
                    {
                        cachedFirstUnallocatedPageIndex_ = ReadFirstUnallocatedPageIndex();
                        hasCachedFirstUnallocatedPageIndex_ = true;
                    }

                    return cachedFirstUnallocatedPageIndex_;
                }
            }

            private set
            {
                lock (locker)
                {
                    WriteFirstUnallocatedPageIndex(value);//This method will update the cache too
                }
            }
        }

        private long? ReadLastUnallocatedPageIndex()
        {
            lock (locker)
            {
                byte[] buffer = new byte[sizeof(Int64)];
                stream_.Position = HeaderLastUnallocatedIndexPosition;
                stream_.Read(buffer, 0, buffer.Length);

                //Update the cache
                long? got = Binary.ReadInt64(buffer, 0, StoreAsLittleEndian);
                if (got == -1)//-1 indicates null
                    got = null;
                cachedLastUnallocatedPageIndex_ = got;
                hasCachedLastUnallocatedPageIndex_ = true;

                return cachedLastUnallocatedPageIndex_;
            }
        }

        private void WriteLastUnallocatedPageIndex(long? index)
        {
            if (IsReadOnly)
                throw new InvalidOperationException("Cannot write the 'Last Unallocated Page Index' header for a read-only " + nameof(StreamingPageStorage) + ".");

            lock (locker)
            {
                byte[] buffer = Binary.GetInt64Bytes(index ?? -1, StoreAsLittleEndian);
                stream_.Position = HeaderLastUnallocatedIndexPosition;
                stream_.Write(buffer, 0, buffer.Length);
                stream_.Flush();

                //Update the cache
                cachedLastUnallocatedPageIndex_ = index;
                hasCachedLastUnallocatedPageIndex_ = true;
            }
        }

        private long? cachedLastUnallocatedPageIndex_ = null;
        private bool hasCachedLastUnallocatedPageIndex_ = false;
        internal long? LastUnallocatedPageIndex
        {
            get
            {
                lock (locker)
                {
                    if (!hasCachedLastUnallocatedPageIndex_)
                    {
                        cachedLastUnallocatedPageIndex_ = ReadLastUnallocatedPageIndex();
                        hasCachedLastUnallocatedPageIndex_ = true;
                    }

                    return cachedLastUnallocatedPageIndex_;
                }
            }

            set
            {
                lock (locker)
                {
                    WriteLastUnallocatedPageIndex(value);//This method will update the cache too
                }
            }
        }

        private long? GetNextUnallocatedPageIndex(long fromPageIndex)
        {
            lock (locker)
            {
                if (!IsPageOnStorage(fromPageIndex))
                    throw new ArgumentOutOfRangeException(nameof(fromPageIndex), "The specified index refers to a page which does not exist on the " + nameof(Stream) + ".");
                if (IsPageAllocated(fromPageIndex))
                    throw new InvalidOperationException("Cannot get the 'Next Unallocated Page Index' field on a page that is currently allocated.");

                //When a page is unallocated, we use its payload to store the indices of the previous and next unallocated pages
                byte[] buffer = new byte[sizeof(Int64)];
                stream_.Position = GetPagePayloadPosition(fromPageIndex) + sizeof(Int64);//Skip the 'Previous Un.. Page Index'
                stream_.Read(buffer, 0, buffer.Length);
                long? got = Binary.ReadInt64(buffer, 0, StoreAsLittleEndian);
                if (got == -1)//-1 indicates null
                    got = null;
                return got;
            }
        }

        private void SetNextUnallocatedPageIndex(long fromPageIndex, long? toPageIndex)
        {
            if (IsReadOnly)
                throw new InvalidOperationException("Cannot set the 'Next Unallocated Page Index' field for any page on a read-only " + nameof(StreamingPageStorage) + ".");

            lock (locker)
            {
                if (!IsPageOnStorage(fromPageIndex))
                    throw new ArgumentOutOfRangeException(nameof(fromPageIndex), "The specified 'from' index refers to a page which does not exist on the " + nameof(Stream) + ".");
                if (IsPageAllocated(fromPageIndex))
                    throw new InvalidOperationException("Cannot set the 'Next Unallocated Page Index' field on a page that is currently allocated.");

                //When a page is unallocated, we use its payload to store the indices of the previous and next unallocated pages
                byte[] buffer = Binary.GetInt64Bytes(toPageIndex ?? -1, StoreAsLittleEndian);
                stream_.Position = GetPagePayloadPosition(fromPageIndex) + sizeof(Int64);//Skip the 'Previous Un.. Page Index'
                stream_.Write(buffer, 0, buffer.Length);
                stream_.Flush();
            }
        }

        private long? GetPreviousUnallocatedPageIndex(long fromPageIndex)
        {
            lock (locker)
            {
                if (!IsPageOnStorage(fromPageIndex))
                    throw new ArgumentOutOfRangeException(nameof(fromPageIndex), "The specified index refers to a page which does not exist on the " + nameof(Stream) + ".");
                if (IsPageAllocated(fromPageIndex))
                    throw new InvalidOperationException("Cannot get the 'Previous Unallocated Page Index' field on a page that is currently allocated.");

                //When a page is unallocated, we use its payload to store the indices of the previous and next unallocated pages
                byte[] buffer = new byte[sizeof(Int64)];
                stream_.Position = GetPagePayloadPosition(fromPageIndex);
                stream_.Read(buffer, 0, buffer.Length);
                long? got = Binary.ReadInt64(buffer, 0, StoreAsLittleEndian);
                if (got == -1)//-1 indicates null
                    got = null;
                return got;
            }
        }

        private void SetPreviousUnallocatedPageIndex(long fromPageIndex, long? toPageIndex)
        {
            if (IsReadOnly)
                throw new InvalidOperationException("Cannot set the 'Previous Unallocated Page Index' field for any page on a read-only " + nameof(StreamingPageStorage) + ".");

            lock (locker)
            {
                if (!IsPageOnStorage(fromPageIndex))
                    throw new ArgumentOutOfRangeException(nameof(fromPageIndex), "The specified index refers to a page which does not exist on the " + nameof(Stream) + ".");
                if (IsPageAllocated(fromPageIndex))
                    throw new InvalidOperationException("Cannot set the 'Previous Unallocated Page Index' field on a page that is currently allocated.");

                //When a page is unallocated, we use its payload to store the indices of the previous and next unallocated pages
                byte[] buffer = Binary.GetInt64Bytes(toPageIndex ?? -1, StoreAsLittleEndian);
                stream_.Position = GetPagePayloadPosition(fromPageIndex);
                stream_.Write(buffer, 0, buffer.Length);
                stream_.Flush();
            }
        }

        private bool ReadPageIsAllocatedFlag(long index)
        {
            lock (locker)
            {
                if (!IsPageOnStorage(index))
                    throw new ArgumentOutOfRangeException(nameof(index), "The specified index refers to a page which does not exist on the " + nameof(Stream) + ".");

                byte[] buffer = new byte[1];
                stream_.Position = GetPageIsAllocatedFlagPosition(index);
                stream_.Read(buffer, 0, buffer.Length);

                if (buffer[0] == 0xFF)
                    return true;
                else if (buffer[0] == 0x00)
                    return false;
                else
                    throw new CorruptDataException("Found a page with an invalid 'Is Allocated' flag. Expected either 0x00 or 0xFF, but something else was found instead. This likely indicates corrupt data.");
            }
        }

        private void WritePageIsAllocatedFlag(long index, bool isAllocated)
        {
            if (IsReadOnly)
                throw new InvalidOperationException("Cannot set the 'Is Page Allocated' flag for any page on a read-only " + nameof(StreamingPageStorage) + ".");

            lock (locker)
            {
                if (!IsPageOnStorage(index))
                    throw new ArgumentOutOfRangeException(nameof(index), "The specified index refers to a page which does not exist on the " + nameof(Stream) + ".");

                byte[] buffer = new byte[] { isAllocated ? (byte)0xFF : (byte)0x00 };
                stream_.Position = GetPageIsAllocatedFlagPosition(index);
                stream_.Write(buffer, 0, buffer.Length);
                stream_.Flush();
            }
        }

        private void AddToUnallocatedPageList(long index)
        {
            if (IsReadOnly)
                throw new InvalidOperationException("Cannot add any page to the 'Unallocated Page List' on a read-only " + nameof(StreamingPageStorage) + ".");

            lock (locker)
            {
                var firstUnallocated = FirstUnallocatedPageIndex;
                var lastUnallocated = LastUnallocatedPageIndex;

                if (firstUnallocated == null)
                {
                    if (lastUnallocated != null)
                        throw new CorruptDataException("The 'First Unallocated Page Index' in the header is null (-1), but the 'Last Unallocated Page Index' is not. This indicates corrupt data.");

                    //The list was empty, add the first item
                    FirstUnallocatedPageIndex = index;
                    LastUnallocatedPageIndex = index;

                    //Since this was the first item, the 'previous' and 'next' unallocated pages are null
                    SetPreviousUnallocatedPageIndex(index, null);
                    SetNextUnallocatedPageIndex(index, null);
                }
                else
                {
                    if (lastUnallocated == null)
                        throw new CorruptDataException("The 'Last Unallocated Page Index' in the header is null (-1), but the 'First Unallocated Page Index' is not. This indicates corrupt data.");

                    //Point the former 'unallocated page' to 'index'
                    SetNextUnallocatedPageIndex(lastUnallocated.Value, index);

                    //Point the 'new last unallocated page' to the 'previous last unallocated page'
                    SetPreviousUnallocatedPageIndex(index, lastUnallocated);

                    //Make the 'last unallocated page' be 'index'
                    LastUnallocatedPageIndex = index;
                }

                //Indicate that there is no 'unallocated page' after the one we just added to the list
                SetNextUnallocatedPageIndex(index, null);
            }
        }

        private void RemoveFromUnallocatedPageList(long index)
        {
            if (IsReadOnly)
                throw new InvalidOperationException("Cannot remove any page from the 'Unallocated Page List' on a read-only " + nameof(StreamingPageStorage) + ".");

            lock (locker)
            {
                var firstUnallocated = FirstUnallocatedPageIndex;
                var lastUnallocated = LastUnallocatedPageIndex;
                var nextUnallocated = GetNextUnallocatedPageIndex(index);
                var previousUnallocated = GetPreviousUnallocatedPageIndex(index);

                if (previousUnallocated != null)
                    SetNextUnallocatedPageIndex(previousUnallocated.Value, nextUnallocated);
                else
                    FirstUnallocatedPageIndex = nextUnallocated;

                if (nextUnallocated != null)
                    SetPreviousUnallocatedPageIndex(nextUnallocated.Value, previousUnallocated);
                else
                    LastUnallocatedPageIndex = previousUnallocated;
            }
        }

        /// <summary>
        /// Validates the header, and scans each unallocated page to ensure that the
        /// 'Unallocated Page List' is valid.
        /// </summary>
        /// <param name="progressReporter"><see cref="IProgress{T}"/> that receives a
        /// <see cref="ProgressReport"/> report as progress is made.</param>
        /// <param name="cancellationToken"><see cref="CancellationToken"/> that allows the
        /// application to cancel the validation operation. Upon cancellation, this
        /// method will unblock and no <see cref="Exception"/> will be thrown.</param>
        /// <returns>True if everything was successfully validated, false if
        /// the operation was cancelled via <paramref name="cancellationToken"/>.
        /// False does <em>not</em> indicate corrupt data, corrupt data is only
        /// indicated via an <see cref="Exception"/>.</returns>
        /// <remarks>
        /// <para>
        /// This method may be called by the application to rule out some causes of 
        /// corruption. Note that this may not catch all indicators of corruption, it
        /// primarily focuses on corruption relating to the 'Unallocated Page List'.
        /// The application-defined payload within each page will <em>not</em> be
        /// validated.
        /// </para>
        /// <para>
        /// Note that this method may block for a considerable amount of time,
        /// and all other methods/properties may be forced to wait in the
        /// meantime.
        /// </para>
        /// <para>
        /// Upon success, this method returns true. Upon cancellation, false is returned.
        /// If data corruption is found, any <see cref="Exception"/> may be thrown. Most
        /// corruption will be indicated via a <see cref="CorruptDataException"/>.
        /// </para>
        /// </remarks>
        /// <exception cref="CorruptDataException">Thrown if corrupt data is found. Note that
        /// other <see cref="Exception"/>s may also indicate corrupt data.</exception>
        public bool Validate(IProgress<ProgressReport> progressReporter, CancellationToken cancellationToken)
        {
            lock (locker)
            {
                long unallocatedPageCount = PageCapacity - AllocatedPageCount;
                long totalProgress = 1/*Header Validation*/ + unallocatedPageCount/*Each unallocated page*/;

                //First, validate the header
                ValidateHeader();

                //Report the current progress
                progressReporter?.Report(new ProgressReport(1, totalProgress));

                long? currentPageIndex = FirstUnallocatedPageIndex;
                long? previousPageIndex = null;
                long pageCount = 0;

                //Iterate through the 'Unallocated Page List'
                for (long i = 0; ; i++)
                {
                    if (currentPageIndex != null)
                    {
                        if (!IsPageOnStorage(currentPageIndex.Value))
                            throw new CorruptDataException("One page index in the 'Unallocated Page List' refers to a page which does not exist on the base " + nameof(Stream) + ".");
                        if (IsPageAllocated(currentPageIndex.Value))
                            throw new CorruptDataException("One page in the 'Unallocated Page List' is actually marked as allocated.");
                        if (GetPreviousUnallocatedPageIndex(currentPageIndex.Value) != previousPageIndex)
                            throw new CorruptDataException("One page in the 'Unallocated Page List' has its 'Previous Unallocated Page Index' field that does not match the actual previous unallocated page index.");

                        previousPageIndex = currentPageIndex;
                        currentPageIndex = GetNextUnallocatedPageIndex(currentPageIndex.Value);
                        pageCount++;

                        if (pageCount > unallocatedPageCount)
                            throw new CorruptDataException("The 'Unallocated Page List' seems to contain more pages than are possible. This may indicate that the 'Allocated Page Count' header is corrupt, or that there are duplicate pages in the 'Unallocated Page List'.");

                        //Report the current progress
                        progressReporter?.Report(new ProgressReport(1/*Header*/ + pageCount, totalProgress));

                        //Check if cancellation was requested
                        if (cancellationToken.IsCancellationRequested)
                            return false;
                    }
                    else
                    {
                        break;
                    }
                }

                //Make sure we counted the correct number of unallocated pages
                if (pageCount != unallocatedPageCount)
                    throw new CorruptDataException("The number of pages in the 'Unallocated Page List' does not match what is expected based on the difference between '" + nameof(PageCapacity) + "' and '" + nameof(AllocatedPageCount) + "'.");
            }

            return true;
        }

        private readonly bool leaveStreamOpen_;

        /// <summary>
        /// Loads an existing <see cref="StreamingPageStorage"/> from a <see cref="Stream"/>.
        /// </summary>
        /// <param name="stream">The <see cref="Stream"/> that contains the <see cref="StreamingPageStorage"/> data.
        /// Must contain at least <see cref="HeaderSize"/> bytes, <see cref="Stream.CanRead"/> and
        /// <see cref="Stream.CanSeek"/> must be true, if <paramref name="isReadOnly"/> is false then
        /// <see cref="Stream.CanWrite"/> must also be true. If <paramref name="isCapacityFixed"/> is false,
        /// then this stream must also be resizable (either via <see cref="Stream.SetLength(long)"/> or
        /// <see cref="ISafeResizable.TrySetSize(long)"/>). If it is likely that this <see cref="Stream"/> will
        /// be resized, then it is highly recommended that it inherit the <see cref="ISafeResizable"/> interface.</param>
        /// <param name="isReadOnly">Should the <see cref="StreamingPageStorage"/> be opened in read-only mode? If 
        /// <paramref name="stream"/> is read-only, then this argument must be true.</param>
        /// <param name="isCapacityFixed">Should the <see cref="StreamingPageStorage"/> be opened in fixed-capacity 
        /// mode? If <paramref name="isReadOnly"/> is true, then this argument must also be true.</param>
        /// <param name="leaveStreamOpen">Should the <paramref name="stream"/> remain open (un-disposed) after this
        /// <see cref="StreamingPageStorage"/> is disposed? If false, then when this <see cref="StreamingPageStorage"/>
        /// is disposed, the <paramref name="stream"/>'s <see cref="Stream.Dispose()"/> method will be called.</param>
        /// <returns>The resulting <see cref="StreamingPageStorage"/> instance.</returns>
        /// <exception cref="ArgumentNullException">Thrown if <paramref name="stream"/> is null.</exception>
        /// <exception cref="ArgumentException">Thrown if <paramref name="stream"/> is not readable or
        /// seekable; or if <paramref name="stream"/> has less than <see cref="HeaderSize"/> bytes;
        /// or if <paramref name="stream"/> is read-only while <paramref name="isReadOnly"/> is false;
        /// or if <paramref name="isReadOnly"/> is true while <paramref name="isCapacityFixed"/> is false.</exception>
        /// <exception cref="CorruptDataException">Thrown if corrupt data is found. Note that other
        /// <see cref="Exception"/>s may also be thrown if corrupt data is found.</exception>
        public static StreamingPageStorage Load(Stream stream, bool isReadOnly, bool isCapacityFixed, bool leaveStreamOpen)
        {
            if (stream == null)
                throw new ArgumentNullException(nameof(stream));
            if (!stream.CanRead)
                throw new ArgumentException("The " + nameof(Stream) + " must be readable.", nameof(stream));
            if (!stream.CanSeek)
                throw new ArgumentException("The " + nameof(Stream) + " must be seekable.", nameof(stream));
            if (stream.Length < HeaderSize)
                throw new ArgumentException("The " + nameof(Stream) + " is too small to contain the required header.", nameof(stream));
            if (!stream.CanWrite && !isReadOnly)
                throw new ArgumentException("The " + nameof(Stream) + " is read-only (" + nameof(Stream.CanWrite) + " is false), so the '" + nameof(isReadOnly) + "' argument must be false.");
            if (isReadOnly && !isCapacityFixed)
                throw new ArgumentException("If '" + nameof(isReadOnly) + "' is true, '" + nameof(isCapacityFixed) + "' must also be true.", nameof(isCapacityFixed));

            return new StreamingPageStorage(stream, isReadOnly, isCapacityFixed, true, leaveStreamOpen);
        }

        /// <summary>
        /// Creates a new <see cref="StreamingPageStorage"/> on a <see cref="Stream"/>.
        /// </summary>
        /// <param name="stream">The <see cref="Stream"/> in which to create the new 
        /// <see cref="StreamingPageStorage"/>. Must be readable, resizable (via <see cref="Stream.SetLength(long)"/>, 
        /// writable, and seekable. It is highly recommended that this <see cref="Stream"/> 
        /// inherit the <see cref="ISafeResizable"/> interface, even though it is 
        /// not directly used by this method (it may be used after creation).</param>
        /// <param name="pageSize">The size of each page's payload, measured in bytes. Must be 
        /// at least <see cref="MinPageSize"/> bytes.</param>
        /// <param name="initialCapacity">The initial capacity, measured in the number of pages.</param>
        /// <param name="progressReporter"><see cref="IProgress{T}"/> that receives a 
        /// <see cref="ProgressReport"/> structure as progress is made. May be null, in which 
        /// case progress will not be reported.</param>
        /// <param name="cancellationToken"><see cref="CancellationToken"/> that may be used to cancel 
        /// the creation process.</param>
        /// <param name="leaveStreamOpen">Should the <paramref name="stream"/> remain open (un-disposed)
        /// after this <see cref="StreamingPageStorage"/> is disposed? If false, then when this
        /// <see cref="StreamingPageStorage"/> is disposed, the <paramref name="stream"/>'s
        /// <see cref="Stream.Dispose()"/> method will be called.</param>
        /// <param name="maxResizeIncrement">The maximum increment, measured in bytes, that will be
        /// used to resize the <paramref name="stream"/>. Must be at least one. Using a value that is
        /// too small may result in more frequent <see cref="Stream.SetLength(long)"/> calls, but using
        /// a value that is too large may result in the <see cref="Stream.SetLength(long)"/> call
        /// blocking for a long time, delaying any potential cancellation via the
        /// <paramref name="cancellationToken"/>.</param>
        /// <returns>The resulting <see cref="StreamingPageStorage"/> instance.</returns>
        /// <remarks>
        /// <para>
        /// This method will create a new <see cref="StreamingPageStorage"/> instance with
        /// a <see cref="IPageStorage.PageCapacity"/> of <paramref name="initialCapacity"/>, all of which
        /// will be unallocated pages. The <see cref="IPageStorage.EntryPageIndex"/> will be initialized
        /// to null. Some data in the <paramref name="stream"/> will be overwritten, but some of it may leak
        /// to freshly allocated pages.
        /// </para>
        /// <para>
        /// If the <paramref name="stream"/> is not exactly the required size (see <see cref="GetRequiredStreamSize(long, long)"/>),
        /// then it will be incrementally resized. The increment is determined by the <paramref name="maxResizeIncrement"/>
        /// argument (which is measured as an absolute value).
        /// </para>
        /// <para>
        /// Upon cancellation, an <see cref="OperationCanceledException"/> will be thrown and the application
        /// must assume that everything that has been written to the <paramref name="stream"/> is undefined data.
        /// </para>
        /// </remarks>
        /// <exception cref="ArgumentNullException">Thrown if <paramref name="stream"/> is null.</exception>
        /// <exception cref="ArgumentException">Thrown if <paramref name="stream"/>'s <see cref="Stream.CanRead"/>,
        /// <see cref="Stream.CanWrite"/>, or <see cref="Stream.CanSeek"/> property is false.</exception>
        /// <exception cref="ArgumentOutOfRangeException">Thrown if <paramref name="pageSize"/> is less than
        /// <see cref="MinPageSize"/>, or if <paramref name="initialCapacity"/> is less than zero, or if
        /// <paramref name="maxResizeIncrement"/> is less than one.</exception>
        /// <exception cref="OperationCanceledException">Thrown if the operation was cancelled via the
        /// <paramref name="cancellationToken"/>.</exception>
        public static StreamingPageStorage Create(Stream stream, long pageSize, long initialCapacity, IProgress<ProgressReport> progressReporter, CancellationToken cancellationToken, bool leaveStreamOpen, long maxResizeIncrement = 1024 * 1024 * 64)
        {
            if (stream == null)
                throw new ArgumentNullException(nameof(stream));
            if (!stream.CanRead)
                throw new ArgumentException("The " + nameof(Stream) + " must be readable.", nameof(stream));
            if (!stream.CanWrite)
                throw new ArgumentException("The " + nameof(Stream) + " must be writable.", nameof(stream));
            if (!stream.CanSeek)
                throw new ArgumentException("The " + nameof(Stream) + " must be seekable.", nameof(stream));
            if (pageSize < MinPageSize)
                throw new ArgumentOutOfRangeException(nameof(pageSize), "The page size cannot be less than " + nameof(MinPageSize) + ".");
            if (initialCapacity < 0)
                throw new ArgumentOutOfRangeException(nameof(initialCapacity), "The initial capacity cannot be less than zero.");
            if (maxResizeIncrement < 1)
                throw new ArgumentOutOfRangeException(nameof(maxResizeIncrement), "The maximum resize increment cannot be less than one.");

            long progressTarget = -1;
            long progress = 0;

            long requiredSize = GetRequiredStreamSize(pageSize, initialCapacity);
            long totalSizeDeltaAbs = requiredSize - stream.Length;
            if (totalSizeDeltaAbs < 0)
                totalSizeDeltaAbs = -totalSizeDeltaAbs;

            long resizeCount = totalSizeDeltaAbs / maxResizeIncrement;
            if (totalSizeDeltaAbs % maxResizeIncrement != 0)
                resizeCount++;

            //Determine how many 'progress update' units we will be reporting
            progressTarget = resizeCount + GetCreateProgressTarget(initialCapacity);

            //Report the current progress
            progressReporter?.Report(new ProgressReport(progress, progressTarget));

            //Bring the stream to the required size (incrementally, to allow cancellation in case SetLength blocks for too long)
            while (stream.Length != requiredSize)
            {
                long delta = requiredSize - stream.Length;
                if (delta > maxResizeIncrement)
                    delta = maxResizeIncrement;
                else if (delta < -maxResizeIncrement)
                    delta = -maxResizeIncrement;

                //We don't care if stream inherits the ISafeResizable interface here, since we need the resize to be
                //successful, otherwise we consider the entire creation process to have failed.
                stream.SetLength(stream.Length + delta);
                progress++;

                //Report the current progress
                progressReporter?.Report(new ProgressReport(progress, progressTarget));

                //Check if cancel was requested
                cancellationToken.ThrowIfCancellationRequested();
            }

            //Create the PageStorageProgressReporter to track progress updates as the 'Create' method is called
            ProgressReporter inlineReporter = new ProgressReporter((x) => {
                //Let the application receive the updated progress
                progressReporter?.Report(new ProgressReport(resizeCount + x.Current, (x.Target != null) ? (resizeCount + x.Target) : null));
            });

            return Create(stream, pageSize, false, inlineReporter, cancellationToken, leaveStreamOpen);
        }

        /// <summary>
        /// Creates a fixed-capacity <see cref="StreamingPageStorage"/> on a <see cref="Stream"/>.
        /// </summary>
        /// <param name="stream">The <see cref="Stream"/> in which to create the new
        /// <see cref="StreamingPageStorage"/>. Must be readable, writable, and seekable, but not
        /// necessarily resizable (<see cref="Stream.SetLength(long)"/> will <em>not</em> be called).</param>
        /// <param name="pageSize">The size, in bytes, of each page's payload. Must be at lease
        /// <see cref="MinPageSize"/> bytes.</param>
        /// <param name="progressReporter"><see cref="IProgress{T}"/> that receives a 
        /// <see cref="ProgressReport"/> structure as progress is made. May be null, in which 
        /// case progress will not be reported.</param>
        /// <param name="cancellationToken"><see cref="CancellationToken"/> that may be used to cancel 
        /// the creation process.</param>
        /// <param name="leaveStreamOpen">Should the <paramref name="stream"/> remain open (un-disposed)
        /// after this <see cref="StreamingPageStorage"/> is disposed? If false, then when this
        /// <see cref="StreamingPageStorage"/> is disposed, the <paramref name="stream"/>'s
        /// <see cref="Stream.Dispose()"/> method will be called.</param>
        /// <returns>The resulting <see cref="StreamingPageStorage"/> instance.</returns>
        /// <remarks>
        /// <para>
        /// This method will create a new <see cref="StreamingPageStorage"/> instance with
        /// a <see cref="PageCapacity"/> based on the size of the <paramref name="stream"/> (see
        /// <see cref="GetPageCapacityForStreamSize(long, long)"/>), all of which will be unallocated pages. 
        /// The <see cref="IPageStorage.EntryPageIndex"/> will be initialized to null. Some data in the 
        /// <paramref name="stream"/> will be overwritten, but some of it may leak to freshly allocated 
        /// pages.
        /// </para>
        /// <para>
        /// The resulting <see cref="StreamingPageStorage"/> will be fixed-capacity (<see cref="IsCapacityFixed"/> will
        /// be true). Since the <see cref="PageCapacity"/> will depend on the size of the <paramref name="stream"/>,
        /// and this method will not resize the <paramref name="stream"/>, it is the application's 
        /// responsibility to ensure that the <paramref name="stream"/> is large enough to be useful. 
        /// If it has less than <see cref="HeaderSize"/> bytes, this method will not be able to write a 
        /// valid header, so an <see cref="ArgumentException"/> will be thrown. If the <paramref name="stream"/>
        /// has enough bytes for the header, but not enough for even one full page, then <see cref="PageCapacity"/> 
        /// will be zero. This would not cause any problems, but would render the instance to be practically useless 
        /// to the application.
        /// </para>
        /// <para>
        /// Upon cancellation, an <see cref="OperationCanceledException"/> will be thrown and the application
        /// must assume that everything that has been written to the <paramref name="stream"/> is undefined data.
        /// </para>
        /// </remarks>
        /// <exception cref="ArgumentNullException">Thrown if <paramref name="stream"/> is null.</exception>
        /// <exception cref="ArgumentException">Thrown if the <paramref name="stream"/>'s <see cref="Stream.CanRead"/>,
        /// <see cref="Stream.CanWrite"/>, or <see cref="Stream.CanSeek"/> property is false, or if its 
        /// <see cref="Stream.Length"/> is less than <see cref="HeaderSize"/>.</exception>
        /// <exception cref="ArgumentOutOfRangeException">Thrown if <paramref name="pageSize"/> is less
        /// than <see cref="MinPageSize"/>.</exception>
        /// <exception cref="OperationCanceledException">Thrown if the operation is cancelled via
        /// the <paramref name="cancellationToken"/>.</exception>
        public static StreamingPageStorage CreateFixed(Stream stream, long pageSize, IProgress<ProgressReport> progressReporter, CancellationToken cancellationToken, bool leaveStreamOpen)
        {
            if (stream == null)
                throw new ArgumentNullException(nameof(stream));
            if (!stream.CanRead)
                throw new ArgumentException("The stream must be readable.", nameof(stream));
            if (!stream.CanWrite)
                throw new ArgumentException("The stream must be writable.", nameof(stream));
            if (!stream.CanSeek)
                throw new ArgumentException("The stream must be seekable.", nameof(stream));
            if (pageSize < MinPageSize)
                throw new ArgumentOutOfRangeException(nameof(pageSize), "The page size cannot be less than " + nameof(MinPageSize) + ".");
            if (stream.Length < HeaderSize)
                throw new ArgumentException("The " + nameof(Stream) + " is too small to write the required header. Must have at least " + nameof(HeaderSize) + " bytes.", nameof(stream));

            return Create(stream, pageSize, true, progressReporter, cancellationToken, leaveStreamOpen);
        }

        private static long GetCreateProgressTarget(long pageCapacity)
        {
            return pageCapacity + 1;//1 for the 'write header' call, then 1 per page
        }

        private static StreamingPageStorage Create(Stream stream, long pageSize, bool isFixedSize, IProgress<ProgressReport> progressReporter, CancellationToken cancellationToken, bool leaveStreamOpen)
        {
            if (stream == null)
                throw new ArgumentNullException(nameof(stream));
            if (!stream.CanRead)
                throw new ArgumentException("The stream must be readable.", nameof(stream));
            if (!stream.CanWrite)
                throw new ArgumentException("The stream must be writable.", nameof(stream));
            if (!stream.CanSeek)
                throw new ArgumentException("The stream must be seekable.", nameof(stream));
            if (pageSize < MinPageSize)
                throw new ArgumentOutOfRangeException(nameof(pageSize), "The page size cannot be less than " + nameof(MinPageSize) + ".");
            if (stream.Length < HeaderSize)
                throw new ArgumentException("The specified " + nameof(Stream) + " is to small to write the required header data. It must be at least " + nameof(HeaderSize) + " bytes.");

            long pageCapacity = GetPageCapacityForStreamSize(stream.Length, pageSize);

            StreamingPageStorage ret = new StreamingPageStorage(stream, false, isFixedSize, false/*Can't validate the header, it hasn't been written yet*/, leaveStreamOpen);

            long progressTarget = GetCreateProgressTarget(pageCapacity);
            long progress = 0;

            //Report the current progress
            progressReporter?.Report(new ProgressReport(progress, progressTarget));

            //Initialize the header
            ret.WriteFullHeader(pageSize, 0, null, null, null);
            progress++;

            //Report the progress after having written the header
            progressReporter?.Report(new ProgressReport(progress, progressTarget));

            //Check if cancel was requested
            cancellationToken.ThrowIfCancellationRequested();

            //Initialize each unallocated page
            for (int i = 0; i < pageCapacity; i++)
            {
                ret.WritePageIsAllocatedFlag(i, false);
                ret.AddToUnallocatedPageList(i);
                progress++;

                //Report the current progress
                progressReporter?.Report(new ProgressReport(i + 1/*Header report*/ + 1/*index+1=count*/, pageCapacity));

                //Check if cancel was requested
                cancellationToken.ThrowIfCancellationRequested();
            }

            return ret;
        }

        private void WriteFullHeader(long pageSize, long allocatedPageCount, long? entryPageIndex, long? firstUnallocatedPageIndex, long? lastUnallocatedPageIndex)
        {
            if (IsReadOnly)
                throw new InvalidOperationException("Cannot write the header to a read-only " + nameof(StreamingPageStorage) + ".");

            lock (locker)
            {
                WritePageSize(pageSize);
                WriteAllocatedPageCount(allocatedPageCount);
                WriteEntryPageIndex(entryPageIndex);
                WriteFirstUnallocatedPageIndex(firstUnallocatedPageIndex);
                WriteLastUnallocatedPageIndex(lastUnallocatedPageIndex);
            }
        }

        private StreamingPageStorage(Stream stream, bool isReadOnly, bool isCapacityFixed, bool validateHeader, bool leaveStreamOpen)
        {
            this.stream_ = stream ?? throw new ArgumentNullException(nameof(stream));
            if (stream.Length < HeaderSize)
                throw new ArgumentException("The " + nameof(Stream) + " is too small to contain a valid header. It must have at least " + nameof(HeaderSize) + " bytes.", nameof(stream));
            if (!stream.CanWrite && !isReadOnly)//TODO: [BugFix: This was !IsReadOnly (the property, not arg, thanks IntelliSense for suggesting a Property before it is useful!)], which was wrong. But why didn't any tests catch it? Make sure future tests handle such typos!
                throw new ArgumentException("If the " + nameof(Stream) + " is read-only (" + nameof(Stream.CanWrite) + " is false), then the '" + nameof(isReadOnly) + "' argument must be true.", nameof(isReadOnly));
            if (isReadOnly && !isCapacityFixed)
                throw new ArgumentException("If the '" + nameof(isReadOnly) + "' argument is true, then '" + nameof(isCapacityFixed) + "' must also be true.", nameof(isCapacityFixed));

            this.IsReadOnly = isReadOnly;
            this.IsCapacityFixed = isCapacityFixed;
            this.leaveStreamOpen_ = leaveStreamOpen;

            if (validateHeader)
                ValidateHeader();
        }

        private void ValidateHeader()
        {
            if (this.PageSize < MinPageSize)
                throw new CorruptDataException("The data within the 'Page Size' portion of the header is invalid, it specifies a size less than " + nameof(MinPageSize) + ".");
            if (this.AllocatedPageCount < 0)
                throw new CorruptDataException("The data within the 'Allocated Page Count' portion of the header is invalid, it specifies a negative value.");
            if (this.EntryPageIndex.HasValue && this.EntryPageIndex < 0)
                throw new CorruptDataException("The data within the 'Entry Page Index' portion of the header is invalid, it is negative and not -1 (which indicates null).");
            if (this.FirstUnallocatedPageIndex.HasValue && !IsPageOnStorage(this.FirstUnallocatedPageIndex.Value))
                throw new CorruptDataException("The data within the 'First Unallocated Page Index' portion of the header is invalid, it is non-null (null indicated by -1), and refers to a page that does not exist within the " + nameof(Stream) + ".");
            if (this.FirstUnallocatedPageIndex.HasValue && IsPageAllocated(this.FirstUnallocatedPageIndex.Value))
                throw new CorruptDataException("The data within the 'First Unallocated Page Index' portion of the header is invalid, it refers to a page which is currently marked as allocated.");
            if (this.LastUnallocatedPageIndex.HasValue && !IsPageOnStorage(this.LastUnallocatedPageIndex.Value))
                throw new CorruptDataException("The data within the 'Last Unallocated Page Index' portion of the header is invalid, it is non-null (null indicated by -1), and refers to a page that does not exist within the " + nameof(Stream) + ".");
            if (this.LastUnallocatedPageIndex.HasValue && IsPageAllocated(this.LastUnallocatedPageIndex.Value))
                throw new CorruptDataException("The data within the 'Last Unallocated Page Index' portion of the header is invalid, it refers to a page which is currently marked as allocated.");
        }

        /// <summary>
        /// Attempts to increase the <see cref="PageCapacity"/> by creating
        /// space for more unallocated pages.
        /// </summary>
        /// <param name="additionalPageCount">The desired additional number of unallocated 
        /// pages to create.</param>
        /// <param name="progressReporter"><see cref="IProgress{T}"/> that receives a
        /// <see cref="ProgressReport"/> as the inflation progresses, or upon
        /// completion, cancellation, or failure. May be null, in which case progress
        /// will not be reported.</param>
        /// <param name="cancellationToken"><see cref="CancellationToken"/> that allows
        /// the application to cancel the inflation. Cancellation will cause the inflation
        /// to stop at the nearest 'safe' position to avoid data corruption. This means
        /// that some pages may have already been created upon cancellation, but not 
        /// necessarily as many as were requested. Upon cancellation, this method will
        /// NOT throw any <see cref="Exception"/>, but will instead return the additional
        /// number of pages that have been created, if any, before the operation was
        /// cancelled.</param>
        /// <returns>The actual number of additional unallocated pages that were created. 
        /// This may be less than the desired <paramref name="additionalPageCount"/>, 
        /// and may even be zero.</returns>
        /// <remarks>
        /// <para>
        /// This method will attempt to increase the <see cref="PageCapacity"/> by creating more 
        /// unallocated pages in the base <see cref="Stream"/>. Calling this method will <em>not</em>
        /// affect any currently allocated pages, except potentially if an <see cref="Exception"/> is
        /// thrown, which may indicate data corruption. The capacity is only ever increased by adding
        /// more unallocated pages to the end of the base <see cref="Stream"/>. Pages will be
        /// added progressively, and each addition will cause the base <see cref="Stream"/>
        /// to be resized. If the base <see cref="Stream"/> inherits the
        /// <see cref="ISafeResizable"/> interface (as is preferred), then its payload will 
        /// be resized via the <see cref="ISafeResizable.TrySetSize(long)"/> method. If this 
        /// call fails to resize the base <see cref="Stream"/> to the required size (indicated
        /// by the return value being false), then this method will end early and return the 
        /// number of pages that were added before the resize failure. In this case, resize 
        /// failure does <em>not</em> indicate data corruption, but rather that the maximum limit 
        /// has been reached. Of course, if the <see cref="ISafeResizable.TrySetSize(long)"/> method
        /// does throw an <see cref="Exception"/>, then it will be passed to the caller, and the application
        /// should assume that the payload may have been corrupted. If the base <see cref="Stream"/> does 
        /// <em>not</em> inherit the <see cref="ISafeResizable"/> interface, then the payload of the base 
        /// <see cref="Stream"/> will be resized via the <see cref="Stream.SetLength(long)"/> method. 
        /// Failure with this method is only indicated by it throwing an <see cref="Exception"/>, which will 
        /// be passed to the caller and the application should assume that payload may have been corrupted.
        /// </para>
        /// <para>
        /// The inflation operation may be cancelled by the application via the <paramref name="cancellationToken"/>.
        /// Cancellation will <em>not</em> cause data corruption, instead it will cause the inflation to stop
        /// at the nearest 'safe' position. Upon cancellation, this method will return the number of pages that
        /// have been added.
        /// </para>
        /// <para>
        /// If <see cref="IsCapacityFixed"/> is true, then an <see cref="InvalidOperationException"/> will be
        /// thrown.
        /// </para>
        /// <para>
        /// If <paramref name="additionalPageCount"/> is a negative number, then an 
        /// <see cref="ArgumentOutOfRangeException"/> will be thrown and the storage will remain
        /// unchanged. To decrease the <see cref="PageCapacity"/>, use the 
        /// <see cref="TryDeflate(long, IProgress{ProgressReport}, CancellationToken)"/> method instead.
        /// </para>
        /// <para>
        /// If any <see cref="Exception"/> is thrown, except the two that were described above, then
        /// the application is expected to assume that some data has been corrupted.
        /// </para>
        /// </remarks>
        /// <exception cref="InvalidOperationException">Thrown if <see cref="IsCapacityFixed"/>
        /// is true.</exception>
        /// <exception cref="ArgumentOutOfRangeException">Thrown if <paramref name="additionalPageCount"/>
        /// is less than zero.</exception>
        /// <seealso cref="IsCapacityFixed"/>
        /// <seealso cref="PageCapacity"/>
        /// <seealso cref="TryDeflate(long, IProgress{ProgressReport}, CancellationToken)"/>
        public long TryInflate(long additionalPageCount, IProgress<ProgressReport> progressReporter, CancellationToken cancellationToken)
        {
            if (additionalPageCount < 0)
                throw new ArgumentOutOfRangeException(nameof(additionalPageCount));
            if (IsCapacityFixed)
                throw new InvalidOperationException("Cannot inflate a fixed-capacity " + nameof(StreamingPageStorage) + ".");

            lock (locker)
            {
                long amountAdded = 0;
                //Report the current progress
                progressReporter?.Report(new ProgressReport(amountAdded, additionalPageCount));

                long currentCapacity = PageCapacity;
                for (long i = 0; i < additionalPageCount; i++)
                {
                    //First, resize the stream to the required size
                    long requiredSize = GetRequiredStreamSize(PageSize, currentCapacity + (i + 1));
                    if (stream_ is ISafeResizable safeResizable)
                    {
                        if (!safeResizable.TrySetSize(requiredSize))
                            break;//Failed to resize to required size (but data is not corrupted)
                    }
                    else
                    {
                        stream_.SetLength(requiredSize);
                    }

                    //Update the PageCapacity property cache
                    cachedPageCapacity_ = GetPageCapacityForStreamSize(stream_.Length, PageSize);

                    long currentIndex = currentCapacity + i;

                    //Now mark the new page as empty
                    WritePageIsAllocatedFlag(currentIndex, false);

                    //Add the new page to the 'unallocated page list'
                    AddToUnallocatedPageList(currentIndex);

                    //Successfully added the new page
                    amountAdded++;

                    //Report the current progress
                    progressReporter?.Report(new ProgressReport(amountAdded, additionalPageCount));

                    //Check if the application requested to cancel
                    if (cancellationToken.IsCancellationRequested)
                        break;//Cancellation does not cause error, but rather stops at nearest 'safe' position, which is right here.
                }

                //Report the final progress
                progressReporter?.Report(new ProgressReport(amountAdded, additionalPageCount));

                return amountAdded;
            }
        }

        /// <summary>
        /// Attempts to decrease the <see cref="PageCapacity"/> by removing unallocated pages
        /// at the very end of the <see cref="Stream"/>.
        /// </summary>
        /// <param name="removePageCount">The desired number of unallocated pages to remove.</param>
        /// <param name="progressReporter"><see cref="IProgress{T}"/> that receives a
        /// <see cref="ProgressReport"/> as the deflation progresses, or upon
        /// completion, cancellation, or failure. May be null, in which case progress
        /// will not be reported.</param>
        /// <param name="cancellationToken"><see cref="CancellationToken"/> that allows the
        /// application to cancel the deflation operation. Cancellation will cause the
        /// deflation to stop at the nearest 'safe' position to avoid data corruption. This
        /// means that some pages may have been removed upon cancellation, but not necessarily
        /// as many as were requested. Upon cancellation, this method will NOT throw an
        /// <see cref="Exception"/>, but will instead return the number of pages that have
        /// been removed before cancellation.</param>
        /// <returns>The actual number of unallocated pages that were removed. This may be
        /// less than <paramref name="removePageCount"/>, and may even be zero.</returns>
        /// <remarks>
        /// <para>
        /// This method will attempt to decrease the <see cref="PageCapacity"/> by removing
        /// unallocated pages at the very end of the <see cref="Stream"/>. Calling this method will <em>not</em>
        /// affect any currently allocated pages, except potentially if an <see cref="Exception"/> is thrown,
        /// which may indicate data corruption. The capacity is only ever decreased by
        /// removing unallocated pages from the very end of the storage. Pages will be removed
        /// progressively, and each removal will cause the base <see cref="Stream"/> to be shrunk.
        /// If the base <see cref="Stream"/> inherits the <see cref="ISafeResizable"/> interface (as
        /// is preferred), then its payload will be resized via the <see cref="ISafeResizable.TrySetSize(long)"/>
        /// method. If this call fails (indicated by a false return value), then this method will end
        /// early and return the number of pages that were removed before the resize failure. In this
        /// case, resize failure does <em>not</em> indicate data corruption. Of course, if the
        /// <see cref="ISafeResizable.TrySetSize(long)"/> method does throw an <see cref="Exception"/>, then
        /// it will be passed to the caller, and the application should assume that the payload may have
        /// been corrupted. If the base <see cref="Stream"/> does <em>not</em> inherit the
        /// <see cref="ISafeResizable"/> interface, then the payload of the base <see cref="Stream"/> will
        /// be resized via the <see cref="Stream.SetLength(long)"/> method. Failure with this method is 
        /// only indicated by it throwing an <see cref="Exception"/>, which will be passed to the caller 
        /// and the application should assume that payload may have been corrupted.
        /// </para>
        /// <para>
        /// Note that this method will only remove consecutively unallocated pages at the end of the
        /// <see cref="Stream"/>. If there is not a consecutive sequence of <paramref name="removePageCount"/>
        /// unallocated pages at the end, then this method will only remove the number of consecutive unallocated
        /// pages that are at the end. If the very last page is allocated, then this method will return zero and
        /// nothing will change.
        /// </para>
        /// <para>
        /// Upon cancellation via the <paramref name="cancellationToken"/>, this method will
        /// return the number of pages that were removed before the operation was cancelled.
        /// Cancellation will <em>not</em> cause data corruption.
        /// </para>
        /// <para>
        /// If <see cref="IsCapacityFixed"/> is true, then an <see cref="InvalidOperationException"/> will
        /// be thrown.
        /// </para>
        /// <para>
        /// If <paramref name="removePageCount"/> is a negative number, then an 
        /// <see cref="ArgumentOutOfRangeException"/> will be thrown and the storage will remain
        /// unchanged. To increase the <see cref="PageCapacity"/>, use the 
        /// <see cref="TryInflate(long, IProgress{ProgressReport}, CancellationToken)"/>
        /// method instead.
        /// </para>
        /// <para>
        /// If any <see cref="Exception"/> is thrown, except the two that were described above, then
        /// the application is expected to assume that some data has been corrupted.
        /// </para>
        /// </remarks>
        /// <exception cref="InvalidOperationException">Thrown if <see cref="IsCapacityFixed"/>
        /// is true.</exception>
        /// <exception cref="ArgumentOutOfRangeException">Thrown if <paramref name="removePageCount"/>
        /// is less than zero.</exception>
        /// <seealso cref="IsCapacityFixed"/>
        /// <seealso cref="PageCapacity"/>
        /// <seealso cref="TryInflate(long, IProgress{ProgressReport}, CancellationToken)"/>
        public long TryDeflate(long removePageCount, IProgress<ProgressReport> progressReporter, CancellationToken cancellationToken)
        {
            if (removePageCount < 0)
                throw new ArgumentOutOfRangeException(nameof(removePageCount));
            if (IsCapacityFixed)
                throw new InvalidOperationException("Cannot deflate a fixed-capacity " + nameof(StreamingPageStorage) + ".");

            lock (locker)
            {
                long amountRemoved = 0;
                long currentCapacity = PageCapacity;

                //Report the current progress
                progressReporter?.Report(new ProgressReport(amountRemoved, removePageCount));

                //Make sure the capacity even supports 'removePageCount'
                if (removePageCount > currentCapacity)
                    removePageCount = currentCapacity;
                
                for (long i = 0; i < removePageCount; i++)
                {
                    long currentIndex = currentCapacity - (i + 1);

                    //First, make sure this page is unallocated
                    if (ReadPageIsAllocatedFlag(currentIndex))
                        break;//Cannot remove this page, it is currently allocated

                    //Remove the page from the 'unallocated page list'
                    RemoveFromUnallocatedPageList(currentIndex);

                    //Resize the Stream
                    long requiredSize = GetRequiredStreamSize(PageSize, currentIndex/*index=count-1*/);
                    if (stream_ is ISafeResizable safeResizable)
                    {
                        if (!safeResizable.TrySetSize(requiredSize))
                        {
                            //Failed to resize, but data has not been corrupted.
                            //However, we already removed the page from the 'unallocated page list'.
                            //So since resize failed, we must add the page back to the 'unallocated page list'.
                            AddToUnallocatedPageList(currentIndex);
                            break;//Remove failed, no error, so stop here
                        }
                    }
                    else
                    {
                        stream_.SetLength(requiredSize);
                    }

                    //Update the PageCapacity property cache
                    cachedPageCapacity_ = GetPageCapacityForStreamSize(stream_.Length, PageSize);

                    //Successfully removed the page
                    amountRemoved++;

                    //Report the progress
                    progressReporter?.Report(new ProgressReport(amountRemoved, removePageCount));

                    //Check if the application requested to cancel
                    if (cancellationToken.IsCancellationRequested)
                        break;//Cancellation does not cause error, but rather stops at nearest 'safe' position, which is right here.
                }

                //Report final progress
                progressReporter?.Report(new ProgressReport(amountRemoved, removePageCount));

                return amountRemoved;
            }
        }

        /// <summary>
        /// Checks whether a page is currently allocated.
        /// </summary>
        /// <param name="index">The index of the page.</param>
        /// <returns>True if the page is currently allocated, otherwise false.</returns>
        /// <remarks>
        /// If <paramref name="index"/> is out of the range of all pages (allocated and unallocated),
        /// then false is returned. See <see cref="IsPageOnStorage(long)"/>.
        /// </remarks>
        /// <seealso cref="IsPageOnStorage(long)"/>
        /// <seealso cref="TryAllocatePage(out long)"/>
        /// <seealso cref="FreePage(long)"/>
        public bool IsPageAllocated(long index)
        {
            lock (locker)
            {
                if (!IsPageOnStorage(index))
                    return false;
                
                return ReadPageIsAllocatedFlag(index);
            }
        }

        /// <summary>
        /// Checks whether a page exists (regardless of whether or not it is allocated)
        /// on the storage.
        /// </summary>
        /// <param name="index">The index of the page.</param>
        /// <returns>True if <paramref name="index"/> is greater than or equal to zero and less
        /// than <see cref="PageCapacity"/>, otherwise false.</returns>
        /// <seealso cref="IsPageAllocated(long)"/>
        /// <seealso cref="PageCapacity"/>
        public bool IsPageOnStorage(long index)
        {
            lock (locker)
            {
                return index >= 0 && index < PageCapacity;
            }
        }

#if DEBUG

        /// <summary>
        /// -Debug Only- Defines the initial payload for newly allocated pages.
        /// </summary>
        internal enum InitialPayload
        {
            /// <summary>
            /// The initial payload consists of incrementing byte values.
            /// </summary>
            Incremental,

            /// <summary>
            /// The initial payload consists of all zeros.
            /// </summary>
            x00,

            /// <summary>
            /// The initial payload is whatever was previously stored.
            /// </summary>
            Unchanged,
        }

        /// <summary>
        /// -Debug Only- Defines the initial payload for newly allocated pages.
        /// </summary>
        internal InitialPayload InitialPayloadMode { get; set; } = InitialPayload.Incremental;
#endif

        /// <summary>
        /// Attempts to allocate a page.
        /// </summary>
        /// <param name="index">Upon success, assigned to the index of the allocated page. Upon
        /// failure, assigned to -1.</param>
        /// <returns>True if a page was successfully allocated. False only indicates that there
        /// is no capacity to allocate a new page.</returns>
        /// <remarks>
        /// <para>
        /// This method will find an unallocated page within the <see cref="PageCapacity"/>, and
        /// mark that page as allocated. The application is expected to keep track of the page
        /// via the assigned <paramref name="index"/>. If there is no unallocated page, then
        /// false will be returned and nothing will change.
        /// </para>
        /// <para>
        /// Note that this method will never change the <see cref="PageCapacity"/>. If there are
        /// no unallocated pages, then the only option to allocate a new page is to call
        /// <see cref="TryInflate(long, IProgress{ProgressReport}, CancellationToken)"/>.
        /// </para>
        /// <para>
        /// Also note that the 'initial payload' of a newly allocated page is considered undefined.
        /// Generally, it will contain a mixture of internal metadata that is used to track unallocated
        /// pages, and some payload that was stored when the page was previously allocated.
        /// </para>
        /// <para>
        /// If <see cref="IsReadOnly"/> is true, then an <see cref="InvalidOperationException"/>
        /// will be thrown and nothing will change.
        /// </para>
        /// <para>
        /// If any <see cref="Exception"/> is thrown, except the <see cref="InvalidOperationException"/>
        /// described above, then the application is expected to assume that some data has been corrupted.
        /// </para>
        /// </remarks>
        /// <exception cref="InvalidOperationException">Thrown if <see cref="IsReadOnly"/> is true.</exception>
        /// <exception cref="CorruptDataException">Thrown if any corrupt data is discovered. Note that other
        /// <see cref="Exception"/> types may also indicate data corruption.</exception>
        /// <seealso cref="FreePage(long)"/>
        /// <seealso cref="IsReadOnly"/>
        /// <seealso cref="IsPageAllocated(long)"/>
        /// <seealso cref="AllocatedPageCount"/>
        public bool TryAllocatePage(out long index)
        {
            if (IsReadOnly)
                throw new InvalidOperationException("Cannot allocate a page on a read-only " + nameof(StreamingPageStorage) + ".");

            lock (locker)
            {
                var firstUnallocated = FirstUnallocatedPageIndex;
                if (firstUnallocated.HasValue)
                {
                    index = firstUnallocated.Value;

                    //Make sure the 'FirstUnallocatedPageIndex' stored in the header was valid and indeed unallocated
                    if (!IsPageOnStorage(index))
                        throw new CorruptDataException("The 'First Unallocated Page Index' is invalid, it does not refer to a page that fits in the base " + nameof(Stream) + ".");
                    if (IsPageAllocated(index))
                        throw new CorruptDataException("The 'First Unallocated Page Index' is invalid, it refers to an allocated page.");

                    //Remove the page from the 'unallocated page list'
                    RemoveFromUnallocatedPageList(index);

                    //Set the 'is allocated' flag
                    WritePageIsAllocatedFlag(index, true);

                    //Increment the 'Allocated Page Count' header
                    AllocatedPageCount++;

#if DEBUG
                    //Write the initial payload.
                    //This is important for certain test cases, to ensure applications understand
                    //and work with the fact that initial payload may be garbage.
                    byte[] initPayload = new byte[1024];
                    for (long i = 0; i < PageSize; i += initPayload.Length)
                    {
                        long toWrite = initPayload.Length;
                        if(toWrite + i > PageSize)
                            toWrite = PageSize - i;

                        for(long j = 0; j < initPayload.Length; j++)
                        {
                            switch (InitialPayloadMode)
                            {
                                case InitialPayload.Incremental:
                                    initPayload[j] = (byte)(i + j + 1);
                                    break;

                                case InitialPayload.Unchanged:
                                    initPayload[j] = initPayload[j];
                                    break;

                                case InitialPayload.x00:
                                    initPayload[j] = 0x00;
                                    break;

                                default:
                                    throw new InvalidOperationException("Unrecognized "+nameof(InitialPayload)+" mode.");
                            }
                        }
                        
                        WriteTo(index, i, initPayload, 0, toWrite);
                    }
#endif

                    //Success
                    return true;
                }
                else
                {
                    //There is no unallocated ('reserved') page to allocate to the application
                    index = -1;
                    return false;
                }
            }
        }

        /// <summary>
        /// Deallocates a page.
        /// </summary>
        /// <param name="index">The index of the page to deallocate.</param>
        /// <returns>True if the page was deallocated, false if it was already
        /// unallocated when this method was called.</returns>
        /// <remarks>
        /// <para>
        /// If the specified page was allocated when this method was called, then 
        /// it will be marked as deallocated, and the <see cref="AllocatedPageCount"/>
        /// will decrease by one, but <see cref="PageCapacity"/> will remain unchanged.
        /// If the specified page was not allocated, then false is returned and 
        /// nothing will change.
        /// </para>
        /// <para>
        /// Note that payload stored in the page will <em>not</em> be erased. If the application has
        /// written information that it wishes to be erased, it must do so manually before calling this
        /// method. Otherwise, the payload may continue to exist on the storage, and may even be leaked
        /// to the application in a future <see cref="TryAllocatePage(out long)"/> call, since that
        /// page may contain the payload that was stored in this to-be-freed page.
        /// </para>
        /// <para>
        /// If <see cref="IsReadOnly"/> is true, then an <see cref="InvalidOperationException"/>
        /// will be thrown and nothing will change.
        /// </para>
        /// <para>
        /// If <paramref name="index"/> is out of the range of valid indices (negative or not less
        /// than <see cref="PageCapacity"/>, see <see cref="IsPageOnStorage(long)"/>), then an
        /// <see cref="ArgumentOutOfRangeException"/> will be thrown and nothing will change.
        /// </para>
        /// <para>
        /// If any <see cref="Exception"/> is thrown, except the two described above, then the
        /// application is expected to assume that some data has been corrupted.
        /// </para>
        /// </remarks>
        /// <exception cref="InvalidOperationException">Thrown if <see cref="IsReadOnly"/> is true.</exception>
        /// <exception cref="ArgumentOutOfRangeException">Thrown if <paramref name="index"/> is negative or
        /// greater than or equal to <see cref="PageCapacity"/>. See <see cref="IsPageOnStorage(long)"/>.</exception>
        /// <seealso cref="TryAllocatePage(out long)"/>
        /// <seealso cref="IsReadOnly"/>
        /// <seealso cref="IsPageAllocated(long)"/>
        /// <seealso cref="AllocatedPageCount"/>
        public bool FreePage(long index)
        {
            if (IsReadOnly)
                throw new InvalidOperationException("Cannot free any page on a read-only " + nameof(StreamingPageStorage) + ".");

            lock (locker)
            {
                if (!IsPageOnStorage(index))
                    throw new ArgumentOutOfRangeException(nameof(index));

                //Check if the specified page is currently allocated
                if (IsPageAllocated(index))
                {
                    //Mark the page as unallocated
                    WritePageIsAllocatedFlag(index, false);

                    //Add the page to the 'unallocated page list'
                    AddToUnallocatedPageList(index);

                    //Decrement the 'Allocated Page Count' header
                    AllocatedPageCount--;

                    return true;
                    //Success
                }
                else
                {
                    //The page was already unallocated, nothing to do
                    return false;
                }
            }
        }

        private long GetPagePayloadPosition(long index)
        {
            return GetPageIsAllocatedFlagPosition(index) + 1;//Skip the 'is allocated' flag
        }

        private long GetPageIsAllocatedFlagPosition(long index)
        {
            return HeaderSize + ((1/*'Is Allocated' flag*/ + PageSize) * index);
        }

        /// <summary>
        /// Reads payload from an allocated page.
        /// </summary>
        /// <param name="pageIndex">The index of the allocated page.</param>
        /// <param name="srcOffset">The source offset within the page's payload, measured in bytes.</param>
        /// <param name="buffer">The destination buffer.</param>
        /// <param name="dstOffset">The destination offset within the destination <paramref name="buffer"/>,
        /// measured in bytes.</param>
        /// <param name="length">The number of bytes to read.</param>
        /// <remarks>
        /// <para>
        /// If <paramref name="pageIndex"/> refers to an unallocated page, then an
        /// <see cref="InvalidOperationException"/> will be thrown.
        /// </para>
        /// <para>
        /// If any of the payload position/length arguments are out of their valid range, an
        /// <see cref="ArgumentOutOfRangeException"/> will be thrown.
        /// </para>
        /// <para>
        /// If <paramref name="buffer"/> is null, then an <see cref="ArgumentNullException"/> will
        /// be thrown.
        /// </para>
        /// </remarks>
        /// <exception cref="InvalidOperationException">Thrown if <paramref name="pageIndex"/> does not refer to
        /// an allocated page.
        /// </exception>
        /// <exception cref="ArgumentOutOfRangeException">Thrown if <paramref name="srcOffset"/>,
        /// <paramref name="dstOffset"/>, or <paramref name="length"/> is negative; or if the sum of
        /// <paramref name="srcOffset"/> and <paramref name="length"/> is greater than <see cref="PageSize"/>;
        /// or if the sum of <paramref name="dstOffset"/> and <paramref name="length"/> is greater than
        /// the <paramref name="buffer"/> size; or if <paramref name="dstOffset"/> or <paramref name="length"/>
        /// is greater than <see cref="Int32.MaxValue"/> (since the <see cref="Stream.Read(byte[], int, int)"/>
        /// method does not accept long arguments).
        /// </exception>
        /// <exception cref="ArgumentNullException">Thrown if <paramref name="buffer"/> is null.</exception>
        /// <seealso cref="WriteTo(long, long, byte[], long, long)"/>
        /// <seealso cref="IsPageAllocated(long)"/>
        public void ReadFrom(long pageIndex, long srcOffset, byte[] buffer, long dstOffset, long length)
        {
            if (buffer == null)
                throw new ArgumentNullException(nameof(buffer));

            lock (locker)
            {
                if (!IsPageOnStorage(pageIndex))
                    throw new ArgumentOutOfRangeException(nameof(pageIndex), "The specified page index does not refer to a page on the " + nameof(Stream) + ".");
                if (!IsPageAllocated(pageIndex))
                    throw new InvalidOperationException("Cannot read from an unallocated page.");
                if (srcOffset < 0)
                    throw new ArgumentOutOfRangeException(nameof(srcOffset), "The source offset cannot be less than zero.");
                if (dstOffset < 0)
                    throw new ArgumentOutOfRangeException(nameof(dstOffset), "The destination offset cannot be less than zero.");
                if (length < 0)
                    throw new ArgumentOutOfRangeException(nameof(dstOffset), "The length cannot be less than zero.");
                if (dstOffset > int.MaxValue)
                    throw new ArgumentOutOfRangeException(nameof(dstOffset), "The destination offset cannot be greater than " + nameof(Int32) + "." + nameof(Int32.MaxValue) + ", since the " + nameof(Stream) + " API does not support reading into arrays with long indices.");
                if (length > int.MaxValue)
                    throw new ArgumentOutOfRangeException(nameof(length), "The length cannot be greater than " + nameof(Int32) + "." + nameof(Int32.MaxValue) + ", since the " + nameof(Stream) + " API does not support reading into arrays with long indices.");
                if (srcOffset + length > PageSize)
                    throw new ArgumentOutOfRangeException(nameof(length), "The sum of the source offset and length cannot be greater than " + nameof(PageSize) + ".");
                if (dstOffset + length > buffer.Length)
                    throw new ArgumentOutOfRangeException(nameof(length), "The sum of the destination offset and length cannot be greater than the length of the destination buffer.");

                stream_.Position = GetPagePayloadPosition(pageIndex) + srcOffset;
                stream_.Read(buffer, (int)dstOffset, (int)length);
            }
        }

        /// <summary>
        /// Writes payload to an allocated page.
        /// </summary>
        /// <param name="pageIndex">The index of the allocated page.</param>
        /// <param name="dstOffset">The destination offset within the page's payload, measured in 
        /// bytes.</param>
        /// <param name="buffer">The source buffer.</param>
        /// <param name="srcOffset">The source offset within the source <paramref name="buffer"/>,
        /// measured in bytes.</param>
        /// <param name="length">The number of bytes to write.</param>
        /// <remarks>
        /// <para>
        /// If <paramref name="pageIndex"/> refers to an unallocated page, or if <see cref="IsReadOnly"/>
        /// is true, then an <see cref="InvalidOperationException"/> will be thrown and nothing
        /// will change.
        /// </para>
        /// <para>
        /// If any of the payload position/length arguments are out of their valid range, an
        /// <see cref="ArgumentOutOfRangeException"/> will be thrown and nothing will change.
        /// </para>
        /// <para>
        /// If <paramref name="buffer"/> is null, then an <see cref="ArgumentNullException"/>
        /// will be thrown.
        /// </para>
        /// <para>
        /// If any <see cref="Exception"/> is thrown, except those described above, then the
        /// application is expected to assume that some data may have been corrupted.
        /// </para>
        /// </remarks>
        /// <exception cref="InvalidOperationException">Thrown if <paramref name="pageIndex"/> refers
        /// to an unallocated page, or if <see cref="IsReadOnly"/> is true.
        /// </exception>
        /// <exception cref="ArgumentOutOfRangeException">Thrown if <paramref name="srcOffset"/>,
        /// <paramref name="dstOffset"/>, or <paramref name="length"/> is negative; or if
        /// the sum of <paramref name="dstOffset"/> and <paramref name="length"/> is greater
        /// than <see cref="PageSize"/>; or if the sum of <paramref name="srcOffset"/> and
        /// <paramref name="length"/> is greater than the <paramref name="buffer"/> size; or
        /// if <paramref name="srcOffset"/> or <paramref name="length"/> is greater than
        /// <see cref="int.MaxValue"/> (since the <see cref="Stream.Write(byte[], int, int)"/>
        /// method does not accept long arguments).
        /// </exception>
        /// <exception cref="ArgumentNullException">Thrown if <paramref name="buffer"/> is null.</exception>
        /// <seealso cref="ReadFrom(long, long, byte[], long, long)"/>
        /// <seealso cref="IsReadOnly"/>
        /// <seealso cref="IsPageAllocated(long)"/>
        public void WriteTo(long pageIndex, long dstOffset, byte[] buffer, long srcOffset, long length)
        {
            if (buffer == null)
                throw new ArgumentNullException(nameof(buffer));
            if (IsReadOnly)
                throw new InvalidOperationException("Cannot write to any page on a read-only " + nameof(StreamingPageStorage) + ".");

            lock (locker)
            {
                if (!IsPageOnStorage(pageIndex))
                    throw new ArgumentOutOfRangeException(nameof(pageIndex), "The specified page index does not refer to a page on the " + nameof(Stream) + ".");
                if (!IsPageAllocated(pageIndex))
                    throw new InvalidOperationException("Cannot write to an unallocated page.");
                if (dstOffset < 0)
                    throw new ArgumentOutOfRangeException(nameof(dstOffset), "The destination offset cannot be less than zero.");
                if (srcOffset < 0)
                    throw new ArgumentOutOfRangeException(nameof(srcOffset), "The source offset cannot be less than zero.");
                if (length < 0)
                    throw new ArgumentOutOfRangeException(nameof(length), "The length cannot be less than zero.");
                if (srcOffset > int.MaxValue)
                    throw new ArgumentOutOfRangeException(nameof(srcOffset), "The source offset cannot be greater than " + nameof(Int32) + "." + nameof(Int32.MaxValue) + ", since the " + nameof(Stream) + " API does not support writing from arrays with long indices.");
                if (length > int.MaxValue)
                    throw new ArgumentOutOfRangeException(nameof(length), "The length cannot be greater than " + nameof(Int32) + "." + nameof(Int32.MaxValue) + ", since the " + nameof(Stream) + " API does not support writing from an array with long indices.");
                if (dstOffset + length > PageSize)
                    throw new ArgumentOutOfRangeException(nameof(length), "The sum of the destination offset and length cannot be greater than " + nameof(PageSize) + ".");
                if (srcOffset + length > buffer.Length)
                    throw new ArgumentOutOfRangeException(nameof(length), "The sum of the source offset and length cannot be greater than the size of the source buffer.");


                stream_.Position = GetPagePayloadPosition(pageIndex) + dstOffset;
                stream_.Write(buffer, (int)srcOffset, (int)length);
                stream_.Flush();
            }
        }

        /// <summary>
        /// Has the <see cref="Dispose"/> method been called?
        /// </summary>
        public bool IsDisposed { get; private set; }

        /// <summary>
        /// Disposes this <see cref="StreamingPageStorage"/>.
        /// </summary>
        /// <remarks>
        /// <para>
        /// The base <see cref="Stream"/> will be flushed via <see cref="Stream.Flush"/>,
        /// though this should be redundant since the <see cref="WriteTo(long, long, byte[], long, long)"/>
        /// method automatically flushes it.
        /// </para>
        /// <para>
        /// If the constructor specified that the base <see cref="Stream"/> should <em>not</em>
        /// remain open (un-disposed), then the <see cref="Stream.Dispose()"/> method will be
        /// called.
        /// </para>
        /// </remarks>
        /// <seealso cref="IsDisposed"/>
        public void Dispose()
        {
            lock(locker)
            {
                if(!IsDisposed)
                {
                    stream_.Flush();//Should be redundant, but let's do this just in case

                    if (!leaveStreamOpen_)
                        stream_.Dispose();

                    IsDisposed = true;
                }
            }
        }
    }
}
