using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;

namespace Storage
{
    /// <summary>
    /// <see cref="IPageStorage"/> that provides caching for another <see cref="IPageStorage"/> implementation.
    /// </summary>
    /// <remarks>
    /// When an <see cref="IPageStorage"/> implementation does not provide sufficient caching, the application should
    /// use this class to wrap the non-cached <see cref="IPageStorage"/>. This is particularly important for applications
    /// which may frequently repeat reads or writes to or from a page.
    /// </remarks>
    public class CachedPageStorage : IPageStorage
    {
        /// <summary>
        /// The base <see cref="IPageStorage"/>.
        /// </summary>
        public IPageStorage PageStorage { get; private set; }

        /// <summary>
        /// Will this <see cref="CachedPageStorage"/> leave the base <see cref="PageStorage"/>
        /// open (un-disposed) after <see cref="Dispose"/> is called?
        /// </summary>
        public bool WillLeaveBasePageStorageOpen { get; private set; }

        /// <summary>
        /// The maximum number of pages that will be stored in cache memory.
        /// </summary>
        public int CachedPageCapacity { get; private set; }

        /// <summary>
        /// Defines the write mode of cache storage.
        /// </summary>
        public enum CacheWriteMode
        {
            /// <summary>
            /// Cache is only stored for reading. Write operations are not supported.
            /// </summary>
            ReadOnly,

            /// <summary>
            /// Written data will be stored in cache until the buffers are flushed 
            /// (either automatically or manually via <see cref="Flush"/>).
            /// </summary>
            WriteBack,

            /// <summary>
            /// All writes are passed directly to the base <see cref="PageStorage"/>.
            /// There is no need to call <see cref="Flush"/>.
            /// </summary>
            WriteThrough
        }

        /// <summary>
        /// The <see cref="CacheWriteMode"/> that determines when written data is passed to
        /// the base <see cref="PageStorage"/>.
        /// </summary>
        public CacheWriteMode Mode { get; private set; }

        /// <summary>
        /// Gets the index of each page that is currently cached, in the order such
        /// that the most recently used page is first.
        /// </summary>
        public IEnumerable<long> CachedPageIndices
        {
            get
            {
                lock (locker)
                {
                    foreach (var pair in cachedPages.OrderByDescending(x => x.Value.RecentUseCounter))
                        yield return pair.Key;
                }
            }
        }

        /// <summary>
        /// Is this <see cref="CachedPageStorage"/> read-only?
        /// </summary>
        public bool IsReadOnly
        {
            get
            {
                return Mode == CacheWriteMode.ReadOnly;
            }
        }

        /// <summary>
        /// Is the <see cref="PageCapacity"/> fixed?
        /// </summary>
        /// <remarks>
        /// If <see cref="IsReadOnly"/> is true, then true is returned. Otherwise,
        /// the return value is equivalent to the <see cref="IPageStorage.IsCapacityFixed"/>
        /// property of the base <see cref="PageStorage"/>.
        /// </remarks>
        public bool IsCapacityFixed
        {
            get
            {
                if (IsReadOnly)
                    return true;
                else
                    return PageStorage.IsCapacityFixed;
            }
        }
        
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
                lock(locker)
                {
                    return PageStorage.PageCapacity;
                }
            }
        }
        
        /// <summary>
        /// The total number of pages that are currently allocated.
        /// </summary>
        /// <seealso cref="PageCapacity"/>
        /// <seealso cref="TryAllocatePage(out long)"/>
        /// <seealso cref="FreePage(long)"/>
        /// <remarks>
        /// Note that this property may potentially be larger than the number of pages
        /// that have been allocated <em>by the application</em>, since some
        /// implementations of the base <see cref="IPageStorage"/> may perform some internal 
        /// page allocations for their various needs. In general, the application is expected 
        /// to keep track of which pages it has allocated.
        /// </remarks>
        public long AllocatedPageCount
        {
            get
            {
                lock(locker)
                {
                    return PageStorage.AllocatedPageCount;
                }
            }
        }

        private long? cachedPageSize_ = null;
        /// <summary>
        /// The size of each page's payload, measured in bytes.
        /// </summary>
        /// <remarks>
        /// This is equivalent to the base <see cref="PageSize"/>'s <see cref="IPageStorage.PageSize"/>.
        /// </remarks>
        public long PageSize
        {
            get
            {
                lock(locker)
                {
                    //Caching this property is safe since page size cannot change
                    if (!cachedPageSize_.HasValue)
                        cachedPageSize_ = PageStorage.PageSize;

                    return cachedPageSize_.Value;
                }
            }
        }
        
        /// <summary>
        /// The index of the application-defined 'entry page,' or null.
        /// </summary>
        /// <remarks>
        /// <para>
        /// This is equivalent to the base <see cref="PageStorage"/>'s <see cref="IPageStorage.EntryPageIndex"/>, it
        /// will not be cached.
        /// </para>
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
        /// <para>
        /// If a negative value is assigned, an <see cref="ArgumentOutOfRangeException"/> will be thrown.
        /// </para>
        /// <para>
        /// If <see cref="IsReadOnly"/> is true and the application assigns a value to this property, then
        /// an <see cref="InvalidOperationException"/> will be thrown.
        /// </para>
        /// </remarks>
        /// <exception cref="ArgumentOutOfRangeException">Thrown when assigning a negative value.</exception>
        /// <exception cref="InvalidOperationException">Thrown when assigning a value when <see cref="IsReadOnly"/>
        /// is true.</exception>
        public long? EntryPageIndex
        {
            get
            {
                lock(locker)
                {
                    return PageStorage.EntryPageIndex;
                }
            }

            set
            {
                lock(locker)
                {
                    if (IsReadOnly)
                        throw new InvalidOperationException("Cannot set the " + nameof(EntryPageIndex) + " of a read-only " + nameof(CachedPageStorage) + ".");
                    if (value != null && value.Value < 0)
                        throw new ArgumentOutOfRangeException(nameof(value), "Cannot assign a negative " + nameof(EntryPageIndex) + ".");

                    PageStorage.EntryPageIndex = value;
                }
            }
        }

#if DEBUG
        internal bool SimulateOutOfMemory { get; set; } = false;
#endif

        /// <summary>
        /// <see cref="CachedPageStorage"/> constructor.
        /// </summary>
        /// <param name="baseStorage">The base <see cref="IPageStorage"/>.</param>
        /// <param name="cacheWriteMode">The <see cref="CacheWriteMode"/> that determines when write operations are
        /// sent to the <paramref name="baseStorage"/>.</param>
        /// <param name="cachedPageCapacity">The maximum number of pages to store in cache at once.</param>
        /// <param name="leaveBaseStorageOpen">Should the <paramref name="baseStorage"/> remain open (non-disposed)
        /// after this <see cref="CachedPageStorage"/> is disposed? If false, then when <see cref="Dispose"/>
        /// is called, the <paramref name="baseStorage"/> will also be disposed.</param>
        /// <exception cref="ArgumentNullException">Thrown if <paramref name="baseStorage"/> is null.</exception>
        /// <exception cref="ArgumentException">Thrown if the <paramref name="baseStorage"/>'s <see cref="IPageStorage.PageSize"/>
        /// is greater than <see cref="int.MaxValue"/>, or if <paramref name="cacheWriteMode"/> is not equal to
        /// <see cref="CacheWriteMode.ReadOnly"/> when the <paramref name="baseStorage"/> is read-only.</exception>
        /// <exception cref="ArgumentOutOfRangeException">Thrown if <paramref name="cachedPageCapacity"/> is less
        /// than zero.</exception>
        /// <remarks>
        /// The application should expect that <paramref name="cachedPageCapacity"/> full pages will be loaded into
        /// memory at any given moment. The amount of memory required depends on the <see cref="IPageStorage.PageSize"/>
        /// of the <paramref name="baseStorage"/>. If the page size is very high, and so is <paramref name="cachedPageCapacity"/>,
        /// then there may not be enough memory for the cache. If allocation fails due to an <see cref="OutOfMemoryException"/>,
        /// then some pages may be evicted from cache, or cache may be bypassed entirely (by writing to and reading from the
        /// base <see cref="PageStorage"/> directly, if necessary).
        /// </remarks>
        public CachedPageStorage(IPageStorage baseStorage, CacheWriteMode cacheWriteMode, int cachedPageCapacity, bool leaveBaseStorageOpen)
        {
            if (baseStorage == null)
                throw new ArgumentNullException(nameof(baseStorage));
            if (baseStorage.PageSize > Int32.MaxValue)
                throw new ArgumentException("Cannot use " + nameof(CachedPageStorage) + " for " + nameof(IPageStorage) + "s with page sizes greater than " + nameof(Int32) + "." + nameof(Int32.MaxValue) + ".", nameof(baseStorage));
            if (baseStorage.IsReadOnly && cacheWriteMode != CacheWriteMode.ReadOnly)
                throw new ArgumentException("When the " + nameof(IPageStorage) + " is read-only, the " + nameof(CacheWriteMode) + " must be " + nameof(CacheWriteMode.ReadOnly) + ".", nameof(cacheWriteMode));
            if (cachedPageCapacity < 0)
                throw new ArgumentOutOfRangeException(nameof(cachedPageCapacity), "The page cache capacity cannot be less than zero.");

            this.PageStorage = baseStorage;
            this.CachedPageCapacity = cachedPageCapacity;
            this.cachedPages = new Dictionary<long, CachedPage>(cachedPageCapacity);
            this.Mode = cacheWriteMode;
            this.WillLeaveBasePageStorageOpen = leaveBaseStorageOpen;
        }

        private object locker = new object();

        private Dictionary<long, CachedPage> cachedPages;

        /// <summary>
        /// Sends all pending write operations to the base <see cref="PageStorage"/>.
        /// </summary>
        /// <remarks>
        /// This method is only necessary when using <see cref="CacheWriteMode.WriteBack"/>.
        /// If <see cref="Mode"/> is anything other than <see cref="CacheWriteMode.WriteBack"/>, calling
        /// this method has no effect.
        /// </remarks>
        public void Flush()
        {
            lock(locker)
            {
                //Evict all pages from cache
                //This will cause cached writes to be sent to base IPageStorage
                List<long> indicesToFlush = CachedPageIndices.ToList();
                foreach (var index in indicesToFlush)
                    EvictPageFromCache(index);
            }
        }

        private CachedPage AddPageToCache(long pageIndex)
        {
            lock(locker)
            {
                //Make sure cache page limit is not exceeded
                long maxCacheCount = CachedPageCapacity - 1;//-1 so we can insert the new page
                long countToEvict = cachedPages.Count - maxCacheCount;
                if(countToEvict > 0)
                {
                    List<long> indicesToEvict = cachedPages.OrderBy(x => x.Value.RecentUseCounter).Select(y => y.Key).Take((int)countToEvict).ToList();
                    foreach (var indexToEvict in indicesToEvict)
                        EvictPageFromCache(indexToEvict);
                }

                CachedPage page = new CachedPage(this, pageIndex);
                cachedPages.Add(pageIndex, page);
                return page;
            }
        }

        private bool TryGetCachedPage(long pageIndex, out CachedPage cachedPage)
        {
            lock(locker)
            {
                if(CachedPageCapacity == 0)
                {
                    cachedPage = null;
                    return false;
                }

                if (!cachedPages.TryGetValue(pageIndex, out cachedPage))
                {
                    //The page is not yet in cache

                    if(PageStorage.IsPageAllocated(pageIndex))
                    {
                        try
                        {
                            cachedPage = AddPageToCache(pageIndex);
                            return true;
                        }
                        catch (OutOfMemoryException)
                        {
                            cachedPage = null;
                            return false;
                        }
                    }
                    else
                    {
                        //The page is not allocated, cannot cache it
                        cachedPage = null;
                        return false;
                    }
                }
                else
                {
                    return true;
                }
            }
        }

        /// <summary>
        /// Evicts a page from cache, writing any cached write data to the base <see cref="PageStorage"/>
        /// if necessary.
        /// </summary>
        /// <param name="pageIndex">The index of the page to cache.</param>
        /// <returns>True if the page was evicted from cache. False indicates
        /// that it was not cached when this method was called, thus nothing
        /// changed.</returns>
        /// <remarks>
        /// If the <see cref="Mode"/> is <see cref="CacheWriteMode.WriteBack"/>, then any data that has
        /// been written to the page will be 'flushed' (written) to the base <see cref="PageStorage"/>.
        /// </remarks>
        public bool EvictPageFromCache(long pageIndex)
        {
            lock(locker)
            {
                if (cachedPages.TryGetValue(pageIndex, out var page))
                    page.Flush();//Flush any cached write data

                return cachedPages.Remove(pageIndex);
            }
        }

        /// <summary>
        /// Checks whether a page is currently cached.
        /// </summary>
        /// <param name="pageIndex">The index of the page.</param>
        /// <returns>True if the page at the specified <paramref name="pageIndex"/> is
        /// currently cached, otherwise false.</returns>
        /// <remarks>
        /// If the <paramref name="pageIndex"/> is out of the valid range of pages
        /// (see <see cref="IPageStorage.IsPageOnStorage(long)"/>), then false will
        /// be returned.
        /// </remarks>
        public bool IsPageCached(long pageIndex)
        {
            lock(locker)
            {
                return cachedPages.ContainsKey(pageIndex);
            }
        }

        /// <summary>
        /// Attempts to increase the <see cref="PageCapacity"/> of the base <see cref="PageStorage"/>
        /// by creating space for more unallocated pages.
        /// </summary>
        /// <param name="additionalPageCount">The desired additional number of unallocated 
        /// pages to create.</param>
        /// <param name="progressReporter"><see cref="IProgress{T}"/> that receives a
        /// <see cref="ProgressReport"/> as the inflation progresses. May be null, in 
        /// which case progress will not be reported.</param>
        /// <param name="cancellationToken"><see cref="CancellationToken"/> that allows
        /// the application to cancel the inflation. Cancellation will cause the inflation
        /// to stop at the nearest 'safe' position to avoid data corruption. This means
        /// that some pages may have already been created upon cancellation, but not 
        /// necessarily as many as were requested. Upon cancellation, this method will
        /// NOT throw any <see cref="Exception"/>, but will instead return the additional
        /// number of pages that have been created, if any, before the operation was
        /// cancelled.</param>
        /// <returns>The actual number of additional unallocated pages that were created. 
        /// This may be more or less than the desired <paramref name="additionalPageCount"/>, 
        /// and may even be zero.</returns>
        /// <remarks>
        /// <para>
        /// This method will simply call the <see cref="IPageStorage.TryInflate(long, IProgress{ProgressReport}, CancellationToken)"/>
        /// method of the base <see cref="PageStorage"/>. Cache will not be affected.
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
            if (IsCapacityFixed)
                throw new InvalidOperationException("Cannot inflate a fixed-capacity " + nameof(CachedPageStorage) + ".");
            if (additionalPageCount < 0)
                throw new ArgumentOutOfRangeException(nameof(additionalPageCount), "Cannot inflate by a negative number of pages.");

            lock(locker)
            {
                long ret = PageStorage.TryInflate(additionalPageCount, progressReporter, cancellationToken);
                return ret;
            }
        }

        /// <summary>
        /// Attempts to decrease the <see cref="PageCapacity"/>, by removing unallocated pages
        /// at the very end of this <see cref="IPageStorage"/>.
        /// </summary>
        /// <param name="removePageCount">The desired number of unallocated pages to remove.</param>
        /// <param name="progressReporter"><see cref="IProgress{T}"/> that receives a
        /// <see cref="ProgressReport"/> as the deflation progresses. May be null, in which
        /// case progress will not be reported.</param>
        /// <param name="cancellationToken"><see cref="CancellationToken"/> that allows the
        /// application to cancel the deflation operation. Cancellation will cause the
        /// deflation to stop at the nearest 'safe' position to avoid data corruption. This
        /// means that some pages may have been removed upon cancellation, but not necessarily
        /// as many as were requested. Upon cancellation, this method will NOT throw an
        /// <see cref="Exception"/>, but will instead return the number of pages that have
        /// been removed before cancellation.</param>
        /// <returns>The actual number of unallocated pages that were removed. This may be more or
        /// less than <paramref name="removePageCount"/>, and may even be zero.</returns>
        /// <remarks>
        /// <para>
        /// This method will simply call the <see cref="IPageStorage.TryDeflate(long, IProgress{ProgressReport}, CancellationToken)"/>
        /// method of the base <see cref="PageStorage"/>. Cache will not be affected.
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
            if (IsCapacityFixed)
                throw new InvalidOperationException("Cannot deflate a fixed-capacity " + nameof(CachedPageStorage) + ".");
            if (removePageCount < 0)
                throw new ArgumentOutOfRangeException(nameof(removePageCount), "Cannot deflate by a negative number of pages.");

            lock(locker)
            {
                long ret = PageStorage.TryDeflate(removePageCount, progressReporter, cancellationToken);
                return ret;
            }
        }

        /// <summary>
        /// Checks whether a page is currently allocated.
        /// </summary>
        /// <param name="index">The index of the page.</param>
        /// <returns>True if the page is currently allocated, otherwise false.</returns>
        /// <remarks>
        /// <para>
        /// If <paramref name="index"/> is out of the range of all pages (allocated and unallocated),
        /// then false is returned. See <see cref="IsPageOnStorage(long)"/>.
        /// </para>
        /// <para>
        /// Note that some base <see cref="PageStorage"/> implementations may perform internal allocations 
        /// for their various needs, and so this method may return true for such pages. In general, the 
        /// application should keep track of which pages it has allocated.
        /// </para>
        /// </remarks>
        /// <seealso cref="IsPageOnStorage(long)"/>
        /// <seealso cref="TryAllocatePage(out long)"/>
        /// <seealso cref="FreePage(long)"/>
        public bool IsPageAllocated(long index)
        {
            lock(locker)
            {
                if (!IsPageOnStorage(index))
                    return false;
                if (IsPageCached(index))
                    return true;//Only allocated pages are cached

                return PageStorage.IsPageAllocated(index);
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
            lock(locker)
            {
                return index >= 0 && index < PageCapacity;
            }
        }

        /// <summary>
        /// Attempts to allocate a page.
        /// </summary>
        /// <param name="index">Upon success, assigned to the index of the allocated page. Upon
        /// failure, assigned to -1.</param>
        /// <returns>True if a page was successfully allocated. False only indicates that there
        /// is no capacity to allocate a new page (<see cref="AllocatedPageCount"/> equals
        /// <see cref="PageCapacity"/>).</returns>
        /// <remarks>
        /// <para>
        /// This method will simply call the <see cref="TryAllocatePage(out long)"/> method of
        /// the base <see cref="PageStorage"/>.
        /// </para>
        /// </remarks>
        /// <exception cref="InvalidOperationException">Thrown if <see cref="IsReadOnly"/> is true.</exception>
        /// <seealso cref="FreePage(long)"/>
        /// <seealso cref="IsReadOnly"/>
        /// <seealso cref="IsPageAllocated(long)"/>
        /// <seealso cref="AllocatedPageCount"/>
        public bool TryAllocatePage(out long index)
        {
            lock(locker)
            {
                if (IsReadOnly)
                    throw new InvalidOperationException("Cannot allocate a page on a read-only " + nameof(CachedPageStorage) + ".");

                bool ret = PageStorage.TryAllocatePage(out index);
                return ret;
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
        /// Before freeing the page, this method will ensure that any pending
        /// write operations are sent to the base <see cref="PageStorage"/>. This
        /// may be important for security cases where the application was storing
        /// sensitive data that it wishes to overwrite before freeing the page. After
        /// cache is flushed, the <see cref="IPageStorage.FreePage(long)"/> method of
        /// the base <see cref="PageStorage"/> will be called.
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
            lock(locker)
            {
                if (IsReadOnly)
                    throw new InvalidOperationException("Cannot free a page on a read-only " + nameof(CachedPageStorage) + ".");
                if (!IsPageOnStorage(index))
                    throw new ArgumentOutOfRangeException(nameof(index), "The index refers to a page which does not exist on the base " + nameof(IPageStorage) + ".");

                //First, evict the page from cache (assuming it is cached).
                //Note that this may cause any pending writes to be sent to the base PageStorage.
                //This is intended! The application may have overwritten sensitive data.
                EvictPageFromCache(index);

                bool ret = PageStorage.FreePage(index);
                return ret;
            }
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
        /// If any of the requested data is not currently cached, it will be loaded into cache (unless
        /// <see cref="CachedPageCapacity"/> is zero). Then, the cached data will be copied into the
        /// destination <paramref name="buffer"/>. If cache fails due to insufficient memory, or if
        /// <see cref="CachedPageCapacity"/> is zero, then this method will bypass cache and read
        /// directly from the base <see cref="PageStorage"/>.
        /// </para>
        /// </remarks>
        /// <exception cref="InvalidOperationException">Thrown if <paramref name="pageIndex"/> does not refer to
        /// an allocated page.
        /// </exception>
        /// <exception cref="ArgumentOutOfRangeException">Thrown if <paramref name="srcOffset"/>,
        /// <paramref name="dstOffset"/>, or <paramref name="length"/> is negative; or if the sum of
        /// <paramref name="srcOffset"/> and <paramref name="length"/> is greater than <see cref="PageSize"/>;
        /// or if the sum of <paramref name="dstOffset"/> and <paramref name="length"/> is greater than
        /// the <paramref name="buffer"/> size.
        /// </exception>
        /// <exception cref="ArgumentNullException">Thrown if <paramref name="buffer"/> is null.</exception>
        /// <seealso cref="WriteTo(long, long, byte[], long, long)"/>
        /// <seealso cref="IsPageAllocated(long)"/>
        public void ReadFrom(long pageIndex, long srcOffset, byte[] buffer, long dstOffset, long length)
        {
            if (buffer == null)
                throw new ArgumentNullException(nameof(buffer));
            if (srcOffset < 0)
                throw new ArgumentOutOfRangeException(nameof(srcOffset), "The source offset cannot be less than zero.");
            if (dstOffset < 0)
                throw new ArgumentOutOfRangeException(nameof(dstOffset), "The destination offset cannot be less than zero.");
            if (length < 0)
                throw new ArgumentOutOfRangeException(nameof(length), "The length cannot be less than zero.");
            if (srcOffset + length > PageSize)
                throw new ArgumentOutOfRangeException(nameof(srcOffset), "The sum of the source offset and length cannot be greater than " + nameof(PageSize) + ".");
            if (dstOffset + length > buffer.Length)
                throw new ArgumentOutOfRangeException(nameof(dstOffset), "The sum of the destination offset and length cannot be greater than the length of the destination buffer.", nameof(dstOffset));

            if (TryGetCachedPage(pageIndex, out var cachedPage))
            {
                cachedPage.Read(srcOffset, buffer, dstOffset, length);
            }
            else
            {
                if (!IsPageAllocated(pageIndex))
                    throw new InvalidOperationException("Cannot read from an unallocated page.");

                PageStorage.ReadFrom(pageIndex, srcOffset, buffer, dstOffset, length);//Failed to use cache, so read directly from base IPageStorage
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
        /// If the page is not loaded into cache, it will be loaded (but not read, as read is not
        /// necessary for this method). The data will be written into the cached page (unless cache
        /// failed or <see cref="CachedPageCapacity"/> is zero), and then if <see cref="Mode"/> is
        /// <see cref="CacheWriteMode.WriteThrough"/> the data will also be immediately written to
        /// the base <see cref="PageStorage"/>. If <see cref="Mode"/> is <see cref="CacheWriteMode.WriteBack"/>,
        /// then data will be written to the base <see cref="PageStorage"/> at a later time (when the 
        /// internal buffers get too full, or when the page is evicted from cache, or when <see cref="Flush"/> 
        /// or <see cref="Dispose"/> is called). If caching fails, or if <see cref="CachedPageCapacity"/>
        /// is zero, then the data will be immediately written directly to the base <see cref="PageStorage"/> 
        /// regardless of the <see cref="Mode"/>.
        /// </para>
        /// </remarks>
        /// <exception cref="InvalidOperationException">Thrown if <paramref name="pageIndex"/> refers
        /// to an unallocated page, or if <see cref="IsReadOnly"/> is true.
        /// </exception>
        /// <exception cref="ArgumentOutOfRangeException">Thrown if <paramref name="srcOffset"/>,
        /// <paramref name="dstOffset"/>, or <paramref name="length"/> is negative; or if
        /// the sum of <paramref name="dstOffset"/> and <paramref name="length"/> is greater
        /// than <see cref="PageSize"/>; or if the sum of <paramref name="srcOffset"/> and
        /// <paramref name="length"/> is greater than the <paramref name="buffer"/> size.
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
                throw new InvalidOperationException("Cannot write to any page on a read-only " + nameof(CachedPageStorage) + ".");
            if (dstOffset < 0)
                throw new ArgumentOutOfRangeException(nameof(dstOffset), "The destination offset cannot be less than zero.");
            if (srcOffset < 0)
                throw new ArgumentOutOfRangeException(nameof(srcOffset), "The source offset cannot be less than zero.");
            if (length < 0)
                throw new ArgumentOutOfRangeException(nameof(length), "The length cannot be less than zero.");
            if (dstOffset + length > PageSize)
                throw new ArgumentOutOfRangeException(nameof(dstOffset), "The sum of the destination offset and length cannot be greater than " + nameof(PageSize) + ".");
            if (srcOffset + length > buffer.Length)
                throw new ArgumentOutOfRangeException(nameof(srcOffset), "The sum of the source offset and length cannot be greater than the size of the source buffer .");
            
            if (TryGetCachedPage(pageIndex, out var cachedPage))
            {
                cachedPage.Write(dstOffset, buffer, srcOffset, length);
            }
            else
            {
                if (!IsPageAllocated(pageIndex))
                    throw new InvalidOperationException("Cannot write to an unallocated page.");

                PageStorage.WriteTo(pageIndex, dstOffset, buffer, srcOffset, length);//Failed to use cache, so write directly to base IPageStorage
            }
        }

        /// <summary>
        /// Has this <see cref="CachedPageStorage"/> been disposed?
        /// </summary>
        public bool IsDisposed { get; private set; } = false;

        /// <summary>
        /// Disposes this <see cref="CachedPageStorage"/>, sending any cached write operations
        /// to the base <see cref="PageStorage"/>.
        /// </summary>
        /// <remarks>
        /// <para>
        /// This method will result in a call to <see cref="Flush"/> to ensure that any cached write
        /// operations are sent to the base <see cref="PageStorage"/>.
        /// </para>
        /// <para>
        /// If <see cref="WillLeaveBasePageStorageOpen"/> is false, then this method will dispose the
        /// base <see cref="PageStorage"/>.
        /// </para>
        /// </remarks>
        /// <seealso cref="IsDisposed"/>
        public void Dispose()
        {
            lock(locker)
            {
                if(!IsDisposed)
                {
                    //Send any cached writes to the base IPageStorage
                    Flush();

                    if (!WillLeaveBasePageStorageOpen)
                        PageStorage.Dispose();

                    IsDisposed = true;
                }
            }
        }
    }
}
