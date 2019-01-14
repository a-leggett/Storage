using System;
using System.Threading;

namespace Storage
{
    /// <summary>
    /// Interface that can store fixed-length blocks of memory, called pages.
    /// </summary>
    /// <remarks>
    /// <para>An <see cref="IPageStorage"/> can be considered similar to an array of pages. Each
    /// page contains a fixed-length payload, defined by <see cref="PageSize"/>. Some pages
    /// can be 'allocated' and some can be 'unallocated.' When an application has an 'allocated'
    /// page, it keeps track of the index of that page. The application can then write payload
    /// to, or read payload from, that page. Generally, this interface is used to store pages
    /// in non-volatile storage, such as a file.</para>
    /// <para>
    /// In this documentation, the terms 'allocated' and 'unallocated' apply to the pages as 
    /// presented to the application by this interface. This distinction is important because 
    /// internally, even pages which are referred to as 'unallocated' are indeed allocated, 
    /// but they are only allocated to the interface's implementation. Think of it this way:
    /// Unallocated pages are actually 'reserved' for allocation. This means that the internal
    /// implementation must obviously have allocated space for those unallocated (reserved) 
    /// pages.
    /// </para>
    /// <para>
    /// The total number of pages that may be allocated by the application is referred to as the
    /// <see cref="PageCapacity"/>. This is actually the number of pages that were internally allocated
    /// by the implementation. This includes 'allocated' as well as 'unallocated' pages, as presented 
    /// to the application. The <see cref="PageCapacity"/> can only be changed via the
    /// <see cref="TryInflate(long, IProgress{ProgressReport}, CancellationToken)"/> or 
    /// <see cref="TryDeflate(long, IProgress{ProgressReport}, CancellationToken)"/> methods.
    /// </para>
    /// <para>
    /// Note that some implementations may internally allocate pages as they need. When this happens, the interface
    /// will make no distinction between pages that were allocated for the internal needs of the implementation and
    /// pages that were allocated by the application. This means, for example, that <see cref="AllocatedPageCount"/>
    /// will account for the number of pages that were allocated for the internal implementation needs as well as the
    /// pages that were allocated by the application. The same is true for the <see cref="IsPageAllocated(long)"/>
    /// method. In general, the application is expected to keep track of which pages it has allocated.
    /// </para>
    /// </remarks>
    public interface IPageStorage : IDisposable
    {
        /// <summary>
        /// Is this <see cref="IPageStorage"/> read-only?
        /// </summary>
        /// <remarks>
        /// The implementation must not allow this property to change in runtime. Once
        /// the instance has been constructed, this property must maintain the same value.
        /// </remarks>
        bool IsReadOnly { get; }

        /// <summary>
        /// Is the <see cref="PageCapacity"/> a fixed constant?
        /// </summary>
        /// <remarks>
        /// <para>
        /// This property defines whether the <see cref="PageCapacity"/> is a fixed constant,
        /// meaning it cannot be changed via <see cref="TryInflate(long, IProgress{ProgressReport}, CancellationToken)"/>
        /// or <see cref="TryDeflate(long, IProgress{ProgressReport}, CancellationToken)"/>.
        /// The implementation must not allow this property to change in runtime. Once
        /// the instance has been constructed, this property must maintain the same value. However,
        /// it may change across different instances for the same payload source object (such as a file).
        /// </para>
        /// <para>
        /// If <see cref="IsReadOnly"/> is true, then this property must also be true.
        /// </para>
        /// </remarks>
        /// <seealso cref="IsReadOnly"/>
        bool IsCapacityFixed { get; }

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
        long PageCapacity { get; }

        /// <summary>
        /// The total number of pages that are currently allocated.
        /// </summary>
        /// <seealso cref="PageCapacity"/>
        /// <seealso cref="TryAllocatePage(out long)"/>
        /// <seealso cref="FreePage(long)"/>
        /// <remarks>
        /// Note that this property may potentially be larger than the number of pages
        /// that have been allocated <em>by the application</em>, since some
        /// implementations may perform some internal page allocations for their
        /// various needs. In general, the application is expected to keep track of 
        /// which pages it has allocated.
        /// </remarks>
        long AllocatedPageCount { get; }

        /// <summary>
        /// The size of each page's payload, measured in bytes.
        /// </summary>
        /// <remarks>
        /// The implementation must not allow this property to change in runtime. Once
        /// the instance has been constructed, this property must maintain the same value.
        /// Furthermore, if the implementation is based on a non-volatile storage object,
        /// such as a file, then this property must maintain the same value even across instances
        /// for the same object. For example, the implementation may read this value from a 
        /// read-only header in a file.
        /// </remarks>
        long PageSize { get; }

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
        long? EntryPageIndex { get; set; }

        /// <summary>
        /// Attempts to increase the <see cref="PageCapacity"/> by creating
        /// space for more unallocated pages.
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
        /// This method will attempt to increase the <see cref="PageCapacity"/> by creating more 
        /// unallocated pages in the storage. Calling this method will <em>not</em> affect any
        /// currently allocated pages, except potentially if an <see cref="Exception"/> is thrown,
        /// which may indicate data corruption. The capacity is only ever increased by adding more 
        /// unallocated pages to the end of the storage (because adding unallocated pages
        /// anywhere else would risk affecting the indices of some allocated pages). Note that 
        /// the implementation may be unable to create the desired number of additional 
        /// unallocated pages (<paramref name="additionalPageCount"/>), in  which case it will 
        /// only create what is possible. If the implementation is unable to create space for even 
        /// one more unallocated page, then zero will be returned. Upon cancellation via the
        /// <paramref name="cancellationToken"/>, this method will return the additional
        /// number of pages that were created before the operation was cancelled. 
        /// Cancellation will <em>not</em> cause data corruption.
        /// </para>
        /// <para>
        /// If <see cref="IsCapacityFixed"/> is true, then the implementation is required to
        /// throw an <see cref="InvalidOperationException"/>.
        /// </para>
        /// <para>
        /// If <paramref name="additionalPageCount"/> is a negative number, then the implementation
        /// will throw an <see cref="ArgumentOutOfRangeException"/> and the storage will remain
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
        long TryInflate(long additionalPageCount, IProgress<ProgressReport> progressReporter, CancellationToken cancellationToken);

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
        /// This method will attempt to decrease the <see cref="PageCapacity"/> by removing
        /// unallocated pages at the very end of the storage. Calling this method will <em>not</em>
        /// affect any currently allocated pages, except potentially if an <see cref="Exception"/> is thrown,
        /// which may indicate data corruption. The capacity is only ever decreased by
        /// removing unallocated pages from the very end of the storage (because removing
        /// unallocated pages from anywhere else would risk changing the indices of currently
        /// allocated pages). If there is not a consecutive sequence of <paramref name="removePageCount"/>
        /// unallocated pages at the very end of the storage, then the implementation will not
        /// be able to decrease the <see cref="PageCapacity"/> by that amount. In this case, the
        /// implementation will try to remove however many consecutively unallocated pages are 
        /// at the very end of the storage. If the very last page on the storage is currently
        /// allocated, then the implementation will not be able to decrease the <see cref="PageCapacity"/>
        /// by any amount, and zero will be returned.
        /// </para>
        /// <para>
        /// Upon cancellation via the <paramref name="cancellationToken"/>, this method will
        /// return the number of pages that were removed before the operation was cancelled.
        /// Cancellation will <em>not</em> cause data corruption.
        /// </para>
        /// <para>
        /// If <see cref="IsCapacityFixed"/> is true, then the implementation is required to
        /// throw an <see cref="InvalidOperationException"/>.
        /// </para>
        /// <para>
        /// If <paramref name="removePageCount"/> is a negative number, then the implementation
        /// will throw an <see cref="ArgumentOutOfRangeException"/> and the storage will remain
        /// unchanged. To increase the <see cref="PageCapacity"/>, use the 
        /// <see cref="TryInflate(long, IProgress{ProgressReport}, CancellationToken)"/> method instead.
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
        long TryDeflate(long removePageCount, IProgress<ProgressReport> progressReporter, CancellationToken cancellationToken);

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
        /// Note that some implementations may perform internal allocations for their various needs, and
        /// so this method may return true for such pages. In general, the application should keep track
        /// of which pages it has allocated.
        /// </para>
        /// </remarks>
        /// <seealso cref="IsPageOnStorage(long)"/>
        /// <seealso cref="TryAllocatePage(out long)"/>
        /// <seealso cref="FreePage(long)"/>
        bool IsPageAllocated(long index);

        /// <summary>
        /// Checks whether a page exists (regardless of whether or not it is allocated)
        /// on the storage.
        /// </summary>
        /// <param name="index">The index of the page.</param>
        /// <returns>True if <paramref name="index"/> is greater than or equal to zero and less
        /// than <see cref="PageCapacity"/>, otherwise false.</returns>
        /// <seealso cref="IsPageAllocated(long)"/>
        /// <seealso cref="PageCapacity"/>
        bool IsPageOnStorage(long index);

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
        /// The implementation is not required to initialize the payload of an allocated page, as it
        /// is expected that doing so could consume a lot of time, depending on the <see cref="PageSize"/>.
        /// For example, the 'initial payload' may simply be the data that was previously stored on
        /// the page when it was allocated at a previous time. Or, the implementation may use unallocated
        /// pages to store internal metadata.
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
        /// <seealso cref="FreePage(long)"/>
        /// <seealso cref="IsReadOnly"/>
        /// <seealso cref="IsPageAllocated(long)"/>
        /// <seealso cref="AllocatedPageCount"/>
        bool TryAllocatePage(out long index);

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
        /// Note that the implementation is <em>not</em> expected to erase any payload that has been
        /// stored by the page. If the application has written information that it wishes to 
        /// be erased, it must do so manually before calling this method. Otherwise, the payload
        /// may exist on the storage, and it may even be leaked to the application in a future 
        /// <see cref="TryAllocatePage(out long)"/> call, since that page may contain the payload 
        /// that was stored in this to-be-freed page.
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
        bool FreePage(long index);

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
        /// the <paramref name="buffer"/> size.
        /// </exception>
        /// <exception cref="ArgumentNullException">Thrown if <paramref name="buffer"/> is null.</exception>
        /// <seealso cref="WriteTo(long, long, byte[], long, long)"/>
        /// <seealso cref="IsPageAllocated(long)"/>
        void ReadFrom(long pageIndex, long srcOffset, byte[] buffer, long dstOffset, long length);

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
        /// <paramref name="length"/> is greater than the <paramref name="buffer"/> size.
        /// </exception>
        /// <exception cref="ArgumentNullException">Thrown if <paramref name="buffer"/> is null.</exception>
        /// <seealso cref="ReadFrom(long, long, byte[], long, long)"/>
        /// <seealso cref="IsReadOnly"/>
        /// <seealso cref="IsPageAllocated(long)"/>
        void WriteTo(long pageIndex, long dstOffset, byte[] buffer, long srcOffset, long length);
    }
}
