using Storage.Data;
using System;

namespace Storage
{
    sealed class CachedPage
    {
        /// <summary>
        /// The index of the page on the base <see cref="IPageStorage"/>.
        /// </summary>
        public long PageIndex { get; private set; }

        /// <summary>
        /// The base <see cref="IPageStorage"/>.
        /// </summary>
        public IPageStorage BasePageStorage { get { return CachedPageStorage.PageStorage; } }
        
        /// <summary>
        /// The <see cref="Storage.CachedPageStorage"/> to which this <see cref="CachedPage"/>
        /// belongs.
        /// </summary>
        public CachedPageStorage CachedPageStorage { get; private set; }

        /// <summary>
        /// The <see cref="CachedPageStorage.CacheWriteMode"/>.
        /// </summary>
        public CachedPageStorage.CacheWriteMode WriteMode { get { return CachedPageStorage.Mode; } }

        /// <summary>
        /// Value that is used to determine the 'most recently used' <see cref="CachedPage"/>s.
        /// Greater values are more recent.
        /// </summary>
        public long RecentUseCounter { get; private set; }

        public CachedPage(CachedPageStorage cachedPageStorage, long pageIndex)
        {
            if (cachedPageStorage == null)
                throw new ArgumentNullException(nameof(cachedPageStorage));
            if (!cachedPageStorage.PageStorage.IsPageOnStorage(pageIndex))
                throw new ArgumentOutOfRangeException(nameof(pageIndex), "The specified page index does not exist on the base " + nameof(IPageStorage) + ".");
            if (!cachedPageStorage.PageStorage.IsPageAllocated(pageIndex))
                throw new InvalidOperationException("Cannot instantiate a " + nameof(CachedPage) + " for an page that is unallocated on the base " + nameof(IPageStorage) + ".");
            if (cachedPageStorage.PageStorage.PageSize > Int32.MaxValue)//Buffer.BlockCopy, List, etc all use Int32, so enforce that upper limit
                throw new ArgumentException("The base " + nameof(IPageStorage) + " has pages that are too large to fit in a " + nameof(CachedPage) + ". The maximum page size is " + nameof(Int32) + "." + nameof(Int32.MaxValue) + ".", nameof(cachedPageStorage));
            
            this.CachedPageStorage = cachedPageStorage;
            this.PageIndex = pageIndex;
            this.cacheBuffer = new byte[cachedPageStorage.PageStorage.PageSize];
            this.readRegions = new DataRegionSet();
            this.writtenRegions = new DataRegionSet();
            this.locker = new object();
            this.UpdateUseCounter();

#if DEBUG
            if (cachedPageStorage.SimulateOutOfMemory)
                throw new OutOfMemoryException("A simulated " + nameof(OutOfMemoryException) + " has been requested.");
#endif
        }

        private static long globalRecentUseCounter = 0;

        private void UpdateUseCounter()
        {
            lock(locker)
            {
                RecentUseCounter = globalRecentUseCounter++;
            }
        }

        private readonly byte[] cacheBuffer;

        private readonly object locker;

        private DataRegionSet readRegions, writtenRegions;

        /// <summary>
        /// Reads from the page, updating cache where necessary.
        /// </summary>
        /// <param name="srcOffset">The source offset within the page's payload.</param>
        /// <param name="buffer">The destination buffer.</param>
        /// <param name="dstOffset">The offset within the destination <paramref name="buffer"/>.</param>
        /// <param name="length">The number of bytes to read.</param>
        public void Read(long srcOffset, byte[] buffer, long dstOffset, long length)
        {
            if (buffer == null)
                throw new ArgumentNullException(nameof(buffer));
            if (srcOffset < 0)
                throw new ArgumentOutOfRangeException(nameof(srcOffset), "The source offset cannot be less than zero.");
            if (dstOffset < 0)
                throw new ArgumentOutOfRangeException(nameof(dstOffset), "The destination offset cannot be less than zero.");
            if (length < 0)
                throw new ArgumentOutOfRangeException(nameof(length), "The length cannot be less than zero.");
            if (srcOffset + length > BasePageStorage.PageSize)
                throw new ArgumentOutOfRangeException(nameof(srcOffset), "The sum of the source offset and length cannot be greater than the size of each page.");
            if (dstOffset + length > buffer.Length)
                throw new ArgumentOutOfRangeException(nameof(dstOffset), "The sum of the destination offset and length cannot be greater than the size of the destination buffer.");
            
            lock (locker)
            {
                if(length != 0)
                {
                    DataRegion srcRegion = new DataRegion(srcOffset, (srcOffset + length) - 1/*-1 to go from count to index*/);

                    //Determine which DataRegions are not currently cached (if any), and load them into cache
                    foreach (var missing in readRegions.GetMissingRegions(srcRegion))
                    {
                        //Read this missing DataRegion into cache
                        BasePageStorage.ReadFrom(PageIndex, missing.FirstIndex, cacheBuffer, missing.FirstIndex, missing.Length);
                        readRegions.Add(missing);
                    }

                    //Copy the data from the cached buffer
                    Buffer.BlockCopy(cacheBuffer, (int)srcOffset, buffer, (int)dstOffset, (int)length);
                }
                //Else we don't need to do anything, read 0 bytes is meaningless
            }

            //Mark this page as more recent
            UpdateUseCounter();
        }

        public void Write(long dstOffset, byte[] buffer, long srcOffset, long length)
        {
            if (BasePageStorage.IsReadOnly)
                throw new InvalidOperationException("Cannot write to a read-only " + nameof(CachedPage) + ".");
            if (dstOffset < 0)
                throw new ArgumentOutOfRangeException(nameof(dstOffset), "The destination offset cannot be less than zero.");
            if (srcOffset < 0)
                throw new ArgumentOutOfRangeException(nameof(srcOffset), "The source offset cannot be less than zero.");
            if (length < 0)
                throw new ArgumentOutOfRangeException(nameof(length), "The length cannot be less than zero.");
            if (dstOffset + length > BasePageStorage.PageSize)
                throw new ArgumentOutOfRangeException(nameof(dstOffset), "The sum of the destination offset and length cannot be greater than the page size.");
            if (srcOffset + length > buffer.Length)
                throw new ArgumentOutOfRangeException(nameof(srcOffset), "The sum of the source offset and length cannot be greater than the size of the source buffer.");
            
            lock (locker)
            {
                DataRegion dstRegion = new DataRegion(dstOffset, (dstOffset + length) - 1/*-1 to go from count to index*/);

                //Copy the data to the cached buffer
                Buffer.BlockCopy(buffer, (int)srcOffset, cacheBuffer, (int)dstOffset, (int)length);

                //Writes affect read cache too
                readRegions.Add(dstRegion);
                
                if(WriteMode == CachedPageStorage.CacheWriteMode.WriteThrough)
                {
                    //Write directly to the base storage
                    BasePageStorage.WriteTo(PageIndex, dstOffset, buffer, srcOffset, length);
                }
                else
                {
                    //Update the 'dirty' write regions
                    writtenRegions.Add(dstRegion);
                }
            }

            //Mark this page as more recent
            UpdateUseCounter();
        }

        public void Flush()
        {
            lock(locker)
            {
                foreach (var dirtyRegion in writtenRegions)
                    BasePageStorage.WriteTo(PageIndex, dirtyRegion.FirstIndex, this.cacheBuffer, dirtyRegion.FirstIndex, dirtyRegion.Length);
            }
        }
    }
}
