using Storage.Data.Serializers;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Threading;

namespace Storage.Data
{
    /// <summary>
    /// <see cref="IStorageDictionary{TKey, TValue}"/> implementation.
    /// </summary>
    /// <typeparam name="TKey">The key type.</typeparam>
    /// <typeparam name="TValue">The value type.</typeparam>
    public sealed class StorageDictionary<TKey, TValue> : IStorageDictionary<TKey, TValue> where TKey : IComparable<TKey>
    {
        internal CachedPageStorage CachedPageStorage { get; private set; }

        internal ISerializer<TKey> KeySerializer { get; private set; }

        internal ISerializer<TValue> ValueSerializer { get; private set; }

        internal long MaxMovePairCount { get; private set; }

        internal StorageDictionaryBTree<TKey, TValue> BTree { get; private set; }

        private object locker = new object();

        /// <summary>
        /// The index of the metadata page on the <see cref="IPageStorage"/>.
        /// </summary>
        /// <remarks>
        /// The application is expected to keep track of this index so that it may
        /// load the <see cref="StorageDictionary{TKey, TValue}"/> in the future.
        /// </remarks>
        public long PageIndex { get; private set; }

        /// <summary>
        /// Is this <see cref="StorageDictionary{TKey, TValue}"/> read-only?
        /// </summary>
        public bool IsReadOnly { get; private set; }

        /// <summary>
        /// The number of key-value pairs.
        /// </summary>
        public long Count { get { return BTree.Count; } }

        /// <summary>
        /// The minimum number of key-value pairs that can be stored in a <see cref="BTreeNode{TKey, TValue}"/>.
        /// </summary>
        /// <remarks>
        /// Note that this is the <em>very minimum</em> capacity. Practical applications should use a much higher
        /// capacity, as using this amount is likely to be woefully inefficient.
        /// </remarks>
        public const long MinKeyValuePairCountPerNode = BTreeNode<TKey, TValue>.VeryMinKeyValuePairCapacity;

        /// <summary>
        /// Gets the page size that is required to support a <see cref="StorageDictionary{TKey, TValue}"/>.
        /// </summary>
        /// <param name="keySize">The <see cref="ISerializer{T}.DataSize"/> of the key, measured in bytes.</param>
        /// <param name="valueSize">The <see cref="ISerializer{T}.DataSize"/> of the value, measured in bytes.</param>
        /// <param name="keyValuePairCapacityPerNode">The maximum number of key-value pairs to internally store
        /// in a <see cref="BTreeNode{TKey, TValue}"/>. Must be an odd number, and must not be less than
        /// <see cref="MinKeyValuePairCountPerNode"/>.</param>
        /// <returns>The required page size, measured in bytes.</returns>
        /// <exception cref="ArgumentOutOfRangeException">Thrown if <paramref name="keySize"/> is less than one,
        /// <paramref name="valueSize"/> is less than zero, or <paramref name="keyValuePairCapacityPerNode"/>
        /// is less than <see cref="MinKeyValuePairCountPerNode"/>.</exception>
        /// <exception cref="ArgumentException">Thrown if <paramref name="keyValuePairCapacityPerNode"/>
        /// is an even number.</exception>
        public static long GetRequiredPageSize(long keySize, long valueSize, long keyValuePairCapacityPerNode)
        {
            if (keySize < 1)
                throw new ArgumentOutOfRangeException(nameof(keySize), "The key size cannot be less than one byte.");
            if (valueSize < 0)
                throw new ArgumentOutOfRangeException(nameof(valueSize), "The value size cannot be less than zero.");
            if (keyValuePairCapacityPerNode < MinKeyValuePairCountPerNode)
                throw new ArgumentOutOfRangeException(nameof(keyValuePairCapacityPerNode), "The key-value pair capacity per node cannot be less than " + nameof(MinKeyValuePairCountPerNode) + ".");
            if (keyValuePairCapacityPerNode % 2 == 0)
                throw new ArgumentException("The key-value pair capacity per node must be an odd number.", nameof(keyValuePairCapacityPerNode));

            return BTreeNode<TKey, TValue>.GetRequiredPageSize(keySize, valueSize, keyValuePairCapacityPerNode);
        }

        /// <summary>
        /// Gets the very minimum required page size.
        /// </summary>
        /// <param name="keySize">The <see cref="ISerializer{T}.DataSize"/> of the key, measured in bytes.</param>
        /// <param name="valueSize">The <see cref="ISerializer{T}.DataSize"/> of the value, measured in bytes.</param>
        /// <returns>The minimum page size, measured in bytes.</returns>
        /// <remarks>
        /// Note that this is based on <see cref="MinKeyValuePairCountPerNode"/>, which is likely to be inefficient
        /// for most applications.
        /// </remarks>
        /// <seealso cref="GetRequiredPageSize(long, long, long)"/>
        /// <seealso cref="MinKeyValuePairCountPerNode"/>
        public static long GetVeryMinRequiredPageSize(long keySize, long valueSize)
        {
            if (keySize < 1)
                throw new ArgumentOutOfRangeException(nameof(keySize), "The key size cannot be less than one byte.");
            if (valueSize < 0)
                throw new ArgumentOutOfRangeException(nameof(valueSize), "The value size cannot be less than zero.");

            return GetRequiredPageSize(keySize, valueSize, MinKeyValuePairCountPerNode);
        }

        /// <summary>
        /// Attempts to create an empty <see cref="StorageDictionary{TKey, TValue}"/> using an <see cref="IPageStorage"/>.
        /// </summary>
        /// <param name="pageStorage">The <see cref="IPageStorage"/> on which the <see cref="StorageDictionary{TKey, TValue}"/>
        /// will store its data.</param>
        /// <param name="keySerializer"><see cref="ISerializer{T}"/> that serializes and deserializes the keys.</param>
        /// <param name="valueSerializer"><see cref="ISerializer{T}"/> that serializes and deserializes the values.</param>
        /// <param name="pageIndex">Assigned to the index of the metadata page that the application must remember so that it
        /// may load the <see cref="StorageDictionary{TKey, TValue}"/> in the future. See <see cref="PageIndex"/>.</param>
        /// <returns>True if creation was successful, false if allocation failed.</returns>
        /// <exception cref="ArgumentNullException">Thrown if <paramref name="pageStorage"/>, <paramref name="keySerializer"/>,
        /// or <paramref name="valueSerializer"/> is null.</exception>
        /// <exception cref="InvalidOperationException">Thrown if <paramref name="pageStorage"/> is read-only.</exception>
        /// <exception cref="ArgumentException">Thrown if the <paramref name="pageStorage"/>'s <see cref="IPageStorage.PageSize"/>
        /// is less than the very minimum required size (see <see cref="GetVeryMinRequiredPageSize(long, long)"/>).</exception>
        /// <remarks>
        /// <para>
        /// This method will only initialize the data stored on the <paramref name="pageStorage"/>. To obtain the
        /// <see cref="StorageDictionary{TKey, TValue}"/> instance, use the <see cref="Load(IPageStorage, ISerializer{TKey}, ISerializer{TValue}, long, bool, int, long)"/>
        /// method (and remember to call <see cref="StorageDictionary{TKey, TValue}.Dispose()"/> when finished
        /// using it).
        /// </para>
        /// <para>
        /// Note that this method will inflate the <paramref name="pageStorage"/> if it is at full capacity and not fixed-capacity.
        /// If inflation is required but the capacity is fixed, or if inflation fails, false will be returned.
        /// </para>
        /// </remarks>
        /// <seealso cref="Load(IPageStorage, ISerializer{TKey}, ISerializer{TValue}, long, bool, int, long)"/>
        public static bool TryCreate(IPageStorage pageStorage, ISerializer<TKey> keySerializer, ISerializer<TValue> valueSerializer, out long pageIndex)
        {
            if (pageStorage == null)
                throw new ArgumentNullException(nameof(pageStorage));
            if (keySerializer == null)
                throw new ArgumentNullException(nameof(keySerializer));
            if (valueSerializer == null)
                throw new ArgumentNullException(nameof(valueSerializer));
            if (pageStorage.IsReadOnly)
                throw new InvalidOperationException("Cannot create a " + nameof(StorageDictionary<TKey, TValue>) + " on a read-only " + nameof(IPageStorage) + ".");
            if (pageStorage.PageSize < GetVeryMinRequiredPageSize(keySerializer.DataSize, valueSerializer.DataSize))
                throw new ArgumentException("The page size of the specified " + nameof(IPageStorage) + " is too small.", nameof(pageStorage));

            if(pageStorage.AllocatedPageCount == pageStorage.PageCapacity)
            {
                if(!pageStorage.IsCapacityFixed)
                {
                    long got = pageStorage.TryInflate(1, null, new CancellationToken(false));
                    if(got < 0)
                    {
                        //Cannot allocate, capacity remains full
                        pageIndex = -1;
                        return false;
                    }
                }
                else
                {
                    //Capacity is full and we cannot inflate it
                    pageIndex = -1;
                    return false;
                }
            }

            if(pageStorage.TryAllocatePage(out pageIndex))
            {
                using (StorageDictionary<TKey, TValue> dict = new StorageDictionary<TKey, TValue>(pageStorage, keySerializer, valueSerializer, pageIndex, false, 0, 256/*not used for creation, but must be >0*/))
                {
                    dict.BTree.InitializeEmpty();
                    dict.CachedPageStorage.Flush();
                }
                return true;
            }
            else
            {
                pageIndex = -1;
                return false;
            }
        }

        /// <summary>
        /// Loads a <see cref="StorageDictionary{TKey, TValue}"/> from an <see cref="IPageStorage"/>.
        /// </summary>
        /// <param name="pageStorage">The <see cref="IPageStorage"/> from which to load. This
        /// <see cref="StorageDictionary{TKey, TValue}"/> will <em>not</em> dispose it (because it
        /// is possible that other APIs may also be using the <see cref="IPageStorage"/>).</param>
        /// <param name="keySerializer"><see cref="ISerializer{T}"/> that serializes and deserializes the keys.</param>
        /// <param name="valueSerializer"><see cref="ISerializer{T}"/> that serializes and deserializes the values.</param>
        /// <param name="pageIndex">The index of the page on the <paramref name="pageStorage"/> that contains the
        /// metadata for the <see cref="StorageDictionary{TKey, TValue}"/>. This was determined during creation via
        /// the <see cref="TryCreate(IPageStorage, ISerializer{TKey}, ISerializer{TValue}, out long)"/>
        /// method.</param>
        /// <param name="isReadOnly">Should the <see cref="StorageDictionary{TKey, TValue}"/> be loaded as read-only? Must be true
        /// if the <paramref name="pageStorage"/> is read-only.</param>
        /// <param name="cachePageCount">The maximum number of pages to store in cache. May be zero, in which case caching
        /// will not be used. If <paramref name="pageStorage"/> already provides caching, then this should be zero.</param>
        /// <param name="maxMoveCount">The maximum number of key-value pairs to move at once during certain operations.
        /// Larger values may improve operation speed, but cost more memory. The memory cost is equivalent to the
        /// <see cref="ISerializer{T}.DataSize"/> of the <paramref name="keySerializer"/> and the <paramref name="valueSerializer"/>,
        /// plus 8-bytes of header data, all multiplied by this argument.</param>
        /// <returns>The loaded <see cref="StorageDictionary{TKey, TValue}"/>.</returns>
        /// <exception cref="ArgumentNullException">Thrown if any argument is null.</exception>
        /// <exception cref="ArgumentOutOfRangeException">Thrown if <paramref name="cachePageCount"/> is less than zero or
        /// <paramref name="maxMoveCount"/> is less than one.</exception>
        /// <exception cref="ArgumentException">Thrown if <paramref name="pageStorage"/> is read-only but
        /// <paramref name="isReadOnly"/> is false, or if the page on <paramref name="pageStorage"/> at
        /// <paramref name="pageIndex"/> is not allocated or does not exist, or if the size of the pages on
        /// the <paramref name="pageStorage"/> is less than the very minimum valid page size.</exception>
        public static StorageDictionary<TKey, TValue> Load(IPageStorage pageStorage, ISerializer<TKey> keySerializer, ISerializer<TValue> valueSerializer, long pageIndex, bool isReadOnly, int cachePageCount = 8, long maxMoveCount = 256)
        {
            if (pageStorage == null)
                throw new ArgumentNullException(nameof(pageStorage));
            if (keySerializer == null)
                throw new ArgumentNullException(nameof(keySerializer));
            if (valueSerializer == null)
                throw new ArgumentNullException(nameof(valueSerializer));
            if (cachePageCount < 0)
                throw new ArgumentOutOfRangeException(nameof(cachePageCount));
            if (maxMoveCount < 1)
                throw new ArgumentOutOfRangeException(nameof(maxMoveCount), nameof(maxMoveCount) + " cannot be less than one.");
            if (pageStorage.IsReadOnly && !isReadOnly)
                throw new ArgumentException(nameof(isReadOnly), "If the " + nameof(IPageStorage) + " is read-only, the " + nameof(isReadOnly) + " argument must be true.");
            if (!pageStorage.IsPageOnStorage(pageIndex))
                throw new ArgumentException("The specified index does not refer to a page that exists on the " + nameof(IPageStorage) + ".", nameof(pageIndex));
            if (!pageStorage.IsPageAllocated(pageIndex))
                throw new ArgumentException("The specified index does not refer to a page that is allocated on the " + nameof(IPageStorage) + ".", nameof(pageIndex));
            if (pageStorage.PageSize < GetVeryMinRequiredPageSize(keySerializer.DataSize, valueSerializer.DataSize))
                throw new ArgumentException("The page size of the specified " + nameof(IPageStorage) + " is too small.", nameof(pageStorage));

            return new StorageDictionary<TKey, TValue>(pageStorage, keySerializer, valueSerializer, pageIndex, isReadOnly, cachePageCount, maxMoveCount);
        }

        private StorageDictionary(IPageStorage pageStorage, ISerializer<TKey> keySerializer, ISerializer<TValue> valueSerializer, long pageIndex, bool isReadOnly, int cachePageCount, long maxMoveCount)
        {
            this.CachedPageStorage = new CachedPageStorage(pageStorage, isReadOnly ? CachedPageStorage.CacheWriteMode.ReadOnly : CachedPageStorage.CacheWriteMode.WriteBack, cachePageCount, true);
            this.KeySerializer = keySerializer ?? throw new ArgumentNullException(nameof(keySerializer));
            this.ValueSerializer = valueSerializer ?? throw new ArgumentNullException(nameof(valueSerializer));
            this.PageIndex = pageIndex;
            this.IsReadOnly = isReadOnly;
            this.MaxMovePairCount = maxMoveCount;
            this.BTree = new StorageDictionaryBTree<TKey, TValue>(this);
        }
        
        /// <summary>
        /// Gets the value associated to a specific key, if it exists.
        /// </summary>
        /// <param name="key">The key.</param>
        /// <param name="valueOrDefault">Assigned to the associated value, or the default
        /// <typeparamref name="TValue"/> value if the key was not found.</param>
        /// <returns>True if the key was found, otherwise false.</returns>
        public bool TryGetValue(TKey key, out TValue valueOrDefault)
        {
            lock(locker)
            {
                return BTree.TryGetValue(key, out valueOrDefault, new CancellationToken(false));
            }
        }

        /// <summary>
        /// Adds a new key-value pair.
        /// </summary>
        /// <param name="key">The key.</param>
        /// <param name="value">The value.</param>
        /// <returns>True if addition was successful, false if it failed only due to capacity limitations
        /// on the base <see cref="IPageStorage"/>.</returns>
        /// <exception cref="InvalidOperationException">Thrown if <see cref="IsReadOnly"/> is true or if this
        /// method is called from a thread that is currently enumerating via <see cref="GetEnumerator"/>.</exception>
        /// <exception cref="ArgumentException">Thrown if the <paramref name="key"/> already exists in
        /// this <see cref="StorageDictionary{TKey, TValue}"/>.</exception>
        /// <remarks>
        /// This method will only add a new key-value pair, it will not update an existing one.
        /// The base <see cref="IPageStorage"/> may be inflated by this method if necessary (unless
        /// it is fixed-capacity). If inflation is required, but not possible or fails, then the
        /// new key-value pair will not be added and false will be returned (this is considered
        /// a graceful failure). If there is already a key-value pair with the specified
        /// <paramref name="key"/>, then an <see cref="ArgumentException"/> will be thrown and
        /// nothing will change. Any other failure will be reported by an <see cref="Exception"/>
        /// (of any type) being thrown, and may indicate data corruption.
        /// </remarks>
        public bool TryAdd(TKey key, TValue value)
        {
            if (IsReadOnly)
                throw new InvalidOperationException("Cannot add to a read-only " + nameof(StorageDictionary<TKey, TValue>) + ".");

            lock(locker)
            {
                if (isEnumerating_)
                    throw new InvalidOperationException("Cannot add a key-value pair on the same thread that is currently enumerating this " + nameof(StorageDictionary<TKey, TValue>) + ".");

                if (BTree.Insert(key, value, false, out bool alreadyExists))
                {
                    CachedPageStorage.Flush();//Ensure that data is written to base storage now
                    return true;
                }
                else
                {
                    if (alreadyExists)
                        throw new ArgumentException("A key-value pair with the specified key already exists.", nameof(key));
                    else
                        return false;//Insert failed due to storage limitation
                }
            }
        }

        /// <summary>
        /// Adds a new key-value pair, or updates an existing one.
        /// </summary>
        /// <param name="key">The key.</param>
        /// <param name="value">The value.</param>
        /// <param name="alreadyExists">Assigned to true if an existing key-value pair was
        /// updated, otherwise false. This should be considered undefined if false is returned.</param>
        /// <returns>True if the new key-value pair was added, or an existing one was updated.
        /// False only indicates failure to add a new key-value pair due to capacity limitations
        /// on the base <see cref="IPageStorage"/>.</returns>
        /// <exception cref="InvalidOperationException">Thrown if <see cref="IsReadOnly"/> is true or if this
        /// method is called from a thread that is currently enumerating via <see cref="GetEnumerator"/>.</exception>
        /// <remarks>
        /// This method will check whether a key-value pair already exists with the specified 
        /// <paramref name="key"/>. If it does, then the value will be updated. Otherwise, a new key-value
        /// pair will be added. Addition of a new key-value pair may fail (as documented in the
        /// <see cref="TryAdd(TKey, TValue)"/> method's remarks) if inflation of the base
        /// <see cref="IPageStorage"/> fails. In this case, false will be returned and
        /// no key-value pairs will be changed (this is considered a graceful failure).
        /// </remarks>
        public bool TryAddOrUpdate(TKey key, TValue value, out bool alreadyExists)
        {
            if (IsReadOnly)
                throw new InvalidOperationException("Cannot add a new key-value pair to, or update an existing one on, a read-only " + nameof(StorageDictionary<TKey, TValue>) + ".");

            lock(locker)
            {
                if (isEnumerating_)
                    throw new InvalidOperationException("Cannot add or update a key-value pair on the same thread that is currently enumerating this " + nameof(StorageDictionary<TKey, TValue>) + ".");

                if (BTree.Insert(key, value, true, out alreadyExists))
                {
                    CachedPageStorage.Flush();//Ensure that data is written to base storage now
                    return true;
                }
                else
                {
                    return false;
                }
            }
        }

        /// <summary>
        /// Updates the value of an existing key-value pair.
        /// </summary>
        /// <param name="key">The key.</param>
        /// <param name="value">The value to assign.</param>
        /// <exception cref="InvalidOperationException">Thrown if <see cref="IsReadOnly"/> is true or if this
        /// method is called from a thread that is currently enumerating via <see cref="GetEnumerator"/>.</exception>
        /// <exception cref="KeyNotFoundException">Thrown if there is no existing key-value pair with the
        /// specified <paramref name="key"/>.</exception>
        public void UpdateValue(TKey key, TValue value)
        {
            if (IsReadOnly)
                throw new InvalidOperationException("Cannot update any key-value pair in a read-only " + nameof(StorageDictionary<TKey, TValue>) + ".");

            lock(locker)
            {
                if (isEnumerating_)
                    throw new InvalidOperationException("Cannot update a key-value pair on the same thread that is currently enumerating this " + nameof(StorageDictionary<TKey, TValue>) + ".");

                if (BTree.TryGetValueOnNode(key, out _, out var onNode, out long indexOnNode, new CancellationToken(false)))
                {
                    onNode.SetValueAt(indexOnNode, value);
                    CachedPageStorage.Flush();//Ensure that data is written to base storage now
                }
                else
                {
                    throw new KeyNotFoundException("There is no key-value pair with the specified key.");
                }
            }
        }

        /// <summary>
        /// Checks whether this <see cref="StorageDictionary{TKey, TValue}"/> contains a key-value pair
        /// with a specific key.
        /// </summary>
        /// <param name="key">The key to find.</param>
        /// <returns>True if this <see cref="StorageDictionary{TKey, TValue}"/> contains the specified
        /// <paramref name="key"/>, otherwise false.</returns>
        public bool ContainsKey(TKey key)
        {
            lock(locker)
            {
                return BTree.ContainsKey(key, new CancellationToken(false));
            }
        }

        /// <summary>
        /// Removes a key-value pair.
        /// </summary>
        /// <param name="key">The key of the key-value pair to remove.</param>
        /// <param name="removedValueOrDefault">Assigned to the value of the removed key-value pair,
        /// or default <typeparamref name="TValue"/> if it did not exist.</param>
        /// <returns>True if the key-value pair was removed, false if it did not exist.</returns>
        /// <exception cref="InvalidOperationException">Thrown if <see cref="IsReadOnly"/> is true or if this
        /// method is called from a thread that is currently enumerating via <see cref="GetEnumerator"/>.</exception>
        public bool Remove(TKey key, out TValue removedValueOrDefault)
        {
            if (IsReadOnly)
                throw new InvalidOperationException("Cannot remove any key-value pair from a read-only " + nameof(StorageDictionary<TKey, TValue>) + ".");

            lock(locker)
            {
                if (isEnumerating_)
                    throw new InvalidOperationException("Cannot remove a key-value pair on the same thread that is currently enumerating this " + nameof(StorageDictionary<TKey, TValue>) + ".");

                if (BTree.Remove(key, out removedValueOrDefault))
                {
                    CachedPageStorage.Flush();//Ensure that data is written to base storage now
                    return true;
                }
                else
                {
                    return false;
                }
            }
        }

        private bool isEnumerating_ = false;
        /// <summary>
        /// Enumerates through all key-value pairs, in ascending key order.
        /// </summary>
        /// <returns>All <see cref="KeyValuePair{TKey, TValue}"/>s stored in this
        /// <see cref="StorageDictionary{TKey, TValue}"/>, in ascending key order.</returns>
        public IEnumerator<KeyValuePair<TKey, TValue>> GetEnumerator()
        {
            lock(locker)
            {
                isEnumerating_ = true;
                try
                {
                    foreach (var pair in BTree.Traverse(true))
                        yield return pair;
                }
                finally
                {
                    isEnumerating_ = false;
                }
            }
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        /// <summary>
        /// Gets the size of auxiliary data that can be stored in the metadata page of a
        /// <see cref="StorageDictionary{TKey, TValue}"/>.
        /// </summary>
        /// <param name="pageSize">The size, in bytes, of each page on the
        /// <see cref="IPageStorage"/>.</param>
        /// <returns>The size, in bytes, of the auxiliary data.</returns>
        /// <remarks>
        /// For more information on auxiliary data, see <see cref="AuxDataSize"/>.
        /// </remarks>
        public static long GetAuxDataSize(long pageSize)
        {
            return StorageDictionaryBTree<TKey, TValue>.GetAuxDataSize(pageSize);
        }

        /// <summary>
        /// The size, in auxiliary data, that can be stored in this <see cref="StorageDictionary{TKey, TValue}"/>'s
        /// "metadata" page.
        /// </summary>
        /// <remarks>
        /// <para>
        /// Each <see cref="StorageDictionary{TKey, TValue}"/> requires an entire page to be allocated on the
        /// <see cref="IPageStorage"/> to store metadata. The required metadata size is rather small, but each
        /// page is expected to be considerably larger. To avoid wasting that extra space, applications can
        /// store auxiliary data in it. The size of the auxiliary data is determined by this property.
        /// Applications should assume that it could be as low as zero, hence it is considered
        /// "auxiliary." Once a <see cref="StorageDictionary{TKey, TValue}"/> has been created
        /// in the <see cref="IPageStorage"/>, its auxiliary data size remains constant.
        /// </para>
        /// <para>
        /// Note that auxiliary data is considered undefined until it is written by the application.
        /// </para>
        /// </remarks>
        /// <seealso cref="ReadAuxData(long, byte[], long, long)"/>
        /// <seealso cref="WriteAuxData(long, byte[], long, long)"/>
        public long AuxDataSize
        {
            get
            {
                return BTree.AuxDataSize;
            }
        }

        /// <summary>
        /// Reads from the application-defined auxiliary data that is stored in this <see cref="StorageDictionary{TKey, TValue}"/>'s
        /// metadata page.
        /// </summary>
        /// <param name="srcOffset">The source offset.</param>
        /// <param name="buffer">The destination buffer.</param>
        /// <param name="dstOffset">The destination offset within the <paramref name="buffer"/>.</param>
        /// <param name="length">The number of bytes to read.</param>
        /// <remarks>
        /// See <see cref="AuxDataSize"/> for more information on auxiliary data.
        /// </remarks>
        /// <seealso cref="WriteAuxData(long, byte[], long, long)"/>
        /// <seealso cref="AuxDataSize"/>
        /// <exception cref="ArgumentNullException">Thrown if <paramref name="buffer"/> is null.</exception>
        /// <exception cref="ArgumentOutOfRangeException">Thrown if any arguments are invalid.</exception>
        public void ReadAuxData(long srcOffset, byte[] buffer, long dstOffset, long length)
        {
            if (buffer == null)
                throw new ArgumentNullException(nameof(buffer));
            if (srcOffset < 0)
                throw new ArgumentOutOfRangeException(nameof(srcOffset), "The source offset cannot be less than zero.");
            if (dstOffset < 0)
                throw new ArgumentOutOfRangeException(nameof(dstOffset), "The destination offset cannot be less than zero.");
            if (length < 0)
                throw new ArgumentOutOfRangeException(nameof(length), "The length cannot be less than zero.");
            if (srcOffset + length > AuxDataSize)
                throw new ArgumentOutOfRangeException(nameof(srcOffset), "The sum of the source offset and length cannot be greater than " + nameof(AuxDataSize) + ".");
            if (dstOffset + length > buffer.Length)
                throw new ArgumentOutOfRangeException(nameof(dstOffset), "The sum of the destination offset and length cannot be greater than the size of the destination buffer.");

            BTree.ReadAuxData(srcOffset, buffer, dstOffset, length);
        }

        /// <summary>
        /// Writes to the application-defined auxiliary data that is stored in this <see cref="StorageDictionary{TKey, TValue}"/>'s
        /// metadata page.
        /// </summary>
        /// <param name="dstOffset">The destination offset.</param>
        /// <param name="buffer">The source buffer.</param>
        /// <param name="srcOffset">The source offset within the <paramref name="buffer"/>.</param>
        /// <param name="length">The number of bytes to write.</param>
        /// <remarks>
        /// See <see cref="AuxDataSize"/> for more information on auxiliary data.
        /// </remarks>
        /// <seealso cref="ReadAuxData(long, byte[], long, long)"/>
        /// <seealso cref="AuxDataSize"/>
        /// <exception cref="ArgumentNullException">Thrown if <paramref name="buffer"/> is null.</exception>
        /// <exception cref="ArgumentOutOfRangeException">Thrown if any arguments are invalid.</exception>
        /// <exception cref="InvalidOperationException">Thrown if <see cref="IsReadOnly"/> is true.</exception>
        public void WriteAuxData(long dstOffset, byte[] buffer, long srcOffset, long length)
        {
            if (IsReadOnly)
                throw new InvalidOperationException("Cannot write auxiliary data to a read-only " + nameof(StorageDictionary<long, string>) + ".");
            if (buffer == null)
                throw new ArgumentNullException(nameof(buffer));
            if (dstOffset < 0)
                throw new ArgumentOutOfRangeException(nameof(dstOffset), "The destination offset cannot be less than zero.");
            if (srcOffset < 0)
                throw new ArgumentOutOfRangeException(nameof(srcOffset), "The source offset cannot be less than zero.");
            if (length < 0)
                throw new ArgumentOutOfRangeException(nameof(length), "The length cannot be less than zero.");
            if (dstOffset + length > AuxDataSize)
                throw new ArgumentOutOfRangeException(nameof(dstOffset), "The sum of the destination offset and length cannot be greater than " + nameof(AuxDataSize) + ".");
            if (srcOffset + length > buffer.Length)
                throw new ArgumentOutOfRangeException(nameof(srcOffset), "The sum of the source offset and length cannot be greater than the size of the source buffer.");

            BTree.WriteAuxData(dstOffset, buffer, srcOffset, length);
            ((CachedPageStorage)BTree.PageStorage).Flush();
        }

        /// <summary>
        /// Has this <see cref="StorageDictionary{TKey, TValue}"/> been disposed?
        /// </summary>
        public bool IsDisposed { get; private set; } = false;

        private void Dispose(bool disposing)
        {
            if (!IsDisposed)
            {
                if (disposing)
                {
                    CachedPageStorage.Dispose();
                }
                
                IsDisposed = true;
            }
        }
        
        /// <summary>
        /// Disposes this <see cref="StorageDictionary{TKey, TValue}"/>.
        /// </summary>
        /// <remarks>
        /// Note that this will <em>not</em> dispose the base <see cref="IPageStorage"/>, since
        /// it is assumed that multiple APIs may be using it.
        /// </remarks>
        public void Dispose()
        {
            Dispose(true);
        }
    }
}
