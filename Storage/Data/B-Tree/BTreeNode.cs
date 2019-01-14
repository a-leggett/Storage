using Storage.Algorithms;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;

[assembly: InternalsVisibleTo("StorageTest")]
namespace Storage.Data
{
    /// <summary>
    /// Stores several key-value pairs and references to sub-trees for a B-Tree node.
    /// </summary>
    /// <typeparam name="TKey">The type of the key.</typeparam>
    /// <typeparam name="TValue">The type of the value.</typeparam>
    public sealed class BTreeNode<TKey, TValue> : IBinarySearchable<TKey, TValue>,
        IEnumerable<KeyValuePair<TKey, TValue>> where TKey : IComparable<TKey>
    {
        /// <summary>
        /// The minimum capacity of a <see cref="BTreeNode{TKey, TValue}"/>, measured in the
        /// number of key-value pairs.
        /// </summary>
        /// <remarks>
        /// In practice, using a <see cref="BTreeNode{TKey, TValue}"/> with this capacity is
        /// likely to be woefully inefficient.
        /// </remarks>
        public const long VeryMinKeyValuePairCapacity = 5;

        private const long HeaderKeyValuePairCountPosition = 0;
        private const long HeaderIsLeafFlagPosition = HeaderKeyValuePairCountPosition + sizeof(Int64);
        private const long HeaderFirstSubTreeRootNodePageIndexPosition = HeaderIsLeafFlagPosition + 1;
        private const long HeaderSize = HeaderFirstSubTreeRootNodePageIndexPosition + sizeof(Int64);

        private const bool StoreAsLittleEndian = StreamingPageStorage.StoreAsLittleEndian;

        internal static long GetElementSize(long keySize, long valueSize)
        {
            return keySize + valueSize + sizeof(Int64)/*Index of the right sub-tree root node's page index*/;
        }

        /// <summary>
        /// Gets the minimum size of an <see cref="IPageStorage"/> that is required to store a specific
        /// capacity of key-value pairs.
        /// </summary>
        /// <param name="keySize">The size, in bytes, of the binary-serialized key. Must be at least
        /// one byte.</param>
        /// <param name="valueSize">The size, in bytes, of the binary-serialized value. Cannot be less
        /// than zero.</param>
        /// <param name="keyValuePairCapacity">The desired key-value pair capacity. This is the
        /// maximum number of key-value pairs that can be stored in the <see cref="BTreeNode{TKey, TValue}"/>.
        /// Must be at least <see cref="VeryMinKeyValuePairCapacity"/>, and must be an odd number.</param>
        /// <returns>The required page size, measured in bytes.</returns>
        /// <exception cref="ArgumentOutOfRangeException">Thrown if <paramref name="keySize"/> is less than
        /// one, if <paramref name="valueSize"/> is less than zero, or if <paramref name="keyValuePairCapacity"/>
        /// is less than <see cref="VeryMinKeyValuePairCapacity"/>.</exception>
        /// <exception cref="ArgumentException">Thrown if <paramref name="keyValuePairCapacity"/> is not an odd 
        /// number.</exception>
        /// <seealso cref="VeryMinKeyValuePairCapacity"/>
        /// <seealso cref="GetKeyValuePairCapacityForPageSize(long, long, long)"/>
        public static long GetRequiredPageSize(long keySize, long valueSize, long keyValuePairCapacity)
        {
            if (keySize < 1)
                throw new ArgumentOutOfRangeException(nameof(keySize), "The key size cannot be less than one byte.");
            if (valueSize < 0)
                throw new ArgumentOutOfRangeException(nameof(valueSize), "The value size cannot be less than zero.");
            if (keyValuePairCapacity < VeryMinKeyValuePairCapacity)
                throw new ArgumentOutOfRangeException(nameof(keyValuePairCapacity), "The key-value pair capacity cannot be less than " + nameof(VeryMinKeyValuePairCapacity) + ".");
            if (keyValuePairCapacity % 2 == 0)
                throw new ArgumentException("The key-value pair capacity must be an odd number.", nameof(keyValuePairCapacity));

            return HeaderSize + (GetElementSize(keySize, valueSize) * keyValuePairCapacity);
        }
        
        /// <summary>
        /// Gets the capacity of a <see cref="BTreeNode{TKey, TValue}"/> that can be stored on a page of a specific size.
        /// </summary>
        /// <param name="pageSize">The size, in bytes, of the page.</param>
        /// <param name="keySize">The size, in bytes, of the binary-serialized key. Cannot be less than
        /// one.</param>
        /// <param name="valueSize">The size, in bytes, of the binary-serialized value. Cannot be less
        /// than zero.</param>
        /// <returns>The maximum number of key-value pairs that can be stored in a <see cref="BTreeNode{TKey, TValue}"/>
        /// that is stored on a page of the specified <paramref name="pageSize"/>.</returns>
        /// <exception cref="ArgumentOutOfRangeException">Thrown if <paramref name="keySize"/> is less than
        /// one, or <paramref name="valueSize"/> is less than zero.</exception>
        /// <remarks>
        /// If the <paramref name="pageSize"/> is not sufficient for the minimum possible key-value pair
        /// capacity (see <see cref="VeryMinKeyValuePairCapacity"/>), then zero is returned. Also note that
        /// a <see cref="BTreeNode{TKey, TValue}"/>'s key-value pair capacity must be an odd number. So if
        /// the <paramref name="pageSize"/> could theoretically store an even number of key-value pairs, this
        /// method will subtract one to enforce the odd number requirement.
        /// </remarks>
        /// <seealso cref="VeryMinKeyValuePairCapacity"/>
        /// <seealso cref="GetRequiredPageSize(long, long, long)"/>
        public static long GetKeyValuePairCapacityForPageSize(long pageSize, long keySize, long valueSize)
        {
            if (keySize < 1)
                throw new ArgumentOutOfRangeException(nameof(keySize), "The key size cannot be less than one byte.");
            if (valueSize < 0)
                throw new ArgumentOutOfRangeException(nameof(valueSize), "The value size cannot be less than zero.");

            long veryMinRequiredSize = GetRequiredPageSize(keySize, valueSize, VeryMinKeyValuePairCapacity);
            if (pageSize < veryMinRequiredSize)
                return 0;//The page size is not sufficient
            
            long available = pageSize - HeaderSize;
            long keyValuePairCount = available / GetElementSize(keySize, valueSize);
            
            if(keyValuePairCount % 2 == 0)
                keyValuePairCount--;//A BTreeNode can only store an odd number of key-value pairs

            return keyValuePairCount;
        }

        /// <summary>
        /// The index on the <see cref="IPageStorage"/> where this <see cref="BTreeNode{TKey, TValue}"/>
        /// is stored.
        /// </summary>
        public long PageIndex { get; private set; }

        /// <summary>
        /// The <see cref="BTree{TKey, TValue}"/> to which this <see cref="BTreeNode{TKey, TValue}"/> belongs.
        /// </summary>
        public BTree<TKey, TValue> BTree { get; private set; }

        private IPageStorage PageStorage { get { return BTree.PageStorage; } }

        internal long KeySize { get { return BTree.KeySerializer.DataSize; } }

        internal long ValueSize { get { return BTree.ValueSerializer.DataSize; } }

        /// <summary>
        /// The maximum number of key-value pairs that can be stored in this <see cref="BTreeNode{TKey, TValue}"/>.
        /// </summary>
        /// <seealso cref="GetKeyValuePairCapacityForPageSize(long, long, long)"/>
        public long MaxKeyValuePairCapacity
        {
            get
            {
                return GetKeyValuePairCapacityForPageSize(PageStorage.PageSize, KeySize, ValueSize);
            }
        }
        
        /// <summary>
        /// The minimum number of key-value pairs that can be stored in this <see cref="BTreeNode{TKey, TValue}"/> (unless
        /// this is the root node).
        /// </summary>
        /// <remarks>
        /// This property does <em>not</em> apply to the root node of the <see cref="BTree{TKey, TValue}"/>, which
        /// can have as few as necessary.
        /// </remarks>
        public long MinKeyValuePairCapacity
        {
            get
            {
                return MaxKeyValuePairCapacity / 2;//Since 'max' is odd, this will round down
            }
        }

        private long? cachedKeyValuePairCount_ = null;
        /// <summary>
        /// The number of key-value pairs that are currently stored in this <see cref="BTreeNode{TKey, TValue}"/>.
        /// </summary>
        public long KeyValuePairCount
        {
            get
            {
                if(cachedKeyValuePairCount_ == null)
                {
                    byte[] buffer = new byte[sizeof(Int64)];
                    PageStorage.ReadFrom(PageIndex, HeaderKeyValuePairCountPosition, buffer, 0, buffer.Length);
                    long got = Binary.ReadInt64(buffer, 0, StoreAsLittleEndian);
                    if (got < 0 || got > MaxKeyValuePairCapacity)
                        throw new CorruptDataException("The 'Key Value Pair Count' header field contains an invalid value.");
                    else
                        cachedKeyValuePairCount_ = got;
                }

                return cachedKeyValuePairCount_.Value;
            }

            internal set
            {
                if (IsReadOnly)
                    throw new InvalidOperationException("Cannot assign the " + nameof(KeyValuePairCount) + " property of a read-only " + nameof(BTreeNode<TKey, TValue>) + ".");
                if (value < 0 || value > MaxKeyValuePairCapacity)
                    throw new ArgumentOutOfRangeException(nameof(value));

                byte[] buffer = Binary.GetInt64Bytes(value, StoreAsLittleEndian);
                PageStorage.WriteTo(PageIndex, HeaderKeyValuePairCountPosition, buffer, 0, buffer.Length);
                cachedKeyValuePairCount_ = value;
            }
        }

        /// <summary>
        /// The number of sub-trees that are stored in this <see cref="BTreeNode{TKey, TValue}"/>.
        /// </summary>
        /// <remarks>
        /// For non-leaf nodes, this is <see cref="KeyValuePairCount"/>+1. For leaf nodes, this
        /// is zero (since leaf nodes have no sub-trees).
        /// </remarks>
        internal long SubTreeCount
        {
            get
            {
                if (IsLeaf)
                    return 0;//Leaf nodes have no sub-trees
                else
                    return KeyValuePairCount + 1;//+1 because there is a sub-tree left and right of each key-value pair
            }
        }

        private bool? cachedIsLeaf_ = null;
        /// <summary>
        /// Is this <see cref="BTreeNode{TKey, TValue}"/> a leaf node?
        /// </summary>
        public bool IsLeaf
        {
            get
            {
                if(cachedIsLeaf_ == null)
                {
                    byte[] buffer = new byte[1];
                    PageStorage.ReadFrom(PageIndex, HeaderIsLeafFlagPosition, buffer, 0, buffer.Length);
                    if (buffer[0] == 0x00)
                        cachedIsLeaf_ = false;
                    else if (buffer[0] == 0xFF)
                        cachedIsLeaf_ = true;
                    else
                        throw new CorruptDataException("The 'Is Leaf' header field contains invalid data.");
                }

                return cachedIsLeaf_.Value;
            }

            internal set
            {
                if (IsReadOnly)
                    throw new InvalidOperationException("Cannot assign the " + nameof(IsLeaf) + " property of a read-only " + nameof(BTreeNode<TKey, TValue>) + ".");

                PageStorage.WriteTo(PageIndex, HeaderIsLeafFlagPosition, new byte[] { value ? (byte)0xFF : (byte)0x00 }, 0, 1);
                cachedIsLeaf_ = value;
            }
        }

        private bool IsReadOnly { get { return BTree.IsReadOnly; } }/* Not public since the entire public interface is read-only by design */

        /// <summary>
        /// Arbitrary object for locking.
        /// </summary>
        /// <remarks>
        /// Note that this lock object will not protect against multiple <see cref="BTreeNode{TKey, TValue}"/>
        /// instances referencing the same page on the <see cref="IPageStorage"/>. However, the tree implementation
        /// (<see cref="BTree{TKey, TValue}"/> and <see cref="BTreeNode{TKey, TValue}"/> internally) will take care
        /// to avoid multiple instances. And the application can only instantiate read-only <see cref="BTreeNode{TKey, TValue}"/>s
        /// via a <see cref="BTreeReader{TKey, TValue}"/>, thus preventing any conflict.
        /// </remarks>
        private readonly object locker = new object();

        internal BTreeNode(BTree<TKey, TValue> bTree, long pageIndex)
        {
            this.BTree = bTree ?? throw new ArgumentNullException(nameof(bTree));
            this.PageIndex = pageIndex;
        }

        internal static bool TryCreateNew(BTree<TKey, TValue> tree, bool isLeaf, out BTreeNode<TKey, TValue> nodeOrDefault)
        {
            if (tree == null)
                throw new ArgumentNullException(nameof(tree));
            if (tree.IsReadOnly)
                throw new InvalidOperationException("Cannot create a new " + nameof(BTreeNode<TKey, TValue>) + " on a read-only " + nameof(BTree) + ".");

            var pageStorage = tree.PageStorage;
            if (pageStorage.AllocatedPageCount == pageStorage.PageCapacity)
            {
                //Capacity is full
                if (!pageStorage.IsCapacityFixed)
                {
                    //Try to inflate the capacity so we can allocate a new page
                    long got = pageStorage.TryInflate(1, null/* Only inflating by one page, progress report is not necessary */, new CancellationToken(false));
                    if (got < 1)
                    {
                        //Failed to inflate, so we cannot allocate a new BTreeNode.
                        nodeOrDefault = null;
                        return false;
                    }
                }
                else
                {
                    //Capacity is full and the storage is fixed-capacity, so we will not be able to allocate
                    nodeOrDefault = null;
                    return false;
                }
            }

            if (tree.PageStorage.TryAllocatePage(out long index))
            {
                nodeOrDefault = new BTreeNode<TKey, TValue>(tree, index);
                nodeOrDefault.KeyValuePairCount = 0;
                nodeOrDefault.IsLeaf = isLeaf;
                return true;
            }
            else
            {
                nodeOrDefault = null;
                return false;
            }
        }

        private long GetKeyPosition(long index)
        {
            return HeaderSize + (GetElementSize(KeySize, ValueSize) * index);
        }

        private long GetValuePosition(long index)
        {
            return GetKeyPosition(index) + KeySize;
        }

        private long GetSubTreePageIndexPosition(long subStreamIndex)
        {
            if (subStreamIndex == 0)
                return HeaderFirstSubTreeRootNodePageIndexPosition;
            else
                return GetValuePosition(subStreamIndex - 1) + ValueSize;
        }

        private void ReadKeyAt(long index, byte[] keyBuffer)
        {
            if (keyBuffer == null)
                throw new ArgumentNullException(nameof(keyBuffer));
            if (index < 0 || index >= KeyValuePairCount)
                throw new ArgumentOutOfRangeException(nameof(index));
            if (keyBuffer.Length != KeySize)
                throw new ArgumentException("Invalid buffer size", nameof(KeySize));

            PageStorage.ReadFrom(PageIndex, GetKeyPosition(index), keyBuffer, 0, KeySize);
        }

        private void WriteKeyAt(long index, byte[] keyBuffer)
        {
            if (IsReadOnly)
                throw new InvalidOperationException("Cannot write any key to a read-only " + nameof(BTreeNode<TKey, TValue>) + ".");
            if (keyBuffer == null)
                throw new ArgumentNullException(nameof(keyBuffer));
            if (index < 0 || index >= KeyValuePairCount)
                throw new ArgumentOutOfRangeException(nameof(index));
            if (keyBuffer.Length != KeySize)
                throw new ArgumentException("Invalid buffer size", nameof(KeySize));
            
            PageStorage.WriteTo(PageIndex, GetKeyPosition(index), keyBuffer, 0, KeySize);
        }

        /// <summary>
        /// Gets a key that is stored at a particular index on this <see cref="BTreeNode{TKey, TValue}"/>.
        /// </summary>
        /// <param name="index">The index of the key.</param>
        /// <returns>The key at the specified <paramref name="index"/>.</returns>
        /// <exception cref="ArgumentOutOfRangeException">Thrown if <paramref name="index"/> is less than
        /// zero or greater than or equal to <see cref="KeyValuePairCount"/>.</exception>
        /// <remarks>
        /// Keys are stored in ascending order.
        /// </remarks>
        public TKey GetKeyAt(long index)
        {
            if (index < 0 || index >= KeyValuePairCount)
                throw new ArgumentOutOfRangeException(nameof(index));

            lock(locker)
            {
                byte[] buffer = new byte[KeySize];
                ReadKeyAt(index, buffer);
                return BTree.KeySerializer.Deserialize(buffer);
            }
        }

        internal void SetKeyAt(long index, TKey key)
        {
            if (IsReadOnly)
                throw new InvalidOperationException("Cannot set any key on a read-only " + nameof(BTreeNode<TKey, TValue>) + ".");
            if (index < 0 || index >= KeyValuePairCount)
                throw new ArgumentOutOfRangeException(nameof(index));

            lock(locker)
            {
                byte[] buffer = new byte[KeySize];
                BTree.KeySerializer.Serialize(key, buffer);
                WriteKeyAt(index, buffer);
            }
        }

        /// <summary>
        /// All keys stored in this <see cref="BTreeNode{TKey, TValue}"/>, in ascending order.
        /// </summary>
        public IEnumerable<TKey> Keys
        {
            get
            {
                lock(locker)
                {
                    for (long i = 0; i < KeyValuePairCount; i++)
                        yield return GetKeyAt(i);
                }
            }
        }

        private void ReadValueAt(long index, byte[] buffer)
        {
            if (buffer == null)
                throw new ArgumentNullException(nameof(buffer));
            if (index < 0 || index >= KeyValuePairCount)
                throw new ArgumentOutOfRangeException(nameof(index));
            if (buffer.Length != ValueSize)
                throw new ArgumentException("Invalid buffer size", nameof(buffer));

            PageStorage.ReadFrom(PageIndex, GetValuePosition(index), buffer, 0, ValueSize);
        }

        private void WriteValueAt(long index, byte[] buffer)
        {
            if (IsReadOnly)
                throw new InvalidOperationException("Cannot write any key to a read-only " + nameof(BTreeNode<TKey, TValue>) + ".");
            if (buffer == null)
                throw new ArgumentNullException(nameof(buffer));
            if (index < 0 || index >= KeyValuePairCount)
                throw new ArgumentOutOfRangeException(nameof(index));
            if (buffer.Length != ValueSize)
                throw new ArgumentException("Invalid buffer size", nameof(buffer));

            PageStorage.WriteTo(PageIndex, GetValuePosition(index), buffer, 0, ValueSize);
        }

        /// <summary>
        /// Gets the value that is stored at a particular index on this <see cref="BTreeNode{TKey, TValue}"/>.
        /// </summary>
        /// <param name="index">The index of the value.</param>
        /// <returns>The value at the specified <paramref name="index"/>.</returns>
        /// <exception cref="ArgumentOutOfRangeException">Thrown if <paramref name="index"/> is less than
        /// zero or greater than or equal to <see cref="KeyValuePairCount"/>.</exception>
        public TValue GetValueAt(long index)
        {
            if (index < 0 || index >= KeyValuePairCount)
                throw new ArgumentOutOfRangeException(nameof(index));

            lock (locker)
            {
                byte[] buffer = new byte[ValueSize];
                ReadValueAt(index, buffer);
                return BTree.ValueSerializer.Deserialize(buffer);
            }
        }

        internal void SetValueAt(long index, TValue value)
        {
            if (IsReadOnly)
                throw new InvalidOperationException("Cannot set any value on a read-only " + nameof(BTreeNode<TKey, TValue>) + ".");
            if (index < 0 || index >= KeyValuePairCount)
                throw new ArgumentOutOfRangeException(nameof(index));

            lock (locker)
            {
                byte[] buffer = new byte[ValueSize];
                BTree.ValueSerializer.Serialize(value, buffer);
                WriteValueAt(index, buffer);
            }
        }

        /// <summary>
        /// All values stored in this <see cref="BTreeNode{TKey, TValue}"/>, in the order determined by
        /// the associated key.
        /// </summary>
        public IEnumerable<TValue> Values
        {
            get
            {
                lock(locker)
                {
                    for (long i = 0; i < KeyValuePairCount; i++)
                        yield return GetValueAt(i);
                }
            }
        }

        long IBinarySearchable<TKey, TValue>.Count { get { return KeyValuePairCount; } }/*Not public to avoid ambiguity (KeyValuePairCount or SubTreeCount) */

        /// <summary>
        /// Enumerates through all key-value pairs in this <see cref="BTreeNode{TKey, TValue}"/>.
        /// </summary>
        /// <returns>An enumeration of all key-value pairs, in ascending key order.</returns>
        /// <remarks>
        /// This method will <em>not</em> enumerate the key-value pairs that are stored in sub-trees.
        /// </remarks>
        public IEnumerator<KeyValuePair<TKey, TValue>> GetEnumerator()
        {
            for (long i = 0; i < KeyValuePairCount; i++)
                yield return new KeyValuePair<TKey, TValue>(GetKeyAt(i), GetValueAt(i));
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        internal BTreeNode<TKey, TValue> GetSubTreeAt(long index)
        {
            if (index < 0 || index >= SubTreeCount)
                throw new ArgumentOutOfRangeException(nameof(index));

            lock(locker)
            {
                byte[] buffer = new byte[sizeof(Int64)];
                PageStorage.ReadFrom(PageIndex, GetSubTreePageIndexPosition(index), buffer, 0, buffer.Length);
                long pageIndex = Binary.ReadInt64(buffer, 0, StoreAsLittleEndian);

                if (pageIndex == -1)
                    return null;//-1 indicates null

                if (!PageStorage.IsPageOnStorage(pageIndex))
                    throw new CorruptDataException("One of the sub-tree references refers to a page which does not exist on the " + nameof(IPageStorage) + ".");
                if (!PageStorage.IsPageAllocated(pageIndex))
                    throw new CorruptDataException("One of the sub-tree references refers to a page which is not allocated.");

                return new BTreeNode<TKey, TValue>(BTree, pageIndex);
            }
        }

        internal void SetSubTreeAt(long index, BTreeNode<TKey, TValue> rootNode)
        {
            if (index < 0 || index >= SubTreeCount)
                throw new ArgumentOutOfRangeException(nameof(index));
            if (IsReadOnly)
                throw new InvalidOperationException("Cannot set any sub-tree reference of a read-only " + nameof(BTreeNode<TKey, TValue>) + ".");

            lock(locker)
            {
                long pageIndex;
                if (rootNode != null)
                    pageIndex = rootNode.PageIndex;
                else
                    pageIndex = -1;//-1 indicates null

                byte[] buffer = Binary.GetInt64Bytes(pageIndex, StoreAsLittleEndian);
                PageStorage.WriteTo(PageIndex, GetSubTreePageIndexPosition(index), buffer, 0, buffer.Length);
            }
        }

        /// <summary>
        /// Attempts to find the index of a <paramref name="key"/> on this <see cref="BTreeNode{TKey, TValue}"/>, or the index
        /// of a sub-tree that <em>may</em> contain the <paramref name="key"/>.
        /// </summary>
        /// <param name="key">The input <typeparamref name="TKey"/>.</param>
        /// <param name="indexOnThisNode">If the <paramref name="key"/> was found on this <see cref="BTreeNode{TKey, TValue}"/> (check return
        /// value), then this argument will be assigned to the index of the <paramref name="key"/>. Otherwise, -1 will be assigned.</param>
        /// <param name="mayBeFoundInSubTree">Assigned to true if the <paramref name="key"/> <em>might</em> be found on one of
        /// this <see cref="BTreeNode{TKey, TValue}"/>'s sub-trees, otherwise false.</param>
        /// <param name="mayBeFoundInSubTreeIndex">If <paramref name="mayBeFoundInSubTree"/> is true, assigned to the index of the
        /// sub-tree that <em>might</em> contain the <paramref name="key"/>. Otherwise, -1 will be assigned.</param>
        /// <returns>True if the <paramref name="key"/> was found on this <see cref="BTreeNode{TKey, TValue}"/>, otherwise false.</returns>
        internal bool TryFindKeyIndexOrSubTreeIndexForKey(TKey key, out long indexOnThisNode, out bool mayBeFoundInSubTree, out long mayBeFoundInSubTreeIndex)
        {
            if(this.TryFindCeiling(key, out long ceilingKeyIndex, out var ceilingKey, null))
            {
                if(ceilingKey.CompareTo(key) == 0)
                {
                    //Found the key in this node
                    indexOnThisNode = ceilingKeyIndex;
                    mayBeFoundInSubTree = false;
                    mayBeFoundInSubTreeIndex = -1;
                    return true;
                }
                else
                {
                    //The key is not in this node.

                    if(!IsLeaf)
                    {
                        //Since we found the lowest key that is greater than input key ('ceilingKey'), we know that the only
                        //possible location for the key is in the sub-tree LEFT of the 'ceilingKey'.
                        //The index of that sub-tree happens to be the same as the index of 'ceilingKey'
                        indexOnThisNode = -1;
                        mayBeFoundInSubTree = true;
                        mayBeFoundInSubTreeIndex = ceilingKeyIndex;
                        return false;//Not in this node, but maybe in sub-tree
                    }
                    else
                    {
                        //Leaf nodes have no sub-trees, so there is no option to find the key. It does not exist.
                        indexOnThisNode = -1;
                        mayBeFoundInSubTree = false;
                        mayBeFoundInSubTreeIndex = -1;
                        return false;
                    }
                }
            }
            else
            {
                //All keys stored in this node are less than the input key
                //So the only possible location for the input key is RIGHT of the last key stored in this node.
                if(!IsLeaf)
                {
                    indexOnThisNode = -1;
                    mayBeFoundInSubTree = true;
                    mayBeFoundInSubTreeIndex = KeyValuePairCount;//Index of the right-most sub-tree
                    return false;//Not found on this node
                }
                else
                {
                    //Leaf nodes have no sub-trees, so there is no option to find the key. It does not exist.
                    indexOnThisNode = -1;
                    mayBeFoundInSubTree = false;
                    mayBeFoundInSubTreeIndex = -1;
                    return false;
                }
            }
        }

        internal static void CopyNonLeafElements(BTreeNode<TKey, TValue> src, long srcOffset, BTreeNode<TKey, TValue> dst, long dstOffset, long amount, BTreeNode<TKey, TValue> dstLeftSubTree, BTreeNode<TKey, TValue> dstRightSubTree)
        {
            if (src == null)
                throw new ArgumentNullException(nameof(src));
            if (dst == null)
                throw new ArgumentNullException(nameof(dst));
            if (src.BTree != dst.BTree)
                throw new InvalidOperationException("Cannot copy elements between " + nameof(BTreeNode<TKey, TValue>) + "s that belong to different " + nameof(BTree<TKey, TValue>) + "s.");
            if (dstLeftSubTree != null && dstLeftSubTree.BTree != src.BTree)
                throw new ArgumentException("The left sub-tree root node must belong to the same " + nameof(BTree<TKey, TValue>) + " that the source and destination " + nameof(BTreeNode<TKey, TValue>) + "s belong to.", nameof(dstLeftSubTree));
            if (dstRightSubTree != null && dstRightSubTree.BTree != src.BTree)
                throw new ArgumentException("The right sub-tree root node must belong to the same " + nameof(BTree<TKey, TValue>) + " that the source and destination " + nameof(BTreeNode<TKey, TValue>) + "s belong to.", nameof(dstRightSubTree));
            if (dst.IsReadOnly)
                throw new InvalidOperationException("Cannot copy elements to a read-only " + nameof(BTreeNode<TKey, TValue>) + ".");
            if (srcOffset < 0)
                throw new ArgumentOutOfRangeException(nameof(srcOffset));
            if (dstOffset < 0)
                throw new ArgumentOutOfRangeException(nameof(dstOffset));
            if (amount < 1)
                throw new ArgumentOutOfRangeException(nameof(amount), "Cannot copy less than one element between " + nameof(BTreeNode<TKey, TValue>) + "s.");
            if (srcOffset + amount > src.KeyValuePairCount)
                throw new ArgumentOutOfRangeException(nameof(amount), "The sum of the source offset and element count cannot be greater than the number of key-value pairs in the source " + nameof(BTreeNode<TKey, TValue>) + ".");
            if (dstOffset + amount > dst.KeyValuePairCount)
                throw new ArgumentOutOfRangeException(nameof(amount), "The sum of the destination offset and element count cannot be greater than the number of key-value pairs in the destination " + nameof(BTreeNode<TKey, TValue>) + ".");
            if (src.IsLeaf)
                throw new ArgumentException("The source " + nameof(BTreeNode<TKey, TValue>) + " cannot be a leaf node when using this overload.", nameof(src));
            if (dst.IsLeaf)
                throw new ArgumentException("The destination " + nameof(BTreeNode<TKey, TValue>) + " cannot be a leaf node when using this overload.", nameof(dst));

            long maxMoveAmount = src.BTree.MaxMoveKeyValuePairCount;
            if (maxMoveAmount < 1)//Sanity check to prevent infinite loop
                throw new InvalidOperationException("The " + nameof(BTree<TKey, TValue>.MaxMoveKeyValuePairCount) + " property cannot be less than one.");
            byte[] moveBuffer = new byte[maxMoveAmount * GetElementSize(src.KeySize, src.ValueSize)];

            if (dstOffset > srcOffset)
            {
                //Copy elements in reverse order (just in case the src and dst are the same node, if not, it doesn't matter)
                long progress = 0;
                while (progress < amount)
                {
                    long amountToMove = amount - progress;
                    if (amountToMove > maxMoveAmount)
                        amountToMove = maxMoveAmount;

                    long srcPosition = ((srcOffset + amount) - progress) - (amountToMove);
                    long dstPosition = ((dstOffset + amount) - progress) - (amountToMove);

                    long bytesToMove = amountToMove * GetElementSize(src.KeySize, src.ValueSize);
                    src.PageStorage.ReadFrom(src.PageIndex, src.GetKeyPosition(srcPosition), moveBuffer, 0, bytesToMove);
                    dst.PageStorage.WriteTo(dst.PageIndex, dst.GetKeyPosition(dstPosition), moveBuffer, 0, bytesToMove);

                    progress += amountToMove;
                }
            }
            else
            {
                //Copy elements in forward order (just in case the src and dst are the same node, if not, it doesn't matter)
                long progress = 0;
                while (progress < amount)
                {
                    long amountToMove = amount - progress;
                    if (amountToMove > maxMoveAmount)
                        amountToMove = maxMoveAmount;

                    long srcPosition = srcOffset + progress;
                    long dstPosition = dstOffset + progress;

                    long bytesToMove = amountToMove * GetElementSize(src.KeySize, src.ValueSize);
                    src.PageStorage.ReadFrom(src.PageIndex, src.GetKeyPosition(srcPosition), moveBuffer, 0, bytesToMove);
                    dst.PageStorage.WriteTo(dst.PageIndex, dst.GetKeyPosition(dstPosition), moveBuffer, 0, bytesToMove);

                    progress += amountToMove;
                }
            }

            //Assign the leftmost and rightmost sub-trees
            dst.SetSubTreeAt(dstOffset, dstLeftSubTree);
            dst.SetSubTreeAt(dstOffset + amount, dstRightSubTree);
        }

        internal static void CopyLeafElements(BTreeNode<TKey, TValue> src, long srcOffset, BTreeNode<TKey, TValue> dst, long dstOffset, long amount)
        {
            if (src == null)
                throw new ArgumentNullException(nameof(src));
            if (dst == null)
                throw new ArgumentNullException(nameof(dst));
            if (src.BTree != dst.BTree)
                throw new InvalidOperationException("Cannot copy elements between " + nameof(BTreeNode<TKey, TValue>) + "s that belong to different " + nameof(BTree<TKey, TValue>) + "s.");
            if (dst.IsReadOnly)
                throw new InvalidOperationException("Cannot copy elements to a read-only " + nameof(BTreeNode<TKey, TValue>) + ".");
            if (srcOffset < 0)
                throw new ArgumentOutOfRangeException(nameof(srcOffset));
            if (dstOffset < 0)
                throw new ArgumentOutOfRangeException(nameof(dstOffset));
            if (amount < 1)
                throw new ArgumentOutOfRangeException(nameof(amount), "Cannot copy less than one element between " + nameof(BTreeNode<TKey, TValue>) + "s.");
            if (srcOffset + amount > src.KeyValuePairCount)
                throw new ArgumentOutOfRangeException(nameof(amount), "The sum of the source offset and element count cannot be greater than the number of key-value pairs in the source " + nameof(BTreeNode<TKey, TValue>) + ".");
            if (dstOffset + amount > dst.KeyValuePairCount)
                throw new ArgumentOutOfRangeException(nameof(amount), "The sum of the destination offset and element count cannot be greater than the number of key-value pairs in the destination " + nameof(BTreeNode<TKey, TValue>) + ".");
            if (!src.IsLeaf)
                throw new ArgumentException("The source " + nameof(BTreeNode<TKey, TValue>) + " must be a leaf node when using this overload.", nameof(src));
            if (!dst.IsLeaf)
                throw new ArgumentException("The destination " + nameof(BTreeNode<TKey, TValue>) + " must be a leaf node when using this overload.", nameof(dst));

            long maxMoveAmount = src.BTree.MaxMoveKeyValuePairCount;
            if (maxMoveAmount < 1)//Sanity check to prevent infinite loop
                throw new InvalidOperationException("The " + nameof(BTree<TKey, TValue>.MaxMoveKeyValuePairCount) + " property cannot be less than one.");
            byte[] moveBuffer = new byte[maxMoveAmount * GetElementSize(src.KeySize, src.ValueSize)];

            if (dstOffset > srcOffset)
            {
                //Copy elements in reverse order (just in case the src and dst are the same node, if not, it doesn't matter)
                long progress = 0;
                while (progress < amount)
                {
                    long amountToMove = amount - progress;
                    if (amountToMove > maxMoveAmount)
                        amountToMove = maxMoveAmount;

                    long srcPosition = ((srcOffset + amount) - progress) - (amountToMove);
                    long dstPosition = ((dstOffset + amount) - progress) - (amountToMove);

                    long bytesToMove = amountToMove * GetElementSize(src.KeySize, src.ValueSize);
                    src.PageStorage.ReadFrom(src.PageIndex, src.GetKeyPosition(srcPosition), moveBuffer, 0, bytesToMove);
                    dst.PageStorage.WriteTo(dst.PageIndex, dst.GetKeyPosition(dstPosition), moveBuffer, 0, bytesToMove);

                    progress += amountToMove;
                }
            }
            else
            {
                //Copy elements in forward order (just in case the src and dst are the same node, if not, it doesn't matter)
                long progress = 0;
                while (progress < amount)
                {
                    long amountToMove = amount - progress;
                    if (amountToMove > maxMoveAmount)
                        amountToMove = maxMoveAmount;

                    long srcPosition = srcOffset + progress;
                    long dstPosition = dstOffset + progress;

                    long bytesToMove = amountToMove * GetElementSize(src.KeySize, src.ValueSize);
                    src.PageStorage.ReadFrom(src.PageIndex, src.GetKeyPosition(srcPosition), moveBuffer, 0, bytesToMove);
                    dst.PageStorage.WriteTo(dst.PageIndex, dst.GetKeyPosition(dstPosition), moveBuffer, 0, bytesToMove);

                    progress += amountToMove;
                }
            }
        }

        internal void InsertAtNonLeaf(byte[] keyBuffer, byte[] valueBuffer, long keyValuePairIndex, BTreeNode<TKey, TValue> subTreeRootNode, bool subTreeGoesLeft)
        {
            if (IsReadOnly)
                throw new InvalidOperationException("Cannot insert into a read-only " + nameof(BTreeNode<TKey, TValue>) + ".");
            if (keyBuffer == null)
                throw new ArgumentNullException(nameof(keyBuffer));
            if (valueBuffer == null)
                throw new ArgumentNullException(nameof(valueBuffer));
            if (keyBuffer.Length != KeySize)
                throw new ArgumentException("Invalid key buffer size.", nameof(keyBuffer));
            if (valueBuffer.Length != ValueSize)
                throw new ArgumentException("Invalid value buffer size.", nameof(keyBuffer));
            if (IsLeaf)
                throw new InvalidOperationException("Cannot insert a sub-tree into a leaf node. Use the other overload instead.");
            if (KeyValuePairCount == MaxKeyValuePairCapacity)
                throw new InvalidOperationException("Cannot insert into a full " + nameof(BTreeNode<TKey, TValue>) + ".");
            if (KeyValuePairCount == 0)//If it were empty, we would need to initialize left and right sub-tree (but we only have one in args)
                throw new InvalidOperationException("Cannot insert into an empty " + nameof(BTreeNode<TKey, TValue>) + ".");
            if (keyValuePairIndex < 0 || keyValuePairIndex > KeyValuePairCount)//Can be equal, which would mean 'at end'
                throw new ArgumentOutOfRangeException(nameof(keyValuePairIndex));
            if (subTreeRootNode == null)
                throw new ArgumentNullException(nameof(subTreeRootNode));
            if (subTreeRootNode.BTree != this.BTree)
                throw new ArgumentException("The sub-tree to insert must belong to the same " + nameof(BTree<TKey, TValue>) + " as this " + nameof(BTreeNode<TKey, TValue>) + ".", nameof(subTreeRootNode));

            long amountToMove = (KeyValuePairCount - keyValuePairIndex);

            //Increment the KeyValuePairCount
            KeyValuePairCount++;

            BTreeNode<TKey, TValue> toGoLeft, toGoRight;
            if (subTreeGoesLeft)
            {
                toGoLeft = subTreeRootNode;
                toGoRight = GetSubTreeAt(keyValuePairIndex);
            }
            else
            {
                toGoLeft = GetSubTreeAt(keyValuePairIndex);
                toGoRight = subTreeRootNode;
            }

            if (amountToMove > 0)
            {
                var toGoFarRight = GetSubTreeAt(KeyValuePairCount - 1);
                CopyNonLeafElements(this, keyValuePairIndex, this, keyValuePairIndex + 1, (this.KeyValuePairCount - 1/*We pre-incremented it*/) - keyValuePairIndex, toGoRight, toGoFarRight);
            }
            else
            {
                //There are no key-value pairs right of the to-insert index,
                //But we still need to set the right-most sub-tree
                SetSubTreeAt(KeyValuePairCount, toGoRight);
            }

            //Insert the new key-value pair
            WriteKeyAt(keyValuePairIndex, keyBuffer);
            WriteValueAt(keyValuePairIndex, valueBuffer);
            
            //Insert the left sub-tree
            SetSubTreeAt(keyValuePairIndex, toGoLeft);
        }

        internal void RemoveAtNonLeaf(long keyValuePairIndex, bool removeLeftSubTree)
        {
            if (IsReadOnly)
                throw new InvalidOperationException("Cannot remove any key-value pair from a read-only " + nameof(BTreeNode<TKey, TValue>) + ".");
            if (IsLeaf)
                throw new InvalidOperationException("Cannot use this remove overload on a non-leaf " + nameof(BTreeNode<TKey, TValue>) + ".");
            if (keyValuePairIndex < 0 || keyValuePairIndex >= KeyValuePairCount)
                throw new ArgumentOutOfRangeException(nameof(keyValuePairIndex));

            var currentLeftSubTree = GetSubTreeAt(keyValuePairIndex);
            var currentRightSubTree = GetSubTreeAt(keyValuePairIndex + 1);
            var rightMostSubTree = GetSubTreeAt(SubTreeCount - 1);

            BTreeNode<TKey, TValue> toKeep;
            if (removeLeftSubTree)
                toKeep = currentRightSubTree;
            else
                toKeep = currentLeftSubTree;

            long amountToMove = (KeyValuePairCount - 1) - keyValuePairIndex;

            if (amountToMove > 0)
                CopyNonLeafElements(this, keyValuePairIndex + 1, this, keyValuePairIndex, amountToMove, toKeep, rightMostSubTree);
            else
                SetSubTreeAt(keyValuePairIndex, toKeep);

            KeyValuePairCount--;
        }
        
        internal void InsertAtLeaf(byte[] keyBuffer, byte[] valueBuffer, long keyValuePairIndex)
        {
            if (IsReadOnly)
                throw new InvalidOperationException("Cannot insert into a read-only " + nameof(BTreeNode<TKey, TValue>) + ".");
            if (keyBuffer == null)
                throw new ArgumentNullException(nameof(keyBuffer));
            if (valueBuffer == null)
                throw new ArgumentNullException(nameof(valueBuffer));
            if (keyBuffer.Length != KeySize)
                throw new ArgumentException("Invalid key buffer size.", nameof(keyBuffer));
            if (valueBuffer.Length != ValueSize)
                throw new ArgumentException("Invalid value buffer size.", nameof(keyBuffer));
            if (!IsLeaf)
                throw new InvalidOperationException("Cannot use this insert overload for a non-leaf " + nameof(BTreeNode<TKey, TValue>) + ".");
            if (KeyValuePairCount == MaxKeyValuePairCapacity)
                throw new InvalidOperationException("Cannot insert into a full " + nameof(BTreeNode<TKey, TValue>) + ".");
            if (KeyValuePairCount == 0)//If it were empty, we would need to initialize left and right sub-tree (but we only have one in args)
                throw new InvalidOperationException("Cannot insert into an empty " + nameof(BTreeNode<TKey, TValue>) + ".");
            if (keyValuePairIndex < 0 || keyValuePairIndex > KeyValuePairCount)//Can be equal, which would mean 'at end'
                throw new ArgumentOutOfRangeException(nameof(keyValuePairIndex));
            
            long amountToMove = (KeyValuePairCount - keyValuePairIndex);

            //Increment the KeyValuePairCount
            KeyValuePairCount++;
            
            if (amountToMove > 0)
                CopyLeafElements(this, keyValuePairIndex, this, keyValuePairIndex + 1, (this.KeyValuePairCount - 1/*We pre-incremented it*/) - keyValuePairIndex);

            //Insert the new key-value pair
            WriteKeyAt(keyValuePairIndex, keyBuffer);
            WriteValueAt(keyValuePairIndex, valueBuffer);
        }

        internal void RemoveAtLeaf(long keyValuePairIndex)
        {
            if (IsReadOnly)
                throw new InvalidOperationException("Cannot remove any key-value pair from a read-only " + nameof(BTreeNode<TKey, TValue>) + ".");
            if (!IsLeaf)
                throw new InvalidOperationException("Cannot use this remove overload on a non-leaf " + nameof(BTreeNode<TKey, TValue>) + ".");
            if (keyValuePairIndex < 0 || keyValuePairIndex >= KeyValuePairCount)
                throw new ArgumentOutOfRangeException(nameof(keyValuePairIndex));
            
            long amountToMove = (KeyValuePairCount - 1) - keyValuePairIndex;

            if (amountToMove > 0)
                CopyLeafElements(this, keyValuePairIndex + 1, this, keyValuePairIndex, amountToMove);
            
            KeyValuePairCount--;
        }

        /// <summary>
        /// Splits a sub-tree at a specific index, and moves the 'middle' value up to this <see cref="BTreeNode{TKey, TValue}"/>.
        /// </summary>
        /// <param name="index">The index of the sub-tree to split. The <see cref="BTreeNode{TKey, TValue}"/> at
        /// this sub-tree index must be full (<see cref="KeyValuePairCount"/> must be equal to <see cref="MaxKeyValuePairCapacity"/>).
        /// This index is also the index where the 'middle' key-value pair will be inserted to this 
        /// <see cref="BTreeNode{TKey, TValue}"/>.</param>
        /// <param name="newNode">An empty <see cref="BTreeNode{TKey, TValue}"/> that will become the
        /// 'right' portion of the split node.</param>
        /// <remarks>
        /// This method must only be called when the sub-tree root <see cref="BTreeNode{TKey, TValue}"/> at the specified
        /// <paramref name="index"/> is full (meaning its <see cref="KeyValuePairCount"/> is equal to <see cref="MaxKeyValuePairCapacity"/>).
        /// This method will 'break' that node into two larger pieces, and a 'middle' value. The left half of that
        /// 'broken' node will remain unchanged. But the 'right' half will be moved into <paramref name="newNode"/>. And
        /// the 'middle' will ascend into this <see cref="BTreeNode{TKey, TValue}"/>, inserted at <paramref name="index"/>.
        /// </remarks>
        internal void SplitSubTreeAt(long index, BTreeNode<TKey, TValue> newNode)
        {
            if (IsReadOnly)
                throw new InvalidOperationException("Cannot split a sub-tree of a read-only " + nameof(BTreeNode<TKey, TValue>) + ".");
            if (IsLeaf)
                throw new InvalidOperationException("Cannot split the sub-tree of a leaf node because leaf nodes have no sub-trees.");
            if (index < 0 || index >= SubTreeCount)
                throw new ArgumentOutOfRangeException(nameof(index));
            if (newNode == null)
                throw new ArgumentNullException(nameof(newNode));
            if (newNode.KeyValuePairCount != 0)
                throw new ArgumentException("Must provide an empty " + nameof(BTreeNode<TKey, TValue>) + " to this argument.", nameof(newNode));

            BTreeNode<TKey, TValue> toSplit = GetSubTreeAt(index);
            if (toSplit.KeyValuePairCount != toSplit.MaxKeyValuePairCapacity)
                throw new InvalidOperationException("Cannot split a non-full sub-tree.");

            //We will move the key-value pair from the middle of 'toSplit' to 'this' node
            long middleIndex = toSplit.KeyValuePairCount / 2;

            //Leave the values left of 'middle' the same,
            //but move everything right of 'middle' to 'newNode'
            newNode.KeyValuePairCount = toSplit.KeyValuePairCount / 2;//Keep in mind, KeyValuePairCount is at max (which is odd #), so this will round down
            newNode.IsLeaf = toSplit.IsLeaf;

            if(newNode.IsLeaf)
            {
                CopyLeafElements(toSplit, middleIndex + 1/*+1 to skip middle, which goes up to parent*/, newNode, 0, newNode.KeyValuePairCount);
            }
            else
            {
                var newNodeLeftSubTree = toSplit.GetSubTreeAt(middleIndex + 1);
                var newNodeRightSubTree = toSplit.GetSubTreeAt(middleIndex + 1 + newNode.KeyValuePairCount);
                CopyNonLeafElements(toSplit, middleIndex + 1, newNode, 0, newNode.KeyValuePairCount, newNodeLeftSubTree, newNodeRightSubTree);
            }

            //Take the key-value pair from the 'middle', and insert it at 'index'
            byte[] keyMover = new byte[KeySize];
            byte[] valueMover = new byte[ValueSize];
            toSplit.ReadKeyAt(middleIndex, keyMover);
            toSplit.ReadValueAt(middleIndex, valueMover);
            this.InsertAtNonLeaf(keyMover, valueMover, index, newNode, false/*goes right*/);

            //Finally, update the KeyValuePairCount of 'toSplit'
            toSplit.KeyValuePairCount = newNode.KeyValuePairCount;
        }

        internal bool TryFindKeyOnNode(TKey key, out BTreeNode<TKey, TValue> onNode, out long indexOnNode, CancellationToken cancellationToken)
        {
            var currentNode = this;
            while(true)
            {
                if(cancellationToken.IsCancellationRequested)
                {
                    onNode = null;
                    indexOnNode = -1;
                    return false;
                }

                if(currentNode.TryFindKeyIndexOrSubTreeIndexForKey(key, out long indexOnThisNode, out bool mayBeFoundInSubTree, out long mayBeFoundInSubTreeIndex))
                {
                    onNode = currentNode;
                    indexOnNode = indexOnThisNode;
                    return true;
                }
                else
                {
                    if(mayBeFoundInSubTree)
                    {
                        currentNode = currentNode.GetSubTreeAt(mayBeFoundInSubTreeIndex);
                        continue;
                    }
                    else
                    {
                        onNode = null;
                        indexOnNode = -1;
                        return false;
                    }
                }
            }
        }

        internal bool ContainsKey(TKey key, CancellationToken cancellationToken)
        {
            return TryFindKeyOnNode(key, out _, out _, cancellationToken);
        }

        internal bool TryUpdateValue(TKey key, byte[] valueBuffer)
        {
            if(TryFindKeyOnNode(key, out BTreeNode<TKey, TValue> onNode, out long indexOnNode, new CancellationToken(false)))
            {
                onNode.WriteValueAt(indexOnNode, valueBuffer);
                return true;
            }
            else
            {
                //Cannot exist, the key does not exist in the tree rooted by this node
                return false;
            }
        }

        internal bool Insert(TKey key, TValue value, bool updateIfExists, out bool alreadyExists)
        {
            if (IsReadOnly)
                throw new InvalidOperationException("Cannot insert a key-value pair to a read-only " + nameof(BTreeNode<TKey, TValue>) + ".");

            byte[] valueBuffer = new byte[ValueSize];
            BTree.ValueSerializer.Serialize(value, valueBuffer);

            BTreeNode<TKey, TValue> currentNode = this;
            while(true)
            {
                if(currentNode.IsLeaf)
                {
                    if(currentNode.TryFindCeiling(key, out long insertIndex, out var existingKeyAtIndex, null, new CancellationToken(false)))
                    {
                        if(existingKeyAtIndex.CompareTo(key) == 0)
                        {
                            //The key already exists
                            alreadyExists = true;

                            if(updateIfExists)
                            {
                                //Update the value to the existing key-value pair
                                currentNode.WriteValueAt(insertIndex, valueBuffer);
                                return true;
                            }
                            else
                            {
                                //Already exists, but caller doesn't want us to update the value
                                return false;
                            }
                        }
                        else
                        {
                            //Key does not already exist, we will insert at the specified position
                            byte[] keyBuffer = new byte[KeySize];
                            BTree.KeySerializer.Serialize(key, keyBuffer);
                            currentNode.InsertAtLeaf(keyBuffer, valueBuffer, insertIndex);
                            alreadyExists = false;
                            return true;
                        }
                    }
                    else
                    {
                        //All keys in this leaf node are less than input key,
                        //so input key goes at far right
                        byte[] keyBuffer = new byte[KeySize];
                        BTree.KeySerializer.Serialize(key, keyBuffer);
                        currentNode.InsertAtLeaf(keyBuffer, valueBuffer, currentNode.KeyValuePairCount);
                        alreadyExists = false;
                        return true;
                    }
                }
                else
                {
                    //Insertion requires a leaf node, so determine which sub-tree we will use to get to a leaf node
                    if(currentNode.TryFindKeyIndexOrSubTreeIndexForKey(key, out long indexOnThisNode, out bool mayBeFoundInSubTree, out long mayBeFoundInSubTreeIndex))
                    {
                        //The key already exists in this non-leaf node (indeed possible!)
                        alreadyExists = true;

                        if(updateIfExists)
                        {
                            //Update the value to the existing key-value pair
                            currentNode.WriteValueAt(indexOnThisNode, valueBuffer);
                            return true;
                        }
                        else
                        {
                            //Caller won't let us update
                            return false;
                        }
                    }
                    else
                    {
                        if(mayBeFoundInSubTree)
                        {
                            var goToSubTree = currentNode.GetSubTreeAt(mayBeFoundInSubTreeIndex);

                            //Before traversing to the sub-tree, make sure it isn't full
                            if(goToSubTree.KeyValuePairCount == goToSubTree.MaxKeyValuePairCapacity)
                            {
                                //Must split before traversing
                                if(TryCreateNew(BTree, false/*Will be assigned in the split operation*/, out var splitUsingNode))
                                {
                                    currentNode.SplitSubTreeAt(mayBeFoundInSubTreeIndex, splitUsingNode);
                                }
                                else
                                {
                                    //Since we failed to allocate the node required for splitting, we cannot insert a new key-value pair.
                                    //But we can still update the value to an existing key-value pair!
                                    if(updateIfExists)
                                    {
                                        if(currentNode.TryUpdateValue(key, valueBuffer))
                                        {
                                            //Successfully updated the existing key-value pair
                                            alreadyExists = true;
                                            return true;
                                        }
                                        else
                                        {
                                            //Does not exist, and insert failed
                                            alreadyExists = false;
                                            return false;
                                        }
                                    }
                                    else
                                    {
                                        //Not allowed to update
                                        alreadyExists = currentNode.ContainsKey(key, new CancellationToken(false));
                                        return false;//Insert failed
                                    }
                                }

                                //If we reached here, split was successful
                                //The split may have changed the 'next node' to traverse, so check
                                if (currentNode.GetKeyAt(mayBeFoundInSubTreeIndex).CompareTo(key) < 0)
                                    mayBeFoundInSubTreeIndex++;
                            }

                            currentNode = currentNode.GetSubTreeAt(mayBeFoundInSubTreeIndex);
                            continue;
                        }
                        else
                        {
                            //This condition is impossible. We know that 'currentNode' is a non-leaf node (otherwise insertion would already be finished!)
                            //And we know that 'TryFindKeyIndexOrSubTreeIndexForKey' always sets 'mayBeFoundInSubTree' true for non-leaf nodes.
                            throw new InvalidOperationException("Traversed to an impossible condition.");
                        }
                    }
                }
            }
        }

        internal bool Remove(TKey key, out TValue valueOrDefault)
        {
            if (IsReadOnly)
                throw new InvalidOperationException("Cannot remove any key-value pair from a read-only " + nameof(BTreeNode<TKey, TValue>) + ".");
            
            var currentNode = this;
            while(true)
            {
                if(currentNode.TryFindKeyIndexOrSubTreeIndexForKey(key, out long indexOnCurrentNode, out bool mayBeFoundInSubTree, out long mayBeFoundInSubTreeIndex))
                {
                    valueOrDefault = currentNode.GetValueAt(indexOnCurrentNode);

                    if (currentNode.IsLeaf)
                    {
                        //Removal from a leaf node is simple
                        currentNode.RemoveAtLeaf(indexOnCurrentNode);
                        return true;
                    }
                    else
                    {
                        currentNode.RemoveElementAtNonLeaf(key, indexOnCurrentNode);
                        return true;
                    }
                }
                else
                {
                    //The key is not in the current node
                    if(mayBeFoundInSubTree)
                    {
                        var subTree = currentNode.GetSubTreeAt(mayBeFoundInSubTreeIndex);
                        
                        if(subTree.KeyValuePairCount == subTree.MinKeyValuePairCapacity)
                        {
                            //Cannot simply remove from this sub-tree, since it is at the minimum.
                            //We have to first increase the KeyValueCount, then remove
                            currentNode = currentNode.BringSubTreeAboveMinKeyValueCount(mayBeFoundInSubTreeIndex);
                            continue;
                        }
                        else
                        {
                            //Traverse to the next sub-tree
                            currentNode = subTree;
                            continue;
                        }
                    }
                    else
                    {
                        //And there is no sub-tree that may contain the key
                        //So remove failed, the key does not exist
                        valueOrDefault = default(TValue);
                        return false;
                    }
                }
            }
        }

        internal void RemoveElementAtNonLeaf(TKey keyToRemove, long indexOfKey)
        {
            var leftSubTree = GetSubTreeAt(indexOfKey);
            var rightSubTree = GetSubTreeAt(indexOfKey + 1);
            
            if (leftSubTree.KeyValuePairCount > leftSubTree.MinKeyValuePairCapacity)
            {
                //Find the predecessor of 'keyToRemove' in the leftSubTree
                byte[] predecessorValueBuffer = new byte[ValueSize];
                var predecessorKey = GetPredecessor(indexOfKey, predecessorValueBuffer);

                //Replace the key-value pair at 'indexOfKey' (to remove) with the predecessor key-value pair
                SetKeyAt(indexOfKey, predecessorKey);
                WriteValueAt(indexOfKey, predecessorValueBuffer);

                //Remove the 'predecessor' from the original node (now that we copied it to this node)
                leftSubTree.Remove(predecessorKey, out _);
            }
            else if (rightSubTree.KeyValuePairCount > rightSubTree.MinKeyValuePairCapacity)
            {
                //Find the successor of 'keyToRemove' in the rightSubTree
                byte[] successorValueBuffer = new byte[ValueSize];
                var successorKey = GetSuccessor(indexOfKey, successorValueBuffer);

                //Replace the key-value pair at 'indexOfKey' (to remove) with the successor key-value pair
                SetKeyAt(indexOfKey, successorKey);
                WriteValueAt(indexOfKey, successorValueBuffer);
                
                //Remove the 'successor' from the original node (now that we copied it to this node)
                rightSubTree.Remove(successorKey, out _);
            }
            else
            {
                //Both the left and right sub-tree are at their minimal key-value pair count,
                //so we cannot 'swap' the value from either. However, since they are both
                //at the minimum key-value count, and the maximum key-value count is MORE
                //than double the minimum, we can simply merge the two, then remove!
                var mergedSubTreeNode = MergeAdjacentSubTreesAt(indexOfKey);
                mergedSubTreeNode.Remove(keyToRemove, out _);
            }
        }

        internal TKey GetPredecessor(long keyIndex, byte[] valueBuffer)
        {
            var currentNode = this.GetSubTreeAt(keyIndex/*Left sub-tree*/);
            while(!currentNode.IsLeaf)
                currentNode = currentNode.GetSubTreeAt(currentNode.SubTreeCount - 1);

            currentNode.ReadValueAt(currentNode.KeyValuePairCount - 1, valueBuffer);
            return currentNode.GetKeyAt(currentNode.KeyValuePairCount - 1);
        }

        internal TKey GetSuccessor(long keyIndex, byte[] valueBuffer)
        {
            var currentNode = this.GetSubTreeAt(keyIndex + 1/*Right sub-tree*/);
            while (!currentNode.IsLeaf)
                currentNode = currentNode.GetSubTreeAt(0);

            currentNode.ReadValueAt(0, valueBuffer);
            return currentNode.GetKeyAt(0);
        }

        internal BTreeNode<TKey, TValue> MergeAdjacentSubTreesAt(long index)
        {
            if (IsReadOnly)
                throw new InvalidOperationException("Cannot merge sub-trees on a read-only " + nameof(BTreeNode<TKey, TValue>) + ".");
            if (IsLeaf)
                throw new InvalidOperationException("Cannot merge sub-trees of a leaf " + nameof(BTreeNode<TKey, TValue>) + ", since leaf nodes have no sub-trees.");
            if (index < 0 || index >= KeyValuePairCount)
                throw new ArgumentOutOfRangeException(nameof(index));

            var left = GetSubTreeAt(index);//Will keep this one
            var right = GetSubTreeAt(index + 1);//Will delete this one (after copying content to former node)

            var key = this.GetKeyAt(index);
            byte[] valueBuffer = new byte[ValueSize];
            this.ReadValueAt(index, valueBuffer);

            //Remove the key from this node (have to, since we are about to remove a sub-tree for the merge)
            //We will move it to the left sub-tree
            this.RemoveAtNonLeaf(index, false/*Will remove the right one*/);

            //Put the key into the left sub-tree (at the very end)
            left.KeyValuePairCount++;
            left.SetKeyAt(left.KeyValuePairCount - 1, key);
            left.WriteValueAt(left.KeyValuePairCount - 1, valueBuffer);

            long copyDstPos = left.KeyValuePairCount;//Right at the end (no -1 since we are adding AFTER the end)

            //Copy all values from the right sub-tree to the left one (after the key that we just added)
            left.KeyValuePairCount += right.KeyValuePairCount;

            if (left.IsLeaf)
                CopyLeafElements(right, 0, left, copyDstPos, right.KeyValuePairCount);
            else
                CopyNonLeafElements(right, 0, left, copyDstPos, right.KeyValuePairCount, right.GetSubTreeAt(0), right.GetSubTreeAt(right.SubTreeCount - 1));

            //Delete the right sub-tree
            PageStorage.FreePage(right.PageIndex);

            return left;
        }

        internal BTreeNode<TKey, TValue> BringSubTreeAboveMinKeyValueCount(long subTreeIndex)
        {
            if (IsReadOnly)
                throw new InvalidOperationException("Cannot bring a sub-tree above its minimum key-value pair count when it is read-only.");
            if (subTreeIndex < 0 || subTreeIndex >= SubTreeCount)
                throw new ArgumentOutOfRangeException(nameof(subTreeIndex));
            
            BTreeNode<TKey, TValue> leftNeighbor = null, rightNeighbor = null;
            if (subTreeIndex > 0)
                leftNeighbor = GetSubTreeAt(subTreeIndex - 1);
            if (subTreeIndex < SubTreeCount - 1)
                rightNeighbor = GetSubTreeAt(subTreeIndex + 1);

            if(leftNeighbor != null && leftNeighbor.KeyValuePairCount > leftNeighbor.MinKeyValuePairCapacity)
            {
                //We can safely move a key-value pair from the left neighbor into 'subTree' to make it above min key-value pair count
                RotateFromLeft(subTreeIndex);
                return GetSubTreeAt(subTreeIndex);
            }
            else if(rightNeighbor != null && rightNeighbor.KeyValuePairCount > rightNeighbor.MinKeyValuePairCapacity)
            {
                //We can safely move a key-value pair from the right neighbor into 'subTree' to make it above min key-value pair count
                RotateFromRight(subTreeIndex);
                return GetSubTreeAt(subTreeIndex);
            }
            else
            {
                //Both the left and right neighbors are at minimum capacity, so we cannot 'move' (rotate) a key-value pair from them.
                //But since they are both at min capacity, and max is more than 2 times min capacity,
                //we can merge the two.
                if (subTreeIndex < SubTreeCount - 1/* -1 because it will merge with right neighbor */)
                    return MergeAdjacentSubTreesAt(subTreeIndex);
                else
                    return MergeAdjacentSubTreesAt(subTreeIndex - 1/*-1 because we are at far right, and merge will go to right*/);
            }
        }

        internal void RotateFromLeft(long subTreeIndex)
        {
            var subTree = GetSubTreeAt(subTreeIndex);
            var leftSubTree = GetSubTreeAt(subTreeIndex - 1);

            //Move the key-value pair from 'this node' to start of 'subTree'
            byte[] valueBuffer = new byte[ValueSize];
            byte[] keyBuffer = new byte[KeySize];
            this.ReadKeyAt(subTreeIndex - 1, keyBuffer);
            this.ReadValueAt(subTreeIndex - 1, valueBuffer);
            if (subTree.IsLeaf)
                subTree.InsertAtLeaf(keyBuffer, valueBuffer, 0);
            else
                subTree.InsertAtNonLeaf(keyBuffer, valueBuffer, 0, leftSubTree.GetSubTreeAt(leftSubTree.SubTreeCount - 1), true);

            //Move the last key-value pair from 'leftSubTree' to 'this node' (where we removed a key-value earlier)
            leftSubTree.ReadKeyAt(leftSubTree.KeyValuePairCount - 1, keyBuffer);
            leftSubTree.ReadValueAt(leftSubTree.KeyValuePairCount - 1, valueBuffer);
            this.WriteKeyAt(subTreeIndex - 1, keyBuffer);
            this.WriteValueAt(subTreeIndex - 1, valueBuffer);
            leftSubTree.KeyValuePairCount--;//Since we removed an element from it
        }

        internal void RotateFromRight(long subTreeIndex)
        {
            var subTree = GetSubTreeAt(subTreeIndex);
            var rightSubTree = GetSubTreeAt(subTreeIndex + 1);

            //Move the key-value pair from 'this node' to end of 'subTree'
            byte[] valueBuffer = new byte[ValueSize];
            byte[] keyBuffer = new byte[KeySize];
            this.ReadKeyAt(subTreeIndex, keyBuffer);
            this.ReadValueAt(subTreeIndex, valueBuffer);

            if (subTree.IsLeaf)
            {
                subTree.InsertAtLeaf(keyBuffer, valueBuffer, subTree.KeyValuePairCount);
            }
            else
            {
                var toRight = rightSubTree.GetSubTreeAt(0);
                subTree.InsertAtNonLeaf(keyBuffer, valueBuffer, subTree.KeyValuePairCount, toRight, false);
            }

            //Remove first the key-value pair from 'rightSubTree' and insert it into 'this node' (where we removed a key-value earlier)
            rightSubTree.ReadKeyAt(0, keyBuffer);
            rightSubTree.ReadValueAt(0, valueBuffer);
            if (rightSubTree.IsLeaf)
                rightSubTree.RemoveAtLeaf(0);
            else
                rightSubTree.RemoveAtNonLeaf(0, true);
            this.WriteKeyAt(subTreeIndex, keyBuffer);
            this.WriteValueAt(subTreeIndex, valueBuffer);
        }

        internal IEnumerable<KeyValuePair<TKey, TValue>> Traverse(bool ascending)
        {
            lock(locker)
            {
                for (long i = 0; i < KeyValuePairCount; i++)
                {
                    if(ascending)
                    {
                        if(!IsLeaf)
                        {
                            //Traverse the left sub-tree
                            foreach (var pair in GetSubTreeAt(i).Traverse(ascending))
                                yield return pair;
                        }

                        //Return the current key
                        yield return new KeyValuePair<TKey, TValue>(GetKeyAt(i), GetValueAt(i));

                        if(!IsLeaf && i == KeyValuePairCount - 1)
                        {
                            //Traverse the right sub-tree
                            foreach (var pair in GetSubTreeAt(i + 1).Traverse(ascending))
                                yield return pair;
                        }
                    }
                    else
                    {
                        long index = (KeyValuePairCount - (i + 1));

                        if(!IsLeaf && index == KeyValuePairCount - 1)
                        {
                            //Traverse the right sub-tree first
                            foreach (var pair in GetSubTreeAt(index + 1).Traverse(ascending))
                                yield return pair;
                        }

                        //Return the current key
                        yield return new KeyValuePair<TKey, TValue>(GetKeyAt(index), GetValueAt(index));

                        if(!IsLeaf)
                        {
                            //Traverse the left sub-tree
                            foreach (var pair in GetSubTreeAt(index).Traverse(ascending))
                                yield return pair;
                        }                        
                    }
                }
            }
        }

        internal void Validate(CancellationToken cancellationToken, ref long keyCounter, long maxKeyCount)
        {
            if (!IsReadOnly)
                throw new InvalidOperationException("Cannot validate a non-read-only " + nameof(BTreeNode<TKey, TValue>) + ".");

            //Since this node is read-only, we will not lock onto 'locker' (doing so could cause unnecessary delay)
            long kvCount = KeyValuePairCount;
            TKey previousKey = default(TKey);
            for(long i = 0; i < kvCount; i++)
            {
                TKey currentKey = GetKeyAt(i);

                //Make sure all keys in this node are ascending
                if (i > 0 && currentKey.CompareTo(previousKey) < 0)
                    throw new CorruptDataException("Found a " + nameof(BTreeNode<TKey, TValue>) + " in which not all keys are stored in ascending order.");

                //Make sure there are no duplicate keys in this node
                for(long j = i + 1; j < kvCount; j++)
                {
                    if (GetKeyAt(j).CompareTo(currentKey) == 0)
                        throw new CorruptDataException("Found a " + nameof(BTreeNode<TKey, TValue>) + " that has duplicate keys.");
                }
                
                if(!IsLeaf)
                {
                    if(i == 0)
                    {
                        //Make sure all keys in the left sub-tree (recursively) are less than the current key
                        var leftMostSubTree = GetSubTreeAt(0);
                        foreach(var extreme in leftMostSubTree.GetExtremeKeys(cancellationToken))
                        {
                            if (extreme.CompareTo(currentKey) >= 0)
                                throw new CorruptDataException("Found a " + nameof(BTreeNode<TKey, TValue>) + " that contains an out-of-position key.");
                        }
                    }

                    //Make sure all keys in the right sub-tree (recursively) are greater than the current key
                    var rightSubTree = GetSubTreeAt(i + 1);
                    foreach(var extreme in rightSubTree.GetExtremeKeys(cancellationToken))
                    {
                        if (extreme.CompareTo(currentKey) <= 0)
                            throw new CorruptDataException("Found a " + nameof(BTreeNode<TKey, TValue>) + " that contains an out-of-position key.");
                    }
                }
                
                previousKey = currentKey;
            }

            keyCounter += kvCount;
            if (keyCounter > maxKeyCount)
                throw new CorruptDataException("Found more than the expected number of key-value pairs.");

            if(!IsLeaf)
            {
                //Validate all sub-trees
                for (long i = 0; i < SubTreeCount; i++)
                    GetSubTreeAt(i).Validate(cancellationToken, ref keyCounter, maxKeyCount);
            }
        }

        private IEnumerable<TKey> GetExtremeKeys(CancellationToken cancellationToken)
        {
            if (cancellationToken.IsCancellationRequested)
                yield break;

            if (KeyValuePairCount > 0)
            {
                if (!IsLeaf)
                {
                    foreach (var extreme in GetSubTreeAt(0).GetExtremeKeys(cancellationToken))
                        yield return extreme;
                }

                yield return GetKeyAt(0);
            }
            
            if (KeyValuePairCount > 1)
            {
                yield return GetKeyAt(KeyValuePairCount - 1);

                if (!IsLeaf)
                {
                    foreach (var extreme in GetSubTreeAt(SubTreeCount - 1).GetExtremeKeys(cancellationToken))
                        yield return extreme;
                }
            }
        }
    }
}
