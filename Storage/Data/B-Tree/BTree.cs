using Storage.Data.Serializers;
using System;
using System.Collections.Generic;
using System.Threading;

namespace Storage.Data
{
    /// <summary>
    /// Base class for a B-Tree.
    /// </summary>
    /// <typeparam name="TKey">The type of the key.</typeparam>
    /// <typeparam name="TValue">The type of the value.</typeparam>
    /// <remarks>
    /// Note that the application cannot interact with the <see cref="BTreeNode{TKey, TValue}"/>s
    /// of a <see cref="BTree{TKey, TValue}"/> directly. The application can read the <see cref="BTreeNode{TKey, TValue}"/>
    /// of a <see cref="BTree{TKey, TValue}"/> by using a <see cref="BTreeReader{TKey, TValue}"/> only when
    /// the <see cref="BTree{TKey, TValue}"/> is read-only.
    /// </remarks>
    public abstract class BTree<TKey, TValue> where TKey : IComparable<TKey>
    {
        /// <summary>
        /// The <see cref="IPageStorage"/> on which this <see cref="BTree{TKey, TValue}"/>
        /// is stored.
        /// </summary>
        public IPageStorage PageStorage { get; private set; }

        /// <summary>
        /// Is this <see cref="BTree{TKey, TValue}"/> read-only?
        /// </summary>
        public bool IsReadOnly { get { return PageStorage.IsReadOnly; } }

        /// <summary>
        /// The <see cref="ISerializer{T}"/> that is used to serialize and
        /// deserialize the keys.
        /// </summary>
        public ISerializer<TKey> KeySerializer { get; private set; }

        /// <summary>
        /// The <see cref="ISerializer{T}"/> that is used to serialize and
        /// deserialize the values.
        /// </summary>
        public ISerializer<TValue> ValueSerializer { get; private set; }

        /// <summary>
        /// The number of key-value pairs that are stored in this <see cref="BTree{TKey, TValue}"/>.
        /// </summary>
        public abstract long Count { get; protected internal set; }

        /// <summary>
        /// The index on the <see cref="PageStorage"/> where the root <see cref="BTreeNode{TKey, TValue}"/>
        /// is stored, or null if there is no root node.
        /// </summary>
        /// <seealso cref="Root"/>
        protected internal abstract long? RootPageIndex { get; set; }

        /// <summary>
        /// The root <see cref="BTreeNode{TKey, TValue}"/>, or null.
        /// </summary>
        internal BTreeNode<TKey, TValue> Root
        {
            get
            {
                lock (locker)
                {
                    long? index = RootPageIndex;
                    if (index.HasValue)
                        return new BTreeNode<TKey, TValue>(this, index.Value);
                    else
                        return null;
                }
            }

            set
            {
                lock (locker)
                {
                    if (IsReadOnly)
                        throw new InvalidOperationException("Cannot set the root of a read-only " + nameof(BTree<TKey, TValue>) + ".");

                    if (value != null)
                        RootPageIndex = value.PageIndex;
                    else
                        RootPageIndex = null;
                }
            }
        }

        internal long MaxMoveKeyValuePairCount { get; private set; }

        private readonly object locker = new object();

        /// <summary>
        /// <see cref="BTree{TKey, TValue}"/> constructor.
        /// </summary>
        /// <param name="pageStorage">The <see cref="IPageStorage"/> on which this <see cref="BTree{TKey, TValue}"/> is
        /// stored or will be stored.</param>
        /// <param name="keySerializer">The <see cref="ISerializer{T}"/> that will be used to serialize and
        /// deserialize the keys.</param>
        /// <param name="valueSerializer">The <see cref="ISerializer{T}"/> that will be used to serialize and
        /// deserialize the values.</param>
        /// <param name="maxMovePairCount">The maximum size of the internal buffer that may be used to move key-value
        /// pairs during certain operations such as insertions, measured in the number of key-value pairs. Must be
        /// at least one.</param>
        /// <remarks>
        /// <para>
        /// Note that the <paramref name="pageStorage"/>'s pages must be sufficiently large to store a <see cref="BTreeNode{TKey, TValue}"/>
        /// with several key-value pairs. The very minimum key-value pair capacity is <see cref="BTreeNode{TKey, TValue}.VeryMinKeyValuePairCapacity"/>,
        /// but most applications should use a significantly larger capacity. To determine the key-value pair capacity for a given
        /// page size, see <see cref="BTreeNode{TKey, TValue}.GetRequiredPageSize(long, long, long)"/>. The 'key size' and
        /// 'value size' are defined by the <see cref="ISerializer{T}.DataSize"/> of the <paramref name="keySerializer"/>
        /// and <paramref name="valueSerializer"/>.
        /// </para>
        /// <para>
        /// During some operations, such as insertions, some <see cref="BTreeNode{TKey, TValue}"/>s may have to move around some
        /// of their key-value pairs. In most cases, it will be more efficient if several key-value pairs are moved at once
        /// (rather than one at a time). The <paramref name="maxMovePairCount"/> argument defines the maximum number of
        /// key-value pairs that will be moved at once. A larger number should result in improved speed, but at the cost
        /// of increased runtime memory. The size, in bytes, of the internal buffer is approximately the size of the
        /// binary-serialized keys and values (see <see cref="ISerializer{T}.DataSize"/> for <paramref name="keySerializer"/>
        /// and <paramref name="valueSerializer"/>) plus eight bytes of overhead, all multiplied by the <paramref name="maxMovePairCount"/> 
        /// argument.
        /// </para>
        /// </remarks>
        /// <exception cref="ArgumentNullException">Thrown if <paramref name="pageStorage"/>, <paramref name="keySerializer"/>,
        /// or <paramref name="valueSerializer"/> is null.</exception>
        /// <exception cref="ArgumentOutOfRangeException">Thrown if <paramref name="maxMovePairCount"/> is less than one.</exception>
        /// <exception cref="ArgumentException">Thrown if the <paramref name="pageStorage"/>'s pages are too small to contain
        /// a <see cref="BTreeNode{TKey, TValue}"/> with <see cref="BTreeNode{TKey, TValue}.VeryMinKeyValuePairCapacity"/>
        /// key-value pairs. See <see cref="BTreeNode{TKey, TValue}.GetKeyValuePairCapacityForPageSize(long, long, long)"/>
        /// and <see cref="BTreeNode{TKey, TValue}.GetRequiredPageSize(long, long, long)"/>.</exception>
        public BTree(IPageStorage pageStorage, ISerializer<TKey> keySerializer, ISerializer<TValue> valueSerializer, long maxMovePairCount = 32)
        {
            this.PageStorage = pageStorage ?? throw new ArgumentNullException(nameof(pageStorage));
            this.KeySerializer = keySerializer ?? throw new ArgumentNullException(nameof(keySerializer));
            this.ValueSerializer = valueSerializer ?? throw new ArgumentNullException(nameof(valueSerializer));
            this.MaxMoveKeyValuePairCount = maxMovePairCount;

            long veryMinRequiredPageSize = BTreeNode<TKey, TValue>.GetRequiredPageSize(keySerializer.DataSize, valueSerializer.DataSize, BTreeNode<TKey, TValue>.VeryMinKeyValuePairCapacity);
            if (pageStorage.PageSize < veryMinRequiredPageSize)
                throw new ArgumentException("The provided " + nameof(IPageStorage) + " has a page size that is insufficient to store a " + nameof(BTreeNode<TKey, TValue>) + " using the specified key and value serializers.");
            if (maxMovePairCount < 1)
                throw new ArgumentOutOfRangeException(nameof(maxMovePairCount));
        }

        internal bool TryGetValueOnNode(TKey key, out TValue valueOrDefault, out BTreeNode<TKey, TValue> onNode, out long onNodeIndex, CancellationToken cancellationToken)
        {
            lock (locker)
            {
                var rootNode = Root;
                if (rootNode != null)
                {
                    if (rootNode.TryFindKeyOnNode(key, out onNode, out onNodeIndex, cancellationToken))
                    {
                        valueOrDefault = onNode.GetValueAt(onNodeIndex);
                        return true;
                    }
                    else
                    {
                        valueOrDefault = default(TValue);
                        onNode = null;
                        onNodeIndex = -1;
                        return false;
                    }
                }
                else
                {
                    valueOrDefault = default(TValue);
                    onNode = null;
                    onNodeIndex = -1;
                    return false;
                }
            }
        }

        /// <summary>
        /// Attempts to get the value that is associated to a particular key in this <see cref="BTree{TKey, TValue}"/>.
        /// </summary>
        /// <param name="key">The key.</param>
        /// <param name="valueOrDefault">Assigned to the value that is associated to the <paramref name="key"/>, or
        /// the default <typeparamref name="TValue"/> value if there is no value associated to the <paramref name="key"/>.</param>
        /// <param name="cancellationToken"><see cref="CancellationToken"/> that can be used to cancel the search
        /// operation. Upon cancellation, false is returned.</param>
        /// <returns>True if the value was found, otherwise false. False indicates that the <paramref name="key"/> was not
        /// present in this <see cref="BTree{TKey, TValue}"/>, or that the search was cancelled via the
        /// <paramref name="cancellationToken"/>.</returns>
        public bool TryGetValue(TKey key, out TValue valueOrDefault, CancellationToken cancellationToken)
        {
            lock (locker)
            {
                return TryGetValueOnNode(key, out valueOrDefault, out _, out _, cancellationToken);
            }
        }

        /// <summary>
        /// Checks whether this <see cref="BTree{TKey, TValue}"/> contains a specific key.
        /// </summary>
        /// <param name="key">The key to find.</param>
        /// <param name="cancellationToken"><see cref="CancellationToken"/> that can be used to cancel the search
        /// operation. Upon cancellation, false is returned.</param>
        /// <returns>True if the <paramref name="key"/> was found in this <see cref="BTree{TKey, TValue}"/>. False
        /// indicates that it is not stored in this <see cref="BTree{TKey, TValue}"/>, or that the search operation
        /// was cancelled via the <paramref name="cancellationToken"/>.</returns>
        public bool ContainsKey(TKey key, CancellationToken cancellationToken)
        {
            lock (locker)
            {
                return TryGetValue(key, out _, cancellationToken);
            }
        }

        /// <summary>
        /// Inserts a new key-value pair to this <see cref="BTree{TKey, TValue}"/>, or optionally updates an existing
        /// one.
        /// </summary>
        /// <param name="key">The key.</param>
        /// <param name="value">The value.</param>
        /// <param name="updateIfExists">If this <see cref="BTree{TKey, TValue}"/> already contains a key-value pair
        /// with the specified <paramref name="key"/>, should it be updated?</param>
        /// <param name="alreadyExists">Assigned to true if the <paramref name="key"/> was already present in this
        /// <see cref="BTree{TKey, TValue}"/>, otherwise false.</param>
        /// <returns>True if any change was made, otherwise false. False indicates that the insertion was rejected
        /// because the <paramref name="key"/> is already stored in this <see cref="BTree{TKey, TValue}"/> while
        /// <paramref name="updateIfExists"/> is false, or the insertion failed due to an allocation failure 
        /// (when <see cref="IPageStorage.TryAllocatePage(out long)"/> returns false).</returns>
        /// <remarks>
        /// <para>
        /// This method may split any <see cref="BTreeNode{TKey, TValue}"/>s that are discovered to be at
        /// full capacity (<see cref="BTreeNode{TKey, TValue}.MaxKeyValuePairCapacity"/>) [often called
        /// 'preemptive split' or 'proactive insertion']. Even if false is returned (due to insertion 
        /// rejection, or allocation failure), some <see cref="BTreeNode{TKey, TValue}"/>s may have been 
        /// split. This will <em>not</em> have any effect on the correctness of the 
        /// <see cref="BTree{TKey, TValue}"/> or any data that is stored in it. When false is returned, the 
        /// caller can be confident that all stored key-value pairs are unchanged, though they may have 
        /// internally moved between <see cref="BTreeNode{TKey, TValue}"/>s (a result of splitting).
        /// </para>
        /// <para>
        /// Insertion may sometimes require a new <see cref="BTreeNode{TKey, TValue}"/> to be allocated on the
        /// <see cref="PageStorage"/>. If the <see cref="PageStorage"/> is at full capacity (<see cref="IPageStorage.PageCapacity"/>),
        /// then inflation may be required to insert a new key-value pair. This method will automatically try to inflate
        /// the <see cref="PageStorage"/> by one page when necessary (unless <see cref="IPageStorage.IsCapacityFixed"/>
        /// is true) via the <see cref="IPageStorage.TryInflate(long, IProgress{ProgressReport}, CancellationToken)"/> method.
        /// If allocation or inflation fails (or <see cref="IPageStorage.IsCapacityFixed"/> is true when inflation is
        /// necessary), then it may not be possible to insert certain <em>new</em> key-value pairs (depending on the 
        /// value of the <paramref name="key"/> and the current structure of the B-Tree). However, even if inflation 
        /// or allocation fails, it will be possible to update the value associated to an <em>existing</em> 
        /// <paramref name="key"/>.
        /// </para>
        /// <para>
        /// If any <see cref="Exception"/> is thrown (except the <see cref="InvalidOperationException"/> mentioned
        /// in this document), then the application should assume that data may have been corrupted.
        /// </para>
        /// </remarks>
        /// <exception cref="InvalidOperationException">Thrown if <see cref="IsReadOnly"/> is true or if this method
        /// was called from within a <see cref="Traverse(bool)"/> enumeration.</exception>
        public bool Insert(TKey key, TValue value, bool updateIfExists, out bool alreadyExists)
        {
            if (IsReadOnly)
                throw new InvalidOperationException("Cannot insert to a read-only " + nameof(BTree<TKey, TValue>) + ".");

            lock (locker)
            {
                if (isTraversing)
                    throw new InvalidOperationException("Cannot insert key-value pairs to a " + nameof(BTreeNode<TKey, TValue>) + " while traversing it.");

                var rootNode = Root;
                if (rootNode == null)
                {
                    alreadyExists = false;//The tree is empty, so the key certainly does not already exist
                    if (BTreeNode<TKey, TValue>.TryCreateNew(this, true, out var newRootNode))
                    {
                        newRootNode.KeyValuePairCount = 1;
                        newRootNode.SetKeyAt(0, key);
                        newRootNode.SetValueAt(0, value);
                        Root = newRootNode;
                        Count++;
                        return true;
                    }
                    else
                    {
                        //Failed to allocate the root node
                        return false;
                    }
                }
                else if (rootNode.KeyValuePairCount == rootNode.MaxKeyValuePairCapacity)
                {
                    //The root node is full, so we will split it now
                    if (BTreeNode<TKey, TValue>.TryCreateNew(this, false, out var newRootNode))//Allocate a new root node
                    {
                        if (BTreeNode<TKey, TValue>.TryCreateNew(this, false, out var splitUsingNode))//Allocate the 'right' part of the split node
                        {
                            newRootNode.KeyValuePairCount = 1;//Just to allow the split
                            newRootNode.SetSubTreeAt(1, null);//Temporary, just to allow the split [otherwise sub tree at 1 will be undefined, very dangerous!]
                            newRootNode.SetSubTreeAt(0, rootNode);
                            newRootNode.SplitSubTreeAt(0, splitUsingNode);
                            newRootNode.KeyValuePairCount--;//Restore to 'correct'

                            //Now 'newRootNode' has only one KeyValuePair (and two sub-trees, one left, one right)
                            //Determine whether we will insert the new key-value pair to the left or the right sub-tree
                            long dstSubTreeIndex = 0;
                            if (newRootNode.GetKeyAt(0).CompareTo(key) < 0)
                                dstSubTreeIndex = 1;

                            Root = newRootNode;

                            if (newRootNode.GetSubTreeAt(dstSubTreeIndex).Insert(key, value, updateIfExists, out alreadyExists))
                            {
                                if (!alreadyExists)
                                    Count++;

                                return true;
                            }
                            else
                            {
                                return false;
                            }
                        }
                        else
                        {
                            //Free 'newRootNode', since we failed to use it
                            PageStorage.FreePage(newRootNode.PageIndex);

                            //Since allocation failed, we cannot insert a new key-value pair.
                            //But we can still update the value of an existing key-value pair, so don't return yet!
                        }
                    }
                    else
                    {
                        //Failed to allocate a new root node, so we cannot insert a new key-value pair.
                        //But we can still update the value of an existing key-value pair, so don't return yet!
                    }

                    //If we made it here, insertion of a new key-value pair failed.
                    if (updateIfExists)
                    {
                        //Try to update the value of the existing key-value pair (if it exists)
                        if (TryUpdateValue(key, value))
                        {
                            alreadyExists = true;
                            return true;
                        }
                        else
                        {
                            //Does not exist, and insertion failed due to allocation failure
                            alreadyExists = false;
                            return false;
                        }
                    }
                    else
                    {
                        //We are not allowed to update any existing key-value pair, so insertion has failed.
                        //But we still need to let the caller know whether the key already exists.
                        alreadyExists = ContainsKey(key, new CancellationToken(false));
                        return false;
                    }
                }
                else
                {
                    //Insert starting from the root
                    if (rootNode.Insert(key, value, updateIfExists, out alreadyExists))
                    {
                        if (!alreadyExists)
                            Count++;

                        return true;
                    }
                    else
                    {
                        return false;
                    }
                }
            }
        }

        private bool TryUpdateValue(TKey key, TValue newValue)
        {
            lock (locker)
            {
                var rootNode = Root;
                if (rootNode != null)
                {
                    byte[] valueBuffer = new byte[ValueSerializer.DataSize];
                    ValueSerializer.Serialize(newValue, valueBuffer);
                    return rootNode.TryUpdateValue(key, valueBuffer);
                }
                else
                {
                    return false;
                }
            }
        }

        /// <summary>
        /// Removes a key-value pair from this <see cref="BTree{TKey, TValue}"/>.
        /// </summary>
        /// <param name="key">The key that defines the key-value pair to remove.</param>
        /// <param name="valueOrDefault">Upon removal, assigned to the value that was removed. Otherwise
        /// assigned to the default <typeparamref name="TValue"/> value.</param>
        /// <returns>True if the key-value pair was removed. False indicates that the <paramref name="key"/>
        /// was not present in this <see cref="BTree{TKey, TValue}"/>.</returns>
        /// <remarks>
        /// <para>
        /// Even though the <see cref="Insert(TKey, TValue, bool, out bool)"/>
        /// method may inflate the <see cref="PageStorage"/> if necessary, this method will never deflate it.
        /// </para>
        /// <para>
        /// If any <see cref="Exception"/> is thrown (except the <see cref="InvalidOperationException"/> mentioned
        /// in this document), then the application should assume that data may have been corrupted.
        /// </para>
        /// </remarks>
        /// <exception cref="InvalidOperationException">Thrown if <see cref="IsReadOnly"/> is true or if this method 
        /// was called from within a <see cref="Traverse(bool)"/> enumeration.</exception>
        public bool Remove(TKey key, out TValue valueOrDefault)
        {
            if (IsReadOnly)
                throw new InvalidOperationException("Cannot remove from a read-only " + nameof(BTree<TKey, TValue>) + ".");

            lock (locker)
            {
                if (isTraversing)
                    throw new InvalidOperationException("Cannot remove a key-value pair from a " + nameof(BTree<TKey, TValue>) + " while traversing it.");

                var rootNode = Root;
                if (rootNode != null)
                {
                    bool ret = rootNode.Remove(key, out valueOrDefault);

                    if (rootNode.KeyValuePairCount == 0)
                    {
                        //We completely emptied the root node
                        if (rootNode.IsLeaf)
                        {
                            //And there are no sub-trees, so we emptied the whole tree
                            this.Root = null;
                        }
                        else
                        {
                            //The new root becomes the only sub-tree
                            this.Root = rootNode.GetSubTreeAt(0);
                        }

                        //Delete the old root node's page
                        PageStorage.FreePage(rootNode.PageIndex);
                    }

                    if (ret)
                        Count--;

                    return ret;
                }
                else
                {
                    //There is no root node, so no key exists!
                    valueOrDefault = default(TValue);
                    return false;
                }
            }
        }

        private bool isTraversing = false;
        /// <summary>
        /// Traverses through all <see cref="KeyValuePair{TKey, TValue}"/>s in this <see cref="BTree{TKey, TValue}"/>.
        /// </summary>
        /// <param name="ascending">Should the pairs be enumerated by ascending key order? If false, descending
        /// key order will be used.</param>
        /// <returns>All <see cref="KeyValuePair{TKey, TValue}"/>s.</returns>
        public IEnumerable<KeyValuePair<TKey, TValue>> Traverse(bool ascending)
        {
            lock (locker)
            {
                try
                {
                    isTraversing = true;
                    var rootNode = Root;
                    if (rootNode != null)
                    {
                        foreach (var pair in rootNode.Traverse(ascending))
                            yield return pair;
                    }
                }
                finally
                {
                    isTraversing = false;
                }
            }
        }

        /// <summary>
        /// Traverses through the entire structure of this <see cref="BTree{TKey, TValue}"/>, searching for
        /// any indications of corrupt data.
        /// </summary>
        /// <param name="cancellationToken"><see cref="CancellationToken"/> that can be used to cancel the
        /// validation operation.</param>
        /// <remarks>
        /// <para>
        /// <see cref="IsReadOnly"/> must be true when calling this method, since validation requires the state
        /// to remain unchanged during the entire process.
        /// </para>
        /// <para>
        /// Note that this operation should be expected to be slow, as it must traverse through the entire
        /// tree structure several times. While validation is being performed, it is still possible to
        /// perform read operations on this <see cref="BTree{TKey, TValue}"/> on a different thread.
        /// </para>
        /// <para>
        /// If this method returns, then no corruption has been detected. When corruption is detected, this
        /// method will throw a <see cref="CorruptDataException"/>. Note that this method may not detect
        /// all possible forms of corruption.
        /// </para>
        /// </remarks>
        /// <exception cref="CorruptDataException">Thrown if any corrupt data is found.</exception>
        /// <exception cref="InvalidOperationException">Thrown if <see cref="IsReadOnly"/> is false.</exception>
        public void Validate(CancellationToken cancellationToken)
        {
            if (!IsReadOnly)
                throw new InvalidOperationException("Cannot validate a read-only " + nameof(BTree<TKey, TValue>) + ".");

            var rootNode = Root;
            if (rootNode != null)
            {
                long gotKeyCount = 0;
                long maxKeyCount = Count;
                rootNode.Validate(cancellationToken, ref gotKeyCount, maxKeyCount);
                if (gotKeyCount != Count)
                    throw new CorruptDataException("Found more key-value pairs in the " + nameof(BTree<TKey, TValue>) + " than was specified by the " + nameof(Count) + " property.");
            }
            else
            {
                if (Count != 0)
                    throw new CorruptDataException("There is no root node, but the tree's " + nameof(Count) + " property is non-zero.");
            }
        }
    }
}
