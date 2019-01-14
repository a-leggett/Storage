using System;

namespace Storage.Data
{
    /// <summary>
    /// Class that allows the application to observe the <see cref="BTreeNode{TKey, TValue}"/> structure
    /// of a <see cref="BTree{TKey, TValue}"/>.
    /// </summary>
    /// <typeparam name="TKey">The key type.</typeparam>
    /// <typeparam name="TValue">The value type.</typeparam>
    public sealed class BTreeReader<TKey, TValue> where TKey : IComparable<TKey>
    {
        /// <summary>
        /// The <see cref="BTree{TKey, TValue}"/> from which this <see cref="BTreeReader{TKey, TValue}"/>
        /// reads.
        /// </summary>
        public BTree<TKey, TValue> BTree { get; private set; }

        /// <summary>
        /// <see cref="BTreeReader{TKey, TValue}"/> constructor.
        /// </summary>
        /// <param name="bTree">The <see cref="BTree{TKey, TValue}"/>. Must be read-only.</param>
        /// <exception cref="ArgumentNullException">Thrown if <paramref name="bTree"/> is null.</exception>
        /// <exception cref="ArgumentException">Thrown if <paramref name="bTree"/> is not read-only. See
        /// <see cref="BTree{TKey, TValue}.IsReadOnly"/>.</exception>
        public BTreeReader(BTree<TKey, TValue> bTree)
        {
            if (bTree == null)
                throw new ArgumentNullException(nameof(bTree));
            if (!bTree.IsReadOnly)
                throw new ArgumentException("A " + nameof(BTreeReader<TKey, TValue>) + " can only be constructed using a read-only " + nameof(BTree<TKey, TValue>) + ".", nameof(bTree));

            this.BTree = bTree;
        }

        /// <summary>
        /// The root <see cref="BTreeNode{TKey, TValue}"/>, or null if the <see cref="BTree"/> is empty.
        /// </summary>
        public BTreeNode<TKey, TValue> RootNode
        {
            get
            {
                return BTree.Root;
            }
        }
    }
}
