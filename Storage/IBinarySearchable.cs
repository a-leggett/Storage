using System;
using System.Collections.Generic;

namespace Storage
{
    /// <summary>
    /// Interface for a set of <see cref="KeyValuePair{TKey, TValue}"/>s with unique keys,
    /// stored in ascending key order.
    /// </summary>
    /// <typeparam name="TKey">The type of the key.</typeparam>
    /// <typeparam name="TValue">The type of the value.</typeparam>
    public interface IBinarySearchable<TKey, TValue> where TKey : IComparable<TKey>
    {
        /// <summary>
        /// Gets a <typeparamref name="TKey"/> key that is stored at a particular index.
        /// </summary>
        /// <param name="index">The index.</param>
        /// <returns>The <typeparamref name="TKey"/> key that is stored at the specified 
        /// <paramref name="index"/>.</returns>
        /// <exception cref="ArgumentOutOfRangeException">Thrown if <paramref name="index"/>
        /// is less than zero or greater than or equal to <see cref="Count"/>.</exception>
        /// <remarks>
        /// Keys are stored in ascending order based on their index.
        /// </remarks>
        TKey GetKeyAt(long index);

        /// <summary>
        /// Gets a <typeparamref name="TValue"/> value that is stored at a particular index.
        /// </summary>
        /// <param name="index">The index.</param>
        /// <returns>The <typeparamref name="TValue"/> value that is stored at the specified
        /// <paramref name="index"/>.</returns>
        /// <exception cref="ArgumentOutOfRangeException">Thrown if <paramref name="index"/>
        /// is less than zero or greater than or equal to <see cref="Count"/>.</exception>
        /// <remarks>
        /// Values are stored such that their indices match the indices of their keys.
        /// Calling <see cref="GetKeyAt(long)"/> will get the key that is associated to
        /// the value at <paramref name="index"/>.
        /// </remarks>
        TValue GetValueAt(long index);

        /// <summary>
        /// The number of <see cref="KeyValuePair{TKey, TValue}"/>s that are stored.
        /// </summary>
        long Count { get; }
    }
}
