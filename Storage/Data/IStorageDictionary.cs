using System;
using System.Collections.Generic;

namespace Storage.Data
{
    /// <summary>
    /// Interface for a collection of key-value pairs that are stored on non-volatile storage.
    /// </summary>
    /// <typeparam name="TKey">The type of the key.</typeparam>
    /// <typeparam name="TValue">The type of the value.</typeparam>
    public interface IStorageDictionary<TKey, TValue> : IEnumerable<KeyValuePair<TKey, TValue>>, IDisposable
    {
        /// <summary>
        /// Is this <see cref="IStorageDictionary{TKey, TValue}"/> read-only?
        /// </summary>
        bool IsReadOnly { get; }

        /// <summary>
        /// The number of key-value pairs that are stored.
        /// </summary>
        long Count { get; }

        /// <summary>
        /// Attempts to get the value that is associated to a key.
        /// </summary>
        /// <param name="key">The key.</param>
        /// <param name="valueOrDefault">Assigned to the value that is associated to
        /// the <paramref name="key"/>, or the default <typeparamref name="TValue"/>
        /// if it does not exist.</param>
        /// <returns>True if the key-value pair was found, otherwise false.</returns>
        bool TryGetValue(TKey key, out TValue valueOrDefault);

        /// <summary>
        /// Attempts to add a new key-value pair.
        /// </summary>
        /// <param name="key">The key to add.</param>
        /// <param name="value">The value to add.</param>
        /// <returns>True if the new key-value pair was added, false if it
        /// could not be added only due to a storage limitation.</returns>
        /// <remarks>
        /// This method will attempt to add a new key-value pair to the <see cref="IStorageDictionary{TKey, TValue}"/>.
        /// If the specified <paramref name="key"/> is already associated to a key-value pair stored in this
        /// <see cref="IStorageDictionary{TKey, TValue}"/>, then an <see cref="ArgumentException"/> will be thrown.
        /// If the implementation is unable to add a new key-value pair due to storage limitations, then false
        /// will be returned and nothing will change. All other failures will be reported via the implementation
        /// throwing an <see cref="Exception"/>.
        /// </remarks>
        /// <exception cref="ArgumentException">Thrown if there is already a key-value pair with the specified
        /// <paramref name="key"/>.</exception>
        /// <exception cref="InvalidOperationException">Thrown if <see cref="IsReadOnly"/> is true.</exception>
        bool TryAdd(TKey key, TValue value);

        /// <summary>
        /// Attempts to add a new key-value pair, or update an existing one.
        /// </summary>
        /// <param name="key">The key.</param>
        /// <param name="value">The value.</param>
        /// <param name="alreadyExists">Assigned to true if the key-value pair already existed, otherwise false.</param>
        /// <returns>True if a new key-value pair was added or an existing one's value was updated. False indicates
        /// that there was no existing key-value pair with the <paramref name="key"/>, and a new key-value pair could
        /// not be inserted only due to storage limitations.</returns>
        /// <remarks>
        /// This method will check whether there is an existing key-value pair with the specified <paramref name="key"/>,
        /// and if so, update the value of that key-value pair to the specified new <paramref name="value"/>. If an existing
        /// key-value pair does not exist, then a new one will be added. If the implementation cannot add a new key-value pair due
        /// to storage limitations, then false will be returned and nothing will change. All other failures will be reported by
        /// the implementation throwing an <see cref="Exception"/>.
        /// </remarks>
        /// <exception cref="InvalidOperationException">Thrown if <see cref="IsReadOnly"/> is true.</exception>
        bool TryAddOrUpdate(TKey key, TValue value, out bool alreadyExists);

        /// <summary>
        /// Updates the value to an existing key-value pair.
        /// </summary>
        /// <param name="key">The key.</param>
        /// <param name="value">The new value to assign.</param>
        /// <remarks>
        /// This method will only update the value of an existing key-value pair.
        /// </remarks>
        /// <exception cref="KeyNotFoundException">Thrown if there is no key-value pair with the specified <paramref name="key"/>.</exception>
        /// <exception cref="InvalidOperationException">Thrown if <see cref="IsReadOnly"/> is true.</exception>
        void UpdateValue(TKey key, TValue value);

        /// <summary>
        /// Removes a key-value pair.
        /// </summary>
        /// <param name="key">The key.</param>
        /// <param name="removedValueOrDefault">Assigned to the value that was removed, or default if nothing was removed.</param>
        /// <returns>True if the key-value pair was removed, false if it did not exist.</returns>
        /// <exception cref="InvalidOperationException">Thrown if <see cref="IsReadOnly"/> is true.</exception>
        bool Remove(TKey key, out TValue removedValueOrDefault);

        /// <summary>
        /// Checks whether there is a key-value pair with a specific key.
        /// </summary>
        /// <param name="key">The key to find.</param>
        /// <returns>True if there is a key-value pair with the specified <paramref name="key"/>, otherwise false.</returns>
        bool ContainsKey(TKey key);
    }
}
