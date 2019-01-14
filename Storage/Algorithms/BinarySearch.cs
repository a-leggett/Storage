using System;
using System.Threading;

namespace Storage.Algorithms
{
    /// <summary>
    /// Static class for binary-search methods.
    /// </summary>
    public static class BinarySearch
    {
        /// <summary>
        /// Calculates the maximum number of iterations for a binary search of <paramref name="n"/> elements.
        /// </summary>
        /// <param name="n">The number of elements.</param>
        /// <returns>The maximum number of iterations in a binary search of <paramref name="n"/> elements.</returns>
        public static long CalculateSearchComplexity(long n)
        {
            if (n < 0)
                throw new ArgumentOutOfRangeException(nameof(n));
            if (n == 0 || n == 1)
                return n;

            long ret = 0;
            long i = n;
            while (i > 1)
            {
                i >>= 1;
                ret++;
            }

            //If 'n' was NOT divisible by two, then we add one (round up)
            if ((n & (n - 1)) != 0)
                ret++;

            return ret;
        }

        /// <summary>
        /// Attempts to find the index of a particular key.
        /// </summary>
        /// <typeparam name="TKey">The type of the key.</typeparam>
        /// <typeparam name="TValue">The type of the value.</typeparam>
        /// <param name="searchable">The <see cref="IBinarySearchable{TKey, TValue}"/> to search.</param>
        /// <param name="key">The key to find.</param>
        /// <param name="index">Upon success, assigned to the index where the <paramref name="key"/> was found.
        /// Upon failure, assigned to -1.</param>
        /// <param name="progressReporter"><see cref="IProgress{T}"/> that receives a <see cref="ProgressReport"/> 
        /// as the search progresses. May be null, in which case progress will not be reported.</param>
        /// <param name="cancellationToken"><see cref="CancellationToken"/> that can be used to cancel the search
        /// operation. Upon cancellation, false is returned.</param>
        /// <returns>True if the <paramref name="key"/> was found, otherwise false. False indicates that the
        /// <paramref name="key"/> was not present in this <see cref="IBinarySearchable{TKey, TValue}"/>, or that
        /// the search was cancelled via the <paramref name="cancellationToken"/>.</returns>
        /// <seealso cref="TryFindValue{TKey, TValue}(IBinarySearchable{TKey, TValue}, TKey, out TValue, IProgress{ProgressReport}, CancellationToken)"/>
        /// <seealso cref="IBinarySearchable{TKey, TValue}.GetValueAt(long)"/>
        /// <seealso cref="TryFindCeiling{TKey, TValue}(IBinarySearchable{TKey, TValue}, TKey, out long, out TKey, IProgress{ProgressReport}, CancellationToken)"/>
        /// <seealso cref="TryFindFloor{TKey, TValue}(IBinarySearchable{TKey, TValue}, TKey, out long, out TKey, IProgress{ProgressReport}, CancellationToken)"/>
        public static bool TryFindIndex<TKey, TValue>(this IBinarySearchable<TKey, TValue> searchable, TKey key, out long index, IProgress<ProgressReport> progressReporter = null, CancellationToken cancellationToken = default(CancellationToken)) where TKey : IComparable<TKey>
        {
            if (searchable == null)
                throw new ArgumentNullException(nameof(searchable));
            if (cancellationToken.Equals(default(CancellationToken)))
                cancellationToken = new CancellationToken(false);

            long start = 0, end = searchable.Count - 1;
            long currentProgress = 0, maxProgress = CalculateSearchComplexity(searchable.Count);
            while (start <= end)
            {
                long middle = start + ((end - start) / 2);

                TKey current = searchable.GetKeyAt(middle);
                int comparison = current.CompareTo(key);
                currentProgress++;
                progressReporter?.Report(new ProgressReport(currentProgress, maxProgress));

                if (comparison == 0)
                {
                    //Found it
                    index = middle;
                    progressReporter?.Report(new ProgressReport(maxProgress, maxProgress));
                    return true;
                }
                else if (comparison < 0)
                {
                    start = middle + 1;
                }
                else
                {
                    end = middle - 1;
                }

                if (cancellationToken.IsCancellationRequested)
                {
                    index = -1;
                    return false;
                }
            }

            //Not found
            progressReporter?.Report(new ProgressReport(maxProgress, maxProgress));
            index = -1;
            return false;
        }

        /// <summary>
        /// Attempts to find the value that is associated to a particular key.
        /// </summary>
        /// <typeparam name="TKey">The type of the key.</typeparam>
        /// <typeparam name="TValue">The type of the value.</typeparam>
        /// <param name="searchable">The <see cref="IBinarySearchable{TKey, TValue}"/> to search.</param>
        /// <param name="key">The key to find.</param>
        /// <param name="valueOrDefault">Upon success, assigned to the value that is associated to the
        /// <paramref name="key"/>. Upon failure, assigned to the default <typeparamref name="TValue"/>
        /// value.</param>
        /// <param name="progressReporter"><see cref="IProgress{T}"/> that receives a <see cref="ProgressReport"/>
        /// as the search progresses. May be null, in which case progress will not be reported.</param>
        /// <param name="cancellationToken"><see cref="CancellationToken"/> that can be used to cancel the search
        /// operation. Upon cancellation, false is returned.</param>
        /// <returns>True if the <paramref name="key"/> was found, otherwise false. False indicates that the
        /// <paramref name="key"/> was not present in this <see cref="IBinarySearchable{TKey, TValue}"/>, or that
        /// the search was cancelled via the <paramref name="cancellationToken"/>.</returns>
        /// <seealso cref="TryFindIndex{TKey, TValue}(IBinarySearchable{TKey, TValue}, TKey, out long, IProgress{ProgressReport}, CancellationToken)"/>
        /// <seealso cref="IBinarySearchable{TKey, TValue}.GetValueAt(long)"/>
        /// <seealso cref="TryFindCeiling{TKey, TValue}(IBinarySearchable{TKey, TValue}, TKey, out long, out TKey, IProgress{ProgressReport}, CancellationToken)"/>
        /// <seealso cref="TryFindFloor{TKey, TValue}(IBinarySearchable{TKey, TValue}, TKey, out long, out TKey, IProgress{ProgressReport}, CancellationToken)"/>
        public static bool TryFindValue<TKey, TValue>(this IBinarySearchable<TKey, TValue> searchable, TKey key, out TValue valueOrDefault, IProgress<ProgressReport> progressReporter = null, CancellationToken cancellationToken = default(CancellationToken)) where TKey : IComparable<TKey>
        {
            if (TryFindIndex(searchable, key, out long index, progressReporter, cancellationToken))
            {
                valueOrDefault = searchable.GetValueAt(index);
                return true;
            }
            else
            {
                valueOrDefault = default(TValue);
                return false;
            }
        }

        /// <summary>
        /// Attempts to find the lowest key that is greater than or equal to an input key.
        /// </summary>
        /// <typeparam name="TKey">The type of the key.</typeparam>
        /// <typeparam name="TValue">The type of the value.</typeparam>
        /// <param name="searchable">The <see cref="IBinarySearchable{TKey, TValue}"/> to search.</param>
        /// <param name="key">The input key.</param>
        /// <param name="index">Upon success, assigned to the index of the lowest key that is greater than
        /// or equal to the input <paramref name="key"/>. Upon failure, assigned to -1.</param>
        /// <param name="keyOrDefault">Upon success, assigned to the key with the lowest value that is
        /// greater than or equal to the input <paramref name="key"/>. This will be the <typeparamref name="TKey"/>
        /// that is stored at <paramref name="index"/>. Upon failure, assigned to the default <typeparamref name="TKey"/>
        /// value.</param>
        /// <param name="progressReporter"><see cref="IProgress{T}"/> that receives a <see cref="ProgressReport"/>
        /// as the search progresses. May be null, in which case progress will not be reported.</param>
        /// <param name="cancellationToken"><see cref="CancellationToken"/> that can be used to cancel the search
        /// operation. Upon cancellation, false is returned.</param>
        /// <returns>True if the ceiling was found, otherwise false. False indicates that there is no stored
        /// key that is greater than or equal to the input <paramref name="key"/>, or that the search was
        /// cancelled via the <paramref name="cancellationToken"/>.</returns>
        public static bool TryFindCeiling<TKey, TValue>(this IBinarySearchable<TKey, TValue> searchable, TKey key, out long index, out TKey keyOrDefault, IProgress<ProgressReport> progressReporter = null, CancellationToken cancellationToken = default(CancellationToken)) where TKey : IComparable<TKey>
        {
            if (searchable == null)
                throw new ArgumentNullException(nameof(searchable));
            if (cancellationToken.Equals(default(CancellationToken)))
                cancellationToken = new CancellationToken(false);

            long start = 0, end = searchable.Count - 1;
            long currentProgress = 0, maxProgress = CalculateSearchComplexity(searchable.Count);
            long ceilingIndex = -1;
            TKey ceilingKey = default(TKey);
            while (start <= end)
            {
                long middle = start + (end - start) / 2;
                TKey currentKey = searchable.GetKeyAt(middle);
                long comparison = currentKey.CompareTo(key);

                currentProgress++;
                progressReporter?.Report(new ProgressReport(currentProgress, maxProgress));

                if (comparison == 0)
                {
                    //Found the equal key
                    progressReporter?.Report(new ProgressReport(maxProgress, maxProgress));
                    index = middle;
                    keyOrDefault = currentKey;
                    return true;
                }
                else if (comparison > 0)
                {
                    ceilingIndex = middle;
                    ceilingKey = currentKey;
                    end = middle - 1;
                }
                else
                {
                    start = middle + 1;
                }

                if (cancellationToken.IsCancellationRequested)
                {
                    index = -1;
                    keyOrDefault = default(TKey);
                    return false;
                }
            }

            if (ceilingIndex != -1)
            {
                index = ceilingIndex;
                keyOrDefault = ceilingKey;
                progressReporter?.Report(new ProgressReport(maxProgress, maxProgress));
                return true;
            }
            else
            {
                index = -1;
                keyOrDefault = default(TKey);
                progressReporter?.Report(new ProgressReport(maxProgress, maxProgress));
                return false;
            }
        }

        /// <summary>
        /// Attempts to find the highest key that is less than or equal to an input key.
        /// </summary>
        /// <typeparam name="TKey">The type of the key.</typeparam>
        /// <typeparam name="TValue">The type of the value.</typeparam>
        /// <param name="searchable">The <see cref="IBinarySearchable{TKey, TValue}"/> to search.</param>
        /// <param name="key">The input key.</param>
        /// <param name="index">Upon success, assigned to the index of the highest key that is less than or
        /// equal to the input <paramref name="key"/>. Upon failure, assigned to -1.</param>
        /// <param name="keyOrDefault">Upon success, assigned to the key with the highest value that is less
        /// than or equal to the input <paramref name="key"/>. Upon failure, assigned to the default <typeparamref name="TKey"/>
        /// value.</param>
        /// <param name="progressReporter"><see cref="IProgress{T}"/> that receives a <see cref="ProgressReport"/>
        /// as the search progresses. May be null, in which case progress will not be reported.</param>
        /// <param name="cancellationToken"><see cref="CancellationToken"/> that can be used to cancel the search
        /// operation. Upon cancellation, false is returned.</param>
        /// <returns>True if the floor was found, otherwise false. False indicates that there is no stored key
        /// that is less than or equal to the input <paramref name="key"/>, or that the search was cancelled via
        /// the <paramref name="cancellationToken"/>.</returns>
        public static bool TryFindFloor<TKey, TValue>(this IBinarySearchable<TKey, TValue> searchable, TKey key, out long index, out TKey keyOrDefault, IProgress<ProgressReport> progressReporter = null, CancellationToken cancellationToken = default(CancellationToken)) where TKey : IComparable<TKey>
        {
            if (searchable == null)
                throw new ArgumentNullException(nameof(searchable));
            if (cancellationToken.Equals(default(CancellationToken)))
                cancellationToken = new CancellationToken(false);

            long start = 0, end = searchable.Count - 1;
            long currentProgress = 0, maxProgress = CalculateSearchComplexity(searchable.Count);
            long floorIndex = -1;
            TKey floorKey = default(TKey);
            while (start <= end)
            {
                long middle = start + (end - start) / 2;
                TKey currentKey = searchable.GetKeyAt(middle);
                long comparison = currentKey.CompareTo(key);

                currentProgress++;
                progressReporter?.Report(new ProgressReport(currentProgress, maxProgress));

                if (comparison == 0)
                {
                    //Found the equal key
                    progressReporter?.Report(new ProgressReport(maxProgress, maxProgress));
                    index = middle;
                    keyOrDefault = currentKey;
                    return true;
                }
                else if (comparison < 0)
                {
                    floorIndex = middle;
                    floorKey = currentKey;
                    start = middle + 1;
                }
                else
                {
                    end = middle - 1;
                }

                if (cancellationToken.IsCancellationRequested)
                {
                    index = -1;
                    keyOrDefault = default(TKey);
                    return false;
                }
            }

            if (floorIndex != -1)
            {
                index = floorIndex;
                keyOrDefault = floorKey;
                progressReporter?.Report(new ProgressReport(maxProgress, maxProgress));
                return true;
            }
            else
            {
                index = -1;
                keyOrDefault = default(TKey);
                progressReporter?.Report(new ProgressReport(maxProgress, maxProgress));
                return false;
            }
        }
    }
}
