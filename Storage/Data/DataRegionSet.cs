using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;

namespace Storage.Data
{
    /// <summary>
    /// Set of <see cref="DataRegion"/>s.
    /// </summary>
    public sealed class DataRegionSet : IEnumerable<DataRegion>
    {
        private List<DataRegion> dataRegions;

        private readonly object locker = new object();
        
        /// <summary>
        /// <see cref="DataRegionSet"/> constructor.
        /// </summary>
        public DataRegionSet()
        {
            this.dataRegions = new List<DataRegion>();
        }

        /// <summary>
        /// Adds a <see cref="DataRegion"/> to this <see cref="DataRegionSet"/>.
        /// </summary>
        /// <param name="region">The <see cref="DataRegion"/> to add.</param>
        /// <returns>The input <paramref name="region"/> combined with all intersecting
        /// <see cref="DataRegion"/>s within this <see cref="DataRegionSet"/> (if any).
        /// See <see cref="DataRegion.Intersects(DataRegion)"/> and 
        /// <see cref="DataRegion.CombineWith(DataRegion)"/>.</returns>
        /// <remarks>
        /// If the <paramref name="region"/> intersects with any of the <see cref="DataRegion"/>s
        /// currently stored in this <see cref="DataRegionSet"/>, then it will be combined with
        /// them.
        /// </remarks>
        /// <seealso cref="DataRegion.Intersects(DataRegion)"/>
        /// <seealso cref="DataRegion.CombineWith(DataRegion)"/>
        /// <seealso cref="Remove(DataRegion)"/>
        /// <exception cref="InvalidOperationException">Thrown if this method is called
        /// while enumerating via <see cref="GetEnumerator"/> (on the same thread).</exception>
        public DataRegion Add(DataRegion region)
        {
            lock(locker)
            {
                if (isEnumerating_)
                    throw new InvalidOperationException("Cannot add a " + nameof(DataRegionSet) + " while enumerating a " + nameof(DataRegionSet) + ".");

                for (int i = 0; i < dataRegions.Count; i++)
                {
                    var otherRegion = dataRegions[i];
                    if(region.CanCombineWith(otherRegion))
                    {
                        region = region.CombineWith(otherRegion);
                        dataRegions.RemoveAt(i);
                        i--;//Since we removed an element
                    }
                }

                dataRegions.Add(region);
                return region;
            }
        }

        /// <summary>
        /// Removes a <see cref="DataRegion"/> from this <see cref="DataRegionSet"/>.
        /// </summary>
        /// <param name="toRemove">The <see cref="DataRegion"/> to remove.</param>
        /// <remarks>
        /// Any <see cref="DataRegion"/> stored by this <see cref="DataRegionSet"/>
        /// that intersects with <paramref name="toRemove"/> will be affected. If any
        /// stored <see cref="DataRegion"/> is completely contained by <paramref name="toRemove"/>,
        /// then it will be removed entirely. If any stored <see cref="DataRegion"/>
        /// is partially intersected, then it will be updated to remove whatever portion
        /// was specified by <paramref name="toRemove"/>. This may cause some stored
        /// <see cref="DataRegion"/>s to be split into two pieces (left and right).
        /// </remarks>
        /// <seealso cref="Add(DataRegion)"/>
        /// <exception cref="InvalidOperationException">Thrown if this method is called
        /// while enumerating via <see cref="GetEnumerator"/> (on the same thread).</exception>
        public void Remove(DataRegion toRemove)
        {
            lock(locker)
            {
                if (isEnumerating_)
                    throw new InvalidOperationException("Cannot remove a " + nameof(DataRegion) + " while enumerating a " + nameof(DataRegionSet) + ".");

                for (int i = 0; i < dataRegions.Count; i++)
                {
                    var otherRegion = dataRegions[i];
                    if(toRemove.Intersects(otherRegion))
                    {
                        dataRegions.RemoveAt(i);
                        i--;//Since we removed an element

                        if (otherRegion.FirstIndex < toRemove.FirstIndex)
                        {
                            //The left portion of 'otherRegion' will remain
                            dataRegions.Add(new DataRegion(otherRegion.FirstIndex, toRemove.FirstIndex - 1));
                        }

                        if(otherRegion.LastIndex > toRemove.LastIndex)
                        {
                            //The right portion of 'otherRegion' will remain
                            dataRegions.Add(new DataRegion(toRemove.LastIndex + 1, otherRegion.LastIndex));
                        }
                    }
                }
            }
        }

        /// <summary>
        /// Gets all <see cref="DataRegion"/>s that are stored in this <see cref="DataRegionSet"/>
        /// but bounded by a specific <see cref="DataRegion"/>.
        /// </summary>
        /// <param name="boundaryRegion">The boundary <see cref="DataRegion"/>.</param>
        /// <returns>All stored <see cref="DataRegion"/>s bounded by the <paramref name="boundaryRegion"/>.</returns>
        /// <remarks>
        /// If any stored <see cref="DataRegion"/> spans both inside and outside of the <paramref name="boundaryRegion"/>,
        /// then only the portion of that <see cref="DataRegion"/> that fits inside of the <paramref name="boundaryRegion"/>
        /// will be returned.
        /// </remarks>
        /// <seealso cref="GetMissingRegions(DataRegion)"/>
        public IEnumerable<DataRegion> GetRegionsWithin(DataRegion boundaryRegion)
        {
            lock(locker)
            {
                foreach(var region in dataRegions)
                {
                    if(boundaryRegion.Contains(region))
                    {
                        //'region' is completely contained by 'boundaryRegion'
                        yield return region;
                    }
                    else if(boundaryRegion.Intersects(region))
                    {
                        long firstIndex = region.FirstIndex;
                        long lastIndex = region.LastIndex;

                        if (firstIndex < boundaryRegion.FirstIndex)
                            firstIndex = boundaryRegion.FirstIndex;
                        if (lastIndex > boundaryRegion.LastIndex)
                            lastIndex = boundaryRegion.LastIndex;

                        yield return new DataRegion(firstIndex, lastIndex);
                    }
                }
            }
        }

        /// <summary>
        /// Gets all <see cref="DataRegion"/>s that are <em>not</em> stored in
        /// this <see cref="DataRegionSet"/> within a specific boundary <see cref="DataRegion"/>.
        /// </summary>
        /// <param name="boundaryRegion">The boundary <see cref="DataRegion"/>.</param>
        /// <returns>All <see cref="DataRegion"/>s within the <paramref name="boundaryRegion"/>
        /// that are <em>not</em> stored in this <see cref="DataRegionSet"/>.</returns>
        /// <seealso cref="GetRegionsWithin(DataRegion)"/>
        public IEnumerable<DataRegion> GetMissingRegions(DataRegion boundaryRegion)
        {
            lock(locker)
            {
                DataRegionSet negative = new DataRegionSet
                {
                    boundaryRegion
                };
                foreach (var toRemove in this.GetRegionsWithin(boundaryRegion))
                    negative.Remove(toRemove);

                return negative;
            }
        }

        private bool isEnumerating_ = false;
        /// <summary>
        /// Enumerates through all <see cref="DataRegion"/>s, in ascending order.
        /// </summary>
        /// <returns>All <see cref="DataRegion"/>s, in ascending order.</returns>
        public IEnumerator<DataRegion> GetEnumerator()
        {
            lock(locker)
            {
                try
                {
                    isEnumerating_ = true;

                    foreach (var region in dataRegions.OrderBy(x => x.FirstIndex))
                        yield return region;
                }
                finally
                {
                    isEnumerating_ = false;
                }
            }
        }

        /// <summary>
        /// Gets an <see cref="IEnumerator"/> to enumerate through all <see cref="DataRegion"/>s
        /// in ascending order.
        /// </summary>
        /// <returns>The <see cref="IEnumerator"/>.</returns>
        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        /// <summary>
        /// Checks whether this <see cref="DataRegionSet"/> is equivalent to another object.
        /// </summary>
        /// <param name="obj">The other object.</param>
        /// <returns>True if the <paramref name="obj"/> is a <see cref="DataRegionSet"/> that is
        /// equivalent to this one, otherwise false.</returns>
        public override bool Equals(object obj)
        {
            if(obj is DataRegionSet other)
            {
                lock(locker)
                {
                    DataRegion[] otherSet = other.ToArray();
                    DataRegion[] thisSet = this.ToArray();

                    if (otherSet.Length == thisSet.Length)
                    {
                        for(long i = 0; i < otherSet.Length; i++)
                        {
                            if (!otherSet[i].Equals(thisSet[i]))
                                return false;
                        }

                        return true;
                    }
                    else
                    {
                        return false;
                    }
                }
            }
            else
            {
                return false;
            }
        }

        /// <summary>
        /// Gets the hash code of this <see cref="DataRegionSet"/>.
        /// </summary>
        /// <returns>The hash code.</returns>
        public override int GetHashCode()
        {
            lock(locker)
            {
                int ret = 14741;
                foreach(var region in this)
                    ret = ret * 727 + region.GetHashCode();

                return ret;
            }
        }
    }
}
