using System;

namespace Storage.Data
{
    /// <summary>
    /// Defines a region of data.
    /// </summary>
    public struct DataRegion
    {
        /// <summary>
        /// The first index of the data region.
        /// </summary>
        public long FirstIndex { get; private set; }

        /// <summary>
        /// The last index of the data region.
        /// </summary>
        public long LastIndex { get; private set; }

        /// <summary>
        /// The length of the data.
        /// </summary>
        public long Length { get { return (LastIndex - FirstIndex) + 1; } }

        /// <summary>
        /// <see cref="DataRegion"/> constructor.
        /// </summary>
        /// <param name="firstIndex">The first index of the data region.</param>
        /// <param name="lastIndex">The last index of the data region.</param>
        /// <exception cref="ArgumentOutOfRangeException">Thrown if <paramref name="firstIndex"/>
        /// or <paramref name="lastIndex"/> is less than zero, or if <paramref name="lastIndex"/>
        /// is less than <paramref name="firstIndex"/>.</exception>
        public DataRegion(long firstIndex, long lastIndex)
        {
            if (firstIndex < 0)
                throw new ArgumentOutOfRangeException(nameof(firstIndex), "The first index cannot be less than zero.");
            if (lastIndex < 0)
                throw new ArgumentOutOfRangeException(nameof(lastIndex), "The last index cannot be less than zero.");
            if (lastIndex < firstIndex)
                throw new ArgumentOutOfRangeException(nameof(lastIndex), "The last index cannot be less than the first index.");

            this.FirstIndex = firstIndex;
            this.LastIndex = lastIndex;
        }
        
        /// <summary>
        /// Checks whether this <see cref="DataRegion"/> contains a specific index of data.
        /// </summary>
        /// <param name="index">The index.</param>
        /// <returns>True if this <see cref="DataRegion"/> contains the specified <paramref name="index"/>,
        /// otherwise false.</returns>
        public bool ContainsIndex(long index)
        {
            return index >= FirstIndex && index <= LastIndex;
        }
        
        /// <summary>
        /// Checks whether this <see cref="DataRegion"/> completely contains another.
        /// </summary>
        /// <param name="otherRegion">The other <see cref="DataRegion"/>.</param>
        /// <returns>True if the <paramref name="otherRegion"/> completely fits within
        /// this <see cref="DataRegion"/>, otherwise false.</returns>
        public bool Contains(DataRegion otherRegion)
        {
            return otherRegion.FirstIndex >= this.FirstIndex && otherRegion.LastIndex <= this.LastIndex;
        }

        /// <summary>
        /// Checks whether this <see cref="DataRegion"/> intersects another.
        /// </summary>
        /// <param name="otherRegion">The other <see cref="DataRegion"/>.</param>
        /// <returns>True if this <see cref="DataRegion"/> contains at least one
        /// index that is also contained by the <paramref name="otherRegion"/>,
        /// otherwise false.</returns>
        public bool Intersects(DataRegion otherRegion)
        {
            return this.LastIndex >= otherRegion.FirstIndex && otherRegion.LastIndex >= this.FirstIndex;
        }

        /// <summary>
        /// Checks whether this <see cref="DataRegion"/> is adjacent to another.
        /// </summary>
        /// <param name="otherRegion">The other <see cref="DataRegion"/>.</param>
        /// <returns>True if this <see cref="DataRegion"/> is adjacent to <paramref name="otherRegion"/>,
        /// otherwise false.</returns>
        /// <remarks>
        /// Two <see cref="DataRegion"/>s are considered adjacent only if there is
        /// no gap between them. Intersecting <see cref="DataRegion"/>s are <em>not</em>
        /// considered adjacent.
        /// </remarks>
        public bool IsAdjacent(DataRegion otherRegion)
        {
            return otherRegion.LastIndex == this.FirstIndex - 1//'otherRegion' is left adjacent to 'this'
                || otherRegion.FirstIndex == this.LastIndex + 1;//'otherRegion' is right adjacent to 'this'
        }

        /// <summary>
        /// Checks whether this <see cref="DataRegion"/> can be combined with another.
        /// </summary>
        /// <param name="otherRegion">The other <see cref="DataRegion"/>.</param>
        /// <returns>True if this <see cref="DataRegion"/> can be combined with the <paramref name="otherRegion"/>,
        /// otherwise false.</returns>
        /// <remarks>
        /// Two <see cref="DataRegion"/>s can be combined only if they are adjacent or
        /// intersecting. See <see cref="Intersects(DataRegion)"/> and
        /// <see cref="IsAdjacent(DataRegion)"/>.
        /// </remarks>
        public bool CanCombineWith(DataRegion otherRegion)
        {
            return IsAdjacent(otherRegion) || Intersects(otherRegion);
        }

        /// <summary>
        /// Combines this <see cref="DataRegion"/> with another one.
        /// </summary>
        /// <param name="otherRegion">The other <see cref="DataRegion"/> that intersects
        /// with, or is adjacent to, this one.</param>
        /// <returns>The combined <see cref="DataRegion"/>.</returns>
        /// <exception cref="ArgumentException">Thrown if the <paramref name="otherRegion"/>
        /// does not intersect with this one and is not adjacent to this one. 
        /// See <see cref="CanCombineWith(DataRegion)"/>.</exception>
        public DataRegion CombineWith(DataRegion otherRegion)
        {
            if (!this.CanCombineWith(otherRegion))
                throw new ArgumentException("Cannot combine non-intersecting " + nameof(DataRegion) + "s.", nameof(otherRegion));

            return new DataRegion(Math.Min(this.FirstIndex, otherRegion.FirstIndex), Math.Max(this.LastIndex, otherRegion.LastIndex));
        }

        /// <summary>
        /// Checks whether this <see cref="DataRegion"/> equals another object.
        /// </summary>
        /// <param name="obj">The other object.</param>
        /// <returns>True if the <paramref name="obj"/> is a <see cref="DataRegion"/> that
        /// is equivalent to this one, otherwise false.</returns>
        public override bool Equals(object obj)
        {
            if (obj is DataRegion other)
                return this.FirstIndex == other.FirstIndex && this.LastIndex == other.LastIndex;
            else
                return false;
        }

        /// <summary>
        /// Gets the hash code of this <see cref="DataRegion"/>.
        /// </summary>
        /// <returns>The hash code.</returns>
        public override int GetHashCode()
        {
            int ret = 14741;
            ret = ret * 727 + FirstIndex.GetHashCode();
            ret = ret * 727 + LastIndex.GetHashCode();
            return ret;
        }

        /// <summary>
        /// Generates a <see cref="string"/> representation of this <see cref="DataRegion"/>.
        /// </summary>
        /// <returns>A <see cref="string"/> that describes this <see cref="DataRegion"/>.</returns>
        public override string ToString()
        {
            return "("+FirstIndex+", "+LastIndex+")";
        }
    }
}
