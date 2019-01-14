using Microsoft.VisualStudio.TestTools.UnitTesting;
using Storage.Data;
using System;

namespace StorageTest
{
    [TestClass]
    public class DataRegionTest
    {
        [DataTestMethod]
        [DataRow(-1, 1)]
        [DataRow(1, -1)]
        [DataRow(2, 1)]
        public void ConstructorThrowsWhenArgsOutOfRange(long firstIndex, long lastIndex)
        {
            Assert.ThrowsException<ArgumentOutOfRangeException>(()=> {
                DataRegion region = new DataRegion(firstIndex, lastIndex);
            });
        }

        [TestMethod]
        public void ConstructorWorks()
        {
            long firstIndex = 3, lastIndex = 9;
            DataRegion region = new DataRegion(firstIndex, lastIndex);
            Assert.AreEqual(firstIndex, region.FirstIndex);
            Assert.AreEqual(lastIndex, region.LastIndex);
            Assert.AreEqual(7, region.Length);
        }

        [DataTestMethod]
        [DataRow(0, 1, 0, 2)]
        [DataRow(0, 2, 0, 3)]
        [DataRow(1, 2, 0, 3)]
        [DataRow(1, 3, 0, 4)]
        [DataRow(2, 2, 0, 3)]
        public void ContainsIndexWorks(long firstIndex, long lastIndex, long scanStartIndex, long scanLastIndex)
        {
            DataRegion region = new DataRegion(firstIndex, lastIndex);
            for(long i = scanStartIndex; i <= scanLastIndex; i++)
            {
                if (i >= firstIndex && i <= lastIndex)
                    Assert.IsTrue(region.ContainsIndex(i));
                else
                    Assert.IsFalse(region.ContainsIndex(i));
            }
        }

        [DataTestMethod]
        [DataRow(0, 0, 0, 0, true)]
        [DataRow(1, 1, 1, 1, true)]
        [DataRow(0, 1, 0, 0, true)]
        [DataRow(0, 1, 1, 1, true)]
        [DataRow(0, 2, 0, 0, true)]
        [DataRow(0, 2, 0, 1, true)]
        [DataRow(0, 2, 0, 2, true)]
        [DataRow(0, 2, 1, 1, true)]
        [DataRow(0, 2, 2, 2, true)]
        [DataRow(1, 2, 1, 1, true)]
        [DataRow(1, 2, 1, 2, true)]
        [DataRow(1, 2, 2, 2, true)]
        [DataRow(1, 3, 2, 2, true)]
        [DataRow(10, 30, 20, 20, true)]
        [DataRow(1, 2, 0, 0, false)]
        [DataRow(1, 2, 0, 1, false)]//Intersecting, but not contained
        [DataRow(1, 2, 2, 3, false)]//Intersecting, but not contained
        [DataRow(1, 2, 3, 3, false)]
        public void ContainsDataRegionWorks(long containerFirstIndex, long containerLastIndex, long containedFirstIndex, long containedLastIndex, bool isContained)
        {
            DataRegion container = new DataRegion(containerFirstIndex, containerLastIndex);
            DataRegion contained = new DataRegion(containedFirstIndex, containedLastIndex);

            if (isContained)
                Assert.IsTrue(container.Contains(contained));
            else
                Assert.IsFalse(container.Contains(contained));
        }

        [DataTestMethod]
        [DataRow(0, 0, 0, 0, true)]
        [DataRow(1, 1, 1, 1, true)]
        [DataRow(0, 1, 0, 0, true)]
        [DataRow(0, 1, 1, 1, true)]
        [DataRow(0, 2, 0, 0, true)]
        [DataRow(0, 2, 0, 1, true)]
        [DataRow(0, 2, 0, 2, true)]
        [DataRow(0, 2, 1, 1, true)]
        [DataRow(0, 2, 2, 2, true)]
        [DataRow(1, 2, 1, 1, true)]
        [DataRow(1, 2, 1, 2, true)]
        [DataRow(1, 2, 2, 2, true)]
        [DataRow(1, 3, 2, 2, true)]
        [DataRow(10, 30, 20, 20, true)]
        [DataRow(1, 2, 0, 0, false)]
        [DataRow(1, 2, 0, 1, true)]//Intersecting, but not contained
        [DataRow(1, 2, 2, 3, true)]//Intersecting, but not contained
        [DataRow(1, 2, 3, 3, false)]
        public void IntersectsDataRegionWorks(long aFirstIndex, long aLastIndex, long bFirstIndex, long bLastIndex, bool isContained)
        {
            DataRegion a = new DataRegion(aFirstIndex, aLastIndex);
            DataRegion b = new DataRegion(bFirstIndex, bLastIndex);

            if (isContained)
            {
                Assert.IsTrue(a.Intersects(b));
                Assert.IsTrue(b.Intersects(a));
            }
            else
            {
                Assert.IsFalse(a.Intersects(b));
                Assert.IsFalse(b.Intersects(a));
            }
        }

        [DataTestMethod]
        [DataRow(0, 0, 0, 0, false)]//Contained does not mean adjacent
        [DataRow(0, 1, 0, 0, false)]//Contained does not mean adjacent
        [DataRow(0, 1, 1, 1, false)]//Contained does not mean adjacent
        [DataRow(0, 5, 1, 1, false)]//Contained does not mean adjacent
        [DataRow(0, 5, 1, 2, false)]//Contained does not mean adjacent
        [DataRow(0, 5, 2, 2, false)]//Contained does not mean adjacent
        [DataRow(0, 5, 0, 5, false)]//Contained does not mean adjacent
        [DataRow(0, 5, 1, 5, false)]//Contained does not mean adjacent
        [DataRow(3, 7, 0, 0, false)]//'b' is too far left
        [DataRow(3, 7, 1, 1, false)]//'b' is too far left
        [DataRow(3, 7, 1, 3, false)]//Intersection does not mean adjacent
        [DataRow(3, 7, 2, 3, false)]//Intersection does not mean adjacent
        [DataRow(3, 7, 1, 4, false)]//Intersection does not mean adjacent
        [DataRow(3, 7, 4, 4, false)]//Intersection does not mean adjacent
        [DataRow(3, 7, 4, 5, false)]//Intersection does not mean adjacent
        [DataRow(3, 7, 4, 7, false)]//Intersection does not mean adjacent
        [DataRow(3, 7, 4, 8, false)]//Intersection does not mean adjacent
        [DataRow(3, 7, 5, 8, false)]//Intersection does not mean adjacent
        [DataRow(3, 7, 7, 8, false)]//Intersection does not mean adjacent
        [DataRow(3, 7, 9, 9, false)]//'b' is too far right
        [DataRow(3, 7, 9, 10, false)]//'b' is too far right
        [DataRow(3, 7, 0, 2, true)]
        [DataRow(3, 7, 1, 2, true)]
        [DataRow(3, 7, 2, 2, true)]
        [DataRow(3, 7, 8, 8, true)]
        [DataRow(3, 7, 8, 9, true)]
        [DataRow(3, 7, 8, 10, true)]
        [DataRow(0, 0, 1, 1, true)]
        [DataRow(2, 2, 1, 1, true)]
        public void IsAdjacentWorks(long aFirstIndex, long aLastIndex, long bFirstIndex, long bLastIndex, bool areAdjacent)
        {
            DataRegion a = new DataRegion(aFirstIndex, aLastIndex);
            DataRegion b = new DataRegion(bFirstIndex, bLastIndex);

            if (areAdjacent)
            {
                Assert.IsTrue(a.IsAdjacent(b));
                Assert.IsTrue(b.IsAdjacent(a));
            }
            else
            {
                Assert.IsFalse(a.IsAdjacent(b));
                Assert.IsFalse(b.IsAdjacent(a));
            }
        }

        [DataTestMethod]
        [DataRow(0, 1, 3, 3)]
        [DataRow(0, 1, 3, 4)]
        [DataRow(0, 2, 4, 4)]
        [DataRow(1, 2, 4, 4)]
        [DataRow(1, 2, 4, 5)]
        [DataRow(3, 4, 0, 0)]
        [DataRow(3, 4, 0, 1)]
        [DataRow(3, 4, 1, 1)]
        [DataRow(3, 4, 6, 6)]
        [DataRow(3, 4, 6, 7)]
        [DataRow(3, 4, 7, 7)]
        [DataRow(2, 3, 0, 0)]
        [DataRow(2, 3, 5, 5)]
        [DataRow(2, 3, 5, 6)]
        public void CombineWithThrowsWhenNeitherIntersectingNorAdjacent(long aFirstIndex, long aLastIndex, long bFirstIndex, long bLastIndex)
        {
            DataRegion a = new DataRegion(aFirstIndex, aLastIndex);
            DataRegion b = new DataRegion(bFirstIndex, bLastIndex);

            Assert.ThrowsException<ArgumentException>(() => {
                a.CombineWith(b);
            });

            Assert.ThrowsException<ArgumentException>(() => {
                b.CombineWith(a);
            });
        }

        [DataTestMethod]
        [DataRow(0, 0, 0, 1, 0, 1)]
        [DataRow(0, 0, 0, 2, 0, 2)]
        [DataRow(0, 1, 0, 0, 0, 1)]
        [DataRow(0, 0, 1, 1, 0, 1)]
        [DataRow(0, 3, 0, 0, 0, 3)]
        [DataRow(0, 3, 0, 1, 0, 3)]
        [DataRow(0, 3, 0, 2, 0, 3)]
        [DataRow(0, 3, 0, 3, 0, 3)]
        [DataRow(0, 3, 1, 1, 0, 3)]
        [DataRow(0, 3, 1, 2, 0, 3)]
        [DataRow(0, 3, 1, 3, 0, 3)]
        [DataRow(0, 3, 3, 3, 0, 3)]
        [DataRow(3, 7, 0, 2, 0, 7)]
        [DataRow(3, 7, 1, 2, 1, 7)]
        [DataRow(3, 7, 2, 2, 2, 7)]
        [DataRow(3, 7, 2, 3, 2, 7)]
        [DataRow(3, 7, 2, 4, 2, 7)]
        [DataRow(3, 7, 2, 7, 2, 7)]
        [DataRow(3, 7, 2, 8, 2, 8)]
        [DataRow(3, 7, 3, 7, 3, 7)]
        [DataRow(3, 7, 3, 8, 3, 8)]
        [DataRow(3, 7, 7, 7, 3, 7)]
        [DataRow(3, 7, 8, 8, 3, 8)]
        [DataRow(3, 7, 7, 9, 3, 9)]
        [DataRow(3, 7, 8, 9, 3, 9)]
        public void CombineWithWorks(long aFirstIndex, long aLastIndex, long bFirstIndex, long bLastIndex, long resultFirstIndex, long resultLastIndex)
        {
            DataRegion a = new DataRegion(aFirstIndex, aLastIndex);
            DataRegion b = new DataRegion(bFirstIndex, bLastIndex);
            DataRegion expected = new DataRegion(resultFirstIndex, resultLastIndex);
            Assert.AreEqual(expected, a.CombineWith(b));
            Assert.AreEqual(expected, b.CombineWith(a));
        }

        [DataTestMethod]
        [DataRow(0, 0, 0, 0, true)]
        [DataRow(0, 1, 0, 1, true)]
        [DataRow(1, 1, 1, 1, true)]
        [DataRow(1, 2, 1, 2, true)]
        [DataRow(0, 0, 0, 1, false)]
        [DataRow(0, 0, 0, 1, false)]
        [DataRow(0, 0, 1, 2, false)]
        [DataRow(0, 0, 1, 3, false)]
        [DataRow(1, 2, 2, 2, false)]
        [DataRow(2, 2, 2, 3, false)]
        public void EqualsWorks(long aFirstIndex, long aLastIndex, long bFirstIndex, long bLastIndex, bool areEqual)
        {
            DataRegion a = new DataRegion(aFirstIndex, aLastIndex);
            DataRegion b = new DataRegion(bFirstIndex, bLastIndex);

            if (areEqual)
            {
                Assert.IsTrue(a.Equals(b));
                Assert.IsTrue(b.Equals(a));
            }
            else
            {
                Assert.IsFalse(a.Equals(b));
                Assert.IsFalse(b.Equals(a));
            }
        }

        [DataTestMethod]
        [DataRow(0, 0, 0, 0)]
        [DataRow(0, 1, 0, 1)]
        [DataRow(1, 1, 1, 1)]
        [DataRow(1, 2, 1, 2)]
        [DataRow(3, 7, 3, 7)]
        public void EqualDataRegionsHaveEqualHashCodes(long aFirstIndex, long aLastIndex, long bFirstIndex, long bLastIndex)
        {
            DataRegion a = new DataRegion(aFirstIndex, aLastIndex);
            DataRegion b = new DataRegion(bFirstIndex, bLastIndex);

            Assert.AreEqual(a.GetHashCode(), b.GetHashCode());
        }

        [DataTestMethod]
        [DataRow(0, 0, 0, 1)]
        [DataRow(0, 0, 0, 1)]
        [DataRow(0, 0, 1, 2)]
        [DataRow(0, 0, 1, 3)]
        [DataRow(1, 2, 2, 2)]
        [DataRow(2, 2, 2, 3)]
        public void DifferentDataRegionsProbablyHaveDifferentHashCodes(long aFirstIndex, long aLastIndex, long bFirstIndex, long bLastIndex)
        {
            DataRegion a = new DataRegion(aFirstIndex, aLastIndex);
            DataRegion b = new DataRegion(bFirstIndex, bLastIndex);

            Assert.AreNotEqual(a.GetHashCode(), b.GetHashCode());
        }

        [TestMethod]
        public void EqualsNonDataRegionReturnsFalse()
        {
            DataRegion region = new DataRegion(0, 1);
            Assert.IsFalse(region.Equals(null));
            Assert.IsFalse(region.Equals("string"));
            Assert.IsFalse(region.Equals(0));
            Assert.IsFalse(region.Equals(1));
        }

        [DataTestMethod]
        [DataRow(0, 0)]
        [DataRow(0, 1)]
        [DataRow(1, 1)]
        [DataRow(0, 2)]
        [DataRow(1, 2)]
        [DataRow(2, 2)]
        public void ToStringWorks(long firstIndex, long lastIndex)
        {
            DataRegion region = new DataRegion(firstIndex, lastIndex);
            string expected = "("+firstIndex+", "+lastIndex+")";
            Assert.AreEqual(expected, region.ToString());
        }
    }
}
