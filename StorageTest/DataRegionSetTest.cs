using Microsoft.VisualStudio.TestTools.UnitTesting;
using Storage.Data;
using System;
using System.Linq;

namespace StorageTest
{
    [TestClass]
    public class DataRegionSetTest
    {
        [TestMethod]
        public void ConstructorWorks()
        {
            DataRegionSet set = new DataRegionSet();
            CollectionAssert.AreEquivalent(new DataRegion[0], set.ToArray());
        }

        [TestMethod]
        public void AddNonCombinableDataRegionsWorks()
        {
            DataRegionSet set = new DataRegionSet();
            DataRegion[] regions = new DataRegion[] {
                new DataRegion(0, 0),
                new DataRegion(2, 2),
                new DataRegion(4, 5),
                new DataRegion(7, 7),
                new DataRegion(10, 15),
            };
            
            foreach(var region in regions)
                Assert.AreEqual(region, set.Add(region));

            CollectionAssert.AreEquivalent(regions, set.ToArray());
        }

        [TestMethod]
        public void AddSomeCombinableDataRegionsWorks()
        {
            DataRegionSet set = new DataRegionSet();

            Assert.AreEqual(new DataRegion(0, 0), set.Add(new DataRegion(0, 0)));
            Assert.AreEqual(new DataRegion(0, 1), set.Add(new DataRegion(1, 1)));//Should be combined with left region at (0,0)
            CollectionAssert.AreEquivalent(new DataRegion[] {
                new DataRegion(0, 1)
            }, set.ToArray());

            Assert.AreEqual(new DataRegion(5, 5), set.Add(new DataRegion(5, 5)));
            Assert.AreEqual(new DataRegion(4, 5), set.Add(new DataRegion(4, 4)));//Should be combined with right region at (5, 5)
            CollectionAssert.AreEquivalent(new DataRegion[] {
                new DataRegion(0, 1),
                new DataRegion(4, 5)
            }, set.ToArray());

            Assert.AreEqual(new DataRegion(4, 6), set.Add(new DataRegion(6, 6)));//Should be combined with left region at (4, 5)
            CollectionAssert.AreEquivalent(new DataRegion[] {
                new DataRegion(0, 1),
                new DataRegion(4, 6)
            }, set.ToArray());

            Assert.AreEqual(new DataRegion(13, 17), set.Add(new DataRegion(13, 17)));
            Assert.AreEqual(new DataRegion(9, 17), set.Add(new DataRegion(9, 12)));//Should be combined with right region at (13, 17)
            CollectionAssert.AreEquivalent(new DataRegion[] {
                new DataRegion(0, 1),
                new DataRegion(4, 6),
                new DataRegion(9, 17)
            }, set.ToArray());

            Assert.AreEqual(new DataRegion(9, 17), set.Add(new DataRegion(9, 17)));//Should have no effect since the exact region already exists
            CollectionAssert.AreEquivalent(new DataRegion[] {
                new DataRegion(0, 1),
                new DataRegion(4, 6),
                new DataRegion(9, 17)
            }, set.ToArray());

            Assert.AreEqual(new DataRegion(9, 17), set.Add(new DataRegion(9, 9)));//Should have no effect since the region is already covered
            CollectionAssert.AreEquivalent(new DataRegion[] {
                new DataRegion(0, 1),
                new DataRegion(4, 6),
                new DataRegion(9, 17)
            }, set.ToArray());

            Assert.AreEqual(new DataRegion(9, 17), set.Add(new DataRegion(9, 10)));//Should have no effect since the region is already covered
            CollectionAssert.AreEquivalent(new DataRegion[] {
                new DataRegion(0, 1),
                new DataRegion(4, 6),
                new DataRegion(9, 17)
            }, set.ToArray());

            Assert.AreEqual(new DataRegion(9, 17), set.Add(new DataRegion(9, 13)));//Should have no effect since the region is already covered
            CollectionAssert.AreEquivalent(new DataRegion[] {
                new DataRegion(0, 1),
                new DataRegion(4, 6),
                new DataRegion(9, 17)
            }, set.ToArray());

            Assert.AreEqual(new DataRegion(9, 17), set.Add(new DataRegion(10, 10)));//Should have no effect since the region is already covered
            CollectionAssert.AreEquivalent(new DataRegion[] {
                new DataRegion(0, 1),
                new DataRegion(4, 6),
                new DataRegion(9, 17)
            }, set.ToArray());

            Assert.AreEqual(new DataRegion(9, 17), set.Add(new DataRegion(10, 11)));//Should have no effect since the region is already covered
            CollectionAssert.AreEquivalent(new DataRegion[] {
                new DataRegion(0, 1),
                new DataRegion(4, 6),
                new DataRegion(9, 17)
            }, set.ToArray());

            Assert.AreEqual(new DataRegion(9, 17), set.Add(new DataRegion(11, 11)));//Should have no effect since the region is already covered
            CollectionAssert.AreEquivalent(new DataRegion[] {
                new DataRegion(0, 1),
                new DataRegion(4, 6),
                new DataRegion(9, 17)
            }, set.ToArray());

            Assert.AreEqual(new DataRegion(9, 17), set.Add(new DataRegion(11, 12)));//Should have no effect since the region is already covered
            CollectionAssert.AreEquivalent(new DataRegion[] {
                new DataRegion(0, 1),
                new DataRegion(4, 6),
                new DataRegion(9, 17)
            }, set.ToArray());

            Assert.AreEqual(new DataRegion(9, 17), set.Add(new DataRegion(11, 17)));//Should have no effect since the region is already covered
            CollectionAssert.AreEquivalent(new DataRegion[] {
                new DataRegion(0, 1),
                new DataRegion(4, 6),
                new DataRegion(9, 17)
            }, set.ToArray());

            Assert.AreEqual(new DataRegion(100, 150), set.Add(new DataRegion(100, 150)));
            CollectionAssert.AreEquivalent(new DataRegion[] {
                new DataRegion(0, 1),
                new DataRegion(4, 6),
                new DataRegion(9, 17),
                new DataRegion(100, 150),
            }, set.ToArray());

            Assert.AreEqual(new DataRegion(99, 151), set.Add(new DataRegion(99, 151)));//Should be combined with (100, 150) since it contains all of this region
            CollectionAssert.AreEquivalent(new DataRegion[] {
                new DataRegion(0, 1),
                new DataRegion(4, 6),
                new DataRegion(9, 17),
                new DataRegion(99, 151),
            }, set.ToArray());

            Assert.AreEqual(new DataRegion(95, 155), set.Add(new DataRegion(95, 155)));//Should be combined with (99, 151) since it contains all of this region
            CollectionAssert.AreEquivalent(new DataRegion[] {
                new DataRegion(0, 1),
                new DataRegion(4, 6),
                new DataRegion(9, 17),
                new DataRegion(95, 155),
            }, set.ToArray());

            Assert.AreEqual(new DataRegion(90, 155), set.Add(new DataRegion(90, 95)));//Should be combined with (95, 155) due to intersection at (95, 95)
            CollectionAssert.AreEquivalent(new DataRegion[] {
                new DataRegion(0, 1),
                new DataRegion(4, 6),
                new DataRegion(9, 17),
                new DataRegion(90, 155),
            }, set.ToArray());

            Assert.AreEqual(new DataRegion(85, 155), set.Add(new DataRegion(85, 100)));//Should be combined with (90, 155) due to intersection at (90, 100)
            CollectionAssert.AreEquivalent(new DataRegion[] {
                new DataRegion(0, 1),
                new DataRegion(4, 6),
                new DataRegion(9, 17),
                new DataRegion(85, 155),
            }, set.ToArray());

            Assert.AreEqual(new DataRegion(85, 200), set.Add(new DataRegion(100, 200)));//Should be combined with (85, 155) due to intersection at (100, 155)
            CollectionAssert.AreEquivalent(new DataRegion[] {
                new DataRegion(0, 1),
                new DataRegion(4, 6),
                new DataRegion(9, 17),
                new DataRegion(85, 200),
            }, set.ToArray());

            Assert.AreEqual(new DataRegion(85, 300), set.Add(new DataRegion(200, 300)));//Should be combined with (85, 200) due to intersection at (200, 200)
            CollectionAssert.AreEquivalent(new DataRegion[] {
                new DataRegion(0, 1),
                new DataRegion(4, 6),
                new DataRegion(9, 17),
                new DataRegion(85, 300),
            }, set.ToArray());

            Assert.AreEqual(new DataRegion(0, 6), set.Add(new DataRegion(2, 3)));//Should be combined with (0, 1) AND (4, 6)
            CollectionAssert.AreEquivalent(new DataRegion[] {
                new DataRegion(0, 6),
                new DataRegion(9, 17),
                new DataRegion(85, 300),
            }, set.ToArray());

            Assert.AreEqual(new DataRegion(0, 17), set.Add(new DataRegion(7, 8)));//Should be combined with (0, 6) AND (9, 17)
            CollectionAssert.AreEquivalent(new DataRegion[] {
                new DataRegion(0, 17),
                new DataRegion(85, 300),
            }, set.ToArray());

            Assert.AreEqual(new DataRegion(0, 300), set.Add(new DataRegion(18, 84)));//Should be combined with (0, 17) AND (85, 300)
            CollectionAssert.AreEquivalent(new DataRegion[] {
                new DataRegion(0, 300)
            }, set.ToArray());

            Assert.AreEqual(new DataRegion(500, 600), set.Add(new DataRegion(500, 600)));
            Assert.AreEqual(new DataRegion(900, 1200), set.Add(new DataRegion(900, 1200)));
            Assert.AreEqual(new DataRegion(1500, 2000), set.Add(new DataRegion(1500, 2000)));
            Assert.AreEqual(new DataRegion(2300, 2500), set.Add(new DataRegion(2300, 2500)));
            CollectionAssert.AreEquivalent(new DataRegion[] {
                new DataRegion(0, 300),
                new DataRegion(500, 600),
                new DataRegion(900, 1200),
                new DataRegion(1500, 2000),
                new DataRegion(2300, 2500),
            }, set.ToArray());

            Assert.AreEqual(new DataRegion(0, 2500), set.Add(new DataRegion(250, 2400)));//Should combine with all
            CollectionAssert.AreEquivalent(new DataRegion[] {
                new DataRegion(0, 2500),
            }, set.ToArray());
        }

        [TestMethod]
        public void RemoveWorks()
        {
            DataRegionSet set = new DataRegionSet();
            set.Add(new DataRegion(0, 1000));
            set.Remove(new DataRegion(0, 1000));//Should remove the whole thing
            CollectionAssert.AreEquivalent(new DataRegion[] {
                /*Empty*/
            }, set.ToArray());

            set.Add(new DataRegion(0, 1000));
            set.Remove(new DataRegion(0, 0));//Should remove intersecting region at (0,0)
            CollectionAssert.AreEquivalent(new DataRegion[] {
                new DataRegion(1, 1000),
            }, set.ToArray());

            set.Remove(new DataRegion(0, 1));//Should remove intersecting region at (1, 1)
            CollectionAssert.AreEquivalent(new DataRegion[] {
                new DataRegion(2, 1000),
            }, set.ToArray());

            set.Remove(new DataRegion(1000, 1000));//Should remove intersecting region at (1000, 1000)
            CollectionAssert.AreEquivalent(new DataRegion[] {
                new DataRegion(2, 999),
            }, set.ToArray());

            set.Remove(new DataRegion(0, 5));//Should remove intersecting region at (2, 5)
            CollectionAssert.AreEquivalent(new DataRegion[] {
                new DataRegion(6, 999),
            }, set.ToArray());

            set.Remove(new DataRegion(950, 1500));//Should remove intersecting region at (950, 999)
            CollectionAssert.AreEquivalent(new DataRegion[] {
                new DataRegion(6, 949),
            }, set.ToArray());

            set.Remove(new DataRegion(12, 12));//Should remove intersecting region at (12, 12)
            CollectionAssert.AreEquivalent(new DataRegion[] {
                new DataRegion(6, 11),
                new DataRegion(13, 949),
            }, set.ToArray());

            set.Remove(new DataRegion(100, 120));//Should remove intersecting region at (100, 120)
            CollectionAssert.AreEquivalent(new DataRegion[] {
                new DataRegion(6, 11),
                new DataRegion(13, 99),
                new DataRegion(121, 949),
            }, set.ToArray());

            set.Remove(new DataRegion(200, 300));//Should remove intersecting region at (200, 300)
            CollectionAssert.AreEquivalent(new DataRegion[] {
                new DataRegion(6, 11),
                new DataRegion(13, 99),
                new DataRegion(121, 199),
                new DataRegion(301, 949)
            }, set.ToArray());

            set.Remove(new DataRegion(95, 250));//Should remove intersecting region at (95, 99) AND (121, 199)
            CollectionAssert.AreEquivalent(new DataRegion[] {
                new DataRegion(6, 11),
                new DataRegion(13, 94),
                new DataRegion(301, 949)
            }, set.ToArray());

            set.Remove(new DataRegion(8, 948));//Should remove intersecting region at (8, 11) AND (13, 94) AND (301, 948)
            CollectionAssert.AreEquivalent(new DataRegion[] {
                new DataRegion(6, 7),
                new DataRegion(949, 949)
            }, set.ToArray());

            set.Remove(new DataRegion(8, 948));//Should not change anything
            CollectionAssert.AreEquivalent(new DataRegion[] {
                new DataRegion(6, 7),
                new DataRegion(949, 949)
            }, set.ToArray());

            set.Remove(new DataRegion(0, 10000000));//Should remove everything
            CollectionAssert.AreEquivalent(new DataRegion[] {
                /*Empty*/
            }, set.ToArray());
        }

        [TestMethod]
        public void GetRegionsWithinWorks()
        {
            DataRegionSet set = new DataRegionSet();
            set.Add(new DataRegion(100, 100));
            set.Add(new DataRegion(200, 300));
            set.Add(new DataRegion(400, 450));

            CollectionAssert.AreEquivalent(set.GetRegionsWithin(new DataRegion(0, 500)).ToArray(),
            new DataRegion[] {
                new DataRegion(100, 100),
                new DataRegion(200, 300),
                new DataRegion(400, 450)
            });

            CollectionAssert.AreEquivalent(set.GetRegionsWithin(new DataRegion(100, 100)).ToArray(),
            new DataRegion[] {
                new DataRegion(100, 100)
            });

            CollectionAssert.AreEquivalent(set.GetRegionsWithin(new DataRegion(99, 101)).ToArray(),
            new DataRegion[] {
                new DataRegion(100, 100)
            });

            CollectionAssert.AreEquivalent(set.GetRegionsWithin(new DataRegion(100, 200)).ToArray(),
            new DataRegion[] {
                new DataRegion(100, 100),
                new DataRegion(200, 200),
            });

            CollectionAssert.AreEquivalent(set.GetRegionsWithin(new DataRegion(100, 202)).ToArray(),
            new DataRegion[] {
                new DataRegion(100, 100),
                new DataRegion(200, 202),
            });

            CollectionAssert.AreEquivalent(set.GetRegionsWithin(new DataRegion(99, 200)).ToArray(),
            new DataRegion[] {
                new DataRegion(100, 100),
                new DataRegion(200, 200),
            });

            CollectionAssert.AreEquivalent(set.GetRegionsWithin(new DataRegion(98, 202)).ToArray(),
            new DataRegion[] {
                new DataRegion(100, 100),
                new DataRegion(200, 202),
            });

            CollectionAssert.AreEquivalent(set.GetRegionsWithin(new DataRegion(200, 300)).ToArray(),
            new DataRegion[] {
                new DataRegion(200, 300),
            });

            CollectionAssert.AreEquivalent(set.GetRegionsWithin(new DataRegion(201, 299)).ToArray(),
            new DataRegion[] {
                new DataRegion(201, 299)
            });

            CollectionAssert.AreEquivalent(set.GetRegionsWithin(new DataRegion(210, 250)).ToArray(),
            new DataRegion[] {
                new DataRegion(210, 250)
            });

            CollectionAssert.AreEquivalent(set.GetRegionsWithin(new DataRegion(350, 375)).ToArray(),
            new DataRegion[] {
                /* Empty */
            });

            CollectionAssert.AreEquivalent(set.GetRegionsWithin(new DataRegion(350, 410)).ToArray(),
            new DataRegion[] {
                new DataRegion(400, 410)
            });

            CollectionAssert.AreEquivalent(set.GetRegionsWithin(new DataRegion(350, 1500)).ToArray(),
            new DataRegion[] {
                new DataRegion(400, 450)
            });
        }

        [TestMethod]
        public void GetMissingRegionsWorks()
        {
            DataRegionSet set = new DataRegionSet();

            CollectionAssert.AreEquivalent(set.GetMissingRegions(new DataRegion(0, 0)).ToArray(),
            new DataRegion[] {
                new DataRegion(0, 0)
            });

            CollectionAssert.AreEquivalent(set.GetMissingRegions(new DataRegion(1, 1)).ToArray(),
            new DataRegion[] {
                new DataRegion(1, 1)
            });

            CollectionAssert.AreEquivalent(set.GetMissingRegions(new DataRegion(0, 20)).ToArray(),
            new DataRegion[] {
                new DataRegion(0, 20)
            });

            set.Add(new DataRegion(0, 20));
            CollectionAssert.AreEquivalent(set.GetMissingRegions(new DataRegion(0, 0)).ToArray(),
            new DataRegion[] {
                /*Empty*/
            });

            CollectionAssert.AreEquivalent(set.GetMissingRegions(new DataRegion(1, 1)).ToArray(),
            new DataRegion[] {
                /*Empty*/
            });

            CollectionAssert.AreEquivalent(set.GetMissingRegions(new DataRegion(0, 20)).ToArray(),
            new DataRegion[] {
                /*Empty*/
            });

            CollectionAssert.AreEquivalent(set.GetMissingRegions(new DataRegion(0, 21)).ToArray(),
            new DataRegion[] {
                new DataRegion(21, 21)
            });

            CollectionAssert.AreEquivalent(set.GetMissingRegions(new DataRegion(20, 21)).ToArray(),
            new DataRegion[] {
                new DataRegion(21, 21)
            });

            set.Remove(new DataRegion(0, 5));
            CollectionAssert.AreEquivalent(set.GetMissingRegions(new DataRegion(0, 21)).ToArray(),
            new DataRegion[] {
                new DataRegion(0, 5),
                new DataRegion(21, 21)
            });

            CollectionAssert.AreEquivalent(set.GetMissingRegions(new DataRegion(1, 19)).ToArray(),
            new DataRegion[] {
                new DataRegion(1, 5),
            });

            CollectionAssert.AreEquivalent(set.GetMissingRegions(new DataRegion(1, 29)).ToArray(),
            new DataRegion[] {
                new DataRegion(1, 5),
                new DataRegion(21, 29)
            });

            set.Remove(new DataRegion(10, 10));
            CollectionAssert.AreEquivalent(set.GetMissingRegions(new DataRegion(1, 29)).ToArray(),
            new DataRegion[] {
                new DataRegion(1, 5),
                new DataRegion(10, 10),
                new DataRegion(21, 29)
            });

            CollectionAssert.AreEquivalent(set.GetMissingRegions(new DataRegion(0, 1)).ToArray(),
            new DataRegion[] {
                new DataRegion(0, 1),
            });

            CollectionAssert.AreEquivalent(set.GetMissingRegions(new DataRegion(0, 29)).ToArray(),
            new DataRegion[] {
                new DataRegion(0, 5),
                new DataRegion(10, 10),
                new DataRegion(21, 29)
            });

            CollectionAssert.AreEquivalent(set.GetMissingRegions(new DataRegion(0, 15)).ToArray(),
            new DataRegion[] {
                new DataRegion(0, 5),
                new DataRegion(10, 10),
            });

            CollectionAssert.AreEquivalent(set.GetMissingRegions(new DataRegion(1, 10)).ToArray(),
            new DataRegion[] {
                new DataRegion(1, 5),
                new DataRegion(10, 10),
            });

            CollectionAssert.AreEquivalent(set.GetMissingRegions(new DataRegion(11, 29)).ToArray(),
            new DataRegion[] {
                new DataRegion(21, 29)
            });

            CollectionAssert.AreEquivalent(set.GetMissingRegions(new DataRegion(10, 10)).ToArray(),
            new DataRegion[] {
                new DataRegion(10, 10),
            });
        }

        [TestMethod]
        public void CannotAddWhileEnumerating()
        {
            DataRegionSet set = new DataRegionSet();
            set.Add(new DataRegion(0, 1));
            set.Add(new DataRegion(3, 5));
            set.Add(new DataRegion(10, 11));

            foreach(var region in set)
            {
                Assert.ThrowsException<InvalidOperationException>(()=> {
                    set.Add(new DataRegion(100, 100));
                });
            }
        }

        [TestMethod]
        public void CannotRemoveWhileEnumerating()
        {
            DataRegionSet set = new DataRegionSet();
            set.Add(new DataRegion(0, 1));
            set.Add(new DataRegion(3, 5));
            set.Add(new DataRegion(10, 11));

            foreach (var region in set)
            {
                Assert.ThrowsException<InvalidOperationException>(() => {
                    set.Remove(new DataRegion(100, 100));
                });
            }
        }

        [TestMethod]
        public void EqualsWorks()
        {
            DataRegionSet a = new DataRegionSet();
            DataRegionSet b = new DataRegionSet();

            Assert.IsTrue(a.Equals(b));
            Assert.IsTrue(b.Equals(a));

            a.Add(new DataRegion(0, 0));
            Assert.IsFalse(a.Equals(b));
            Assert.IsFalse(b.Equals(a));

            b.Add(new DataRegion(0, 1));
            Assert.IsFalse(a.Equals(b));
            Assert.IsFalse(b.Equals(a));

            a.Add(new DataRegion(1, 1));
            Assert.IsTrue(a.Equals(b));
            Assert.IsTrue(b.Equals(a));

            b.Add(new DataRegion(10, 20));
            Assert.IsFalse(a.Equals(b));
            Assert.IsFalse(b.Equals(a));

            a.Add(new DataRegion(10, 20));
            Assert.IsTrue(a.Equals(b));
            Assert.IsTrue(b.Equals(a));

            Assert.IsFalse(a.Equals(""));
            Assert.IsFalse(a.Equals(null));
        }

        [TestMethod]
        public void HashCodesMatchWhenEqualAndProbablyDontMatchWhenNotEqual()
        {
            DataRegionSet a = new DataRegionSet();
            DataRegionSet b = new DataRegionSet();
            
            Assert.AreEqual(a.GetHashCode(), b.GetHashCode());

            a.Add(new DataRegion(0, 0));
            Assert.AreNotEqual(a.GetHashCode(), b.GetHashCode());

            b.Add(new DataRegion(0, 1));
            Assert.AreNotEqual(a.GetHashCode(), b.GetHashCode());

            a.Add(new DataRegion(1, 1));
            Assert.AreEqual(a.GetHashCode(), b.GetHashCode());

            b.Add(new DataRegion(10, 20));
            Assert.AreNotEqual(a.GetHashCode(), b.GetHashCode());

            a.Add(new DataRegion(10, 20));
            Assert.AreEqual(a.GetHashCode(), b.GetHashCode());
        }
    }
}
