using Microsoft.VisualStudio.TestTools.UnitTesting;
using Storage;
using Storage.Algorithms;
using StorageTest.Mocks;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;

namespace StorageTest
{
    [TestClass]
    public class BinarySearchTest
    {
        [DataTestMethod]
        [DataRow(-1)]
        [DataRow(-2)]
        [DataRow(-3)]
        public void CalculateSearchComplexityThrowsWhenArgOutOfRange(long badArg)
        {
            Assert.ThrowsException<ArgumentOutOfRangeException>(()=> {
                BinarySearch.CalculateSearchComplexity(badArg);
            });
        }

        [DataTestMethod]
        [DataRow(0, 0)]//No elements requires no search
        [DataRow(1, 1)]//One element requires one search
        [DataRow(2, 1)]//Log2(2)=1
        [DataRow(3, 2)]//Log2(3)=2 (round up)
        [DataRow(4, 2)]//Log2(4)=2
        [DataRow(5, 3)]//Log2(5)=3 (round up)
        [DataRow(6, 3)]//Log2(6)=3 (round up)
        [DataRow(7, 3)]//Log2(7)=3 (round up)
        [DataRow(8, 3)]//Log2(8)=3
        [DataRow(9, 4)]//Log2(9)=4 (round up)
        [DataRow(10, 4)]//Log2(10)=4 (round up)
        [DataRow(15, 4)]//Log2(15)=4 (round up)
        [DataRow(16, 4)]//Log2(16)=4
        [DataRow(17, 5)]//Log2(17)=5 (round up)
        [DataRow(Int32.MaxValue - 2, 31)]
        [DataRow(Int32.MaxValue - 1, 31)]
        [DataRow(Int32.MaxValue, 31)]
        [DataRow((long)Int32.MaxValue + 1, 31)]
        [DataRow((long)Int32.MaxValue + 2, 32)]
        [DataRow(UInt32.MaxValue - 2, 32)]
        [DataRow(UInt32.MaxValue - 1, 32)]
        [DataRow(UInt32.MaxValue, 32)]
        [DataRow((long)UInt32.MaxValue + 1, 32)]
        [DataRow((long)UInt32.MaxValue + 2, 33)]
        [DataRow(Int64.MaxValue - 2, 63)]
        [DataRow(Int64.MaxValue - 1, 63)]
        [DataRow(Int64.MaxValue, 63)]
        public void CalculateSearchComplexityWorks(long n, long expected)
        {
            Assert.AreEqual(expected, BinarySearch.CalculateSearchComplexity(n));
        }

        [TestMethod]
        public void TryFindIndexThrowsWhenArgIsNull()
        {
            Assert.ThrowsException<ArgumentNullException>(()=> {
                BinarySearch.TryFindIndex<long, string>(null, 0, out _);
            });
        }

        [DataTestMethod]
        [DataRow(0)]
        [DataRow(1)]
        [DataRow(2)]
        [DataRow(3)]
        [DataRow(4)]
        [DataRow(5)]
        [DataRow(6)]
        [DataRow(7)]
        [DataRow(8)]
        [DataRow(15)]
        [DataRow(16)]
        [DataRow(17)]
        public void TryFindIndexFailsWhenNotKeyNotPresent(long count)
        {
            long[] badKeys = new long[] { 1, 2, 5, 6, 7, 10, long.MaxValue, long.MinValue };
            MockBinarySearchable searchable = new MockBinarySearchable(count, 5, badKeys);

            foreach(var badKey in badKeys)
            {
                Assert.IsFalse(BinarySearch.TryFindIndex(searchable, badKey, out long index));
                Assert.AreEqual(-1, index);
            }
        }

        [DataTestMethod]
        [DataRow(1)]
        [DataRow(2)]
        [DataRow(3)]
        [DataRow(4)]
        [DataRow(5)]
        [DataRow(6)]
        [DataRow(7)]
        [DataRow(8)]
        [DataRow(14)]
        [DataRow(15)]
        [DataRow(16)]
        [DataRow(17)]
        [DataRow(31)]
        [DataRow(32)]
        [DataRow(33)]
        public void TryFindIndexWorks(long count)
        {
            MockBinarySearchable searchable = new MockBinarySearchable(count, 5);
            foreach (var key in searchable.SortedList.Keys)
            {
                Assert.IsTrue(BinarySearch.TryFindIndex(searchable, key, out long index));
                Assert.AreEqual(key, searchable.GetKeyAt(index));
            }
        }

        [TestMethod]
        public void TryFindIndexWorked_FixedSample1()
        {
            KeyValuePair<long, string>[] sample = new KeyValuePair<long, string>[] {
                new KeyValuePair<long, string>(-3, "0"),
                new KeyValuePair<long, string>(-1, "C"),
                new KeyValuePair<long, string>(0, "B"),
                new KeyValuePair<long, string>(1, "A"),
                new KeyValuePair<long, string>(3, "Value"),
                new KeyValuePair<long, string>(4, "A"),
                new KeyValuePair<long, string>(400, "E"),
                new KeyValuePair<long, string>(401, "F"),
                new KeyValuePair<long, string>(405, "G"),
            };

            MockBinarySearchable searchable = new MockBinarySearchable(sample);
            for (long i = 0; i < sample.LongLength; i++)
            {
                Assert.IsTrue(BinarySearch.TryFindIndex(searchable, sample[i].Key, out long gotIndex));
                Assert.AreEqual(i, gotIndex);
            }
        }

        [TestMethod]
        public void TryFindIndexWorked_FixedSample2()
        {
            KeyValuePair<long, string>[] sample = new KeyValuePair<long, string>[] {
                new KeyValuePair<long, string>(-102, "_"),
                new KeyValuePair<long, string>(0, "A"),
                new KeyValuePair<long, string>(1, "B"),
                new KeyValuePair<long, string>(2, "C"),
                new KeyValuePair<long, string>(4, "D"),
                new KeyValuePair<long, string>(8, "E"),
                new KeyValuePair<long, string>(10, "F"),
                new KeyValuePair<long, string>(13, "G"),
                new KeyValuePair<long, string>(1024, "H"),
            };

            MockBinarySearchable searchable = new MockBinarySearchable(sample);
            for (long i = 0; i < sample.LongLength; i++)
            {
                Assert.IsTrue(BinarySearch.TryFindIndex(searchable, sample[i].Key, out long gotIndex));
                Assert.AreEqual(i, gotIndex);
            }
        }

        [TestMethod]
        public void TryFindIndexReportsProgressWorstCase()
        {
            KeyValuePair<long, string>[] sample = new KeyValuePair<long, string>[] {
                new KeyValuePair<long, string>(-3, "0"),
                new KeyValuePair<long, string>(-1, "C"),
                new KeyValuePair<long, string>(0, "B"),
                new KeyValuePair<long, string>(1, "A"),
                new KeyValuePair<long, string>(3, "Value"),
                new KeyValuePair<long, string>(4, "A"),
                new KeyValuePair<long, string>(400, "E"),
                new KeyValuePair<long, string>(401, "F"),
                new KeyValuePair<long, string>(405, "G"),
            };

            MockBinarySearchable searchable = new MockBinarySearchable(sample);

            List<ProgressReport> gotReports = new List<ProgressReport>();
            ProgressReporter reporter = new ProgressReporter((x) => {
                gotReports.Add(x);
            });

            Assert.IsTrue(searchable.TryFindIndex(-3, out long index, reporter));
            Assert.AreEqual(0, index);
            long complexity = 4;
            CollectionAssert.AreEquivalent(new ProgressReport[] {
                new ProgressReport(1, complexity),
                new ProgressReport(2, complexity),
                new ProgressReport(3, complexity),
                new ProgressReport(4, complexity),
            }, gotReports);
        }

        [TestMethod]
        public void TryFindIndexReportsProgressBestCase()
        {
            KeyValuePair<long, string>[] sample = new KeyValuePair<long, string>[] {
                new KeyValuePair<long, string>(-3, "0"),
                new KeyValuePair<long, string>(-1, "C"),
                new KeyValuePair<long, string>(0, "B"),
                new KeyValuePair<long, string>(1, "A"),
                new KeyValuePair<long, string>(3, "Value"),
                new KeyValuePair<long, string>(4, "A"),
                new KeyValuePair<long, string>(400, "E"),
                new KeyValuePair<long, string>(401, "F"),
                new KeyValuePair<long, string>(405, "G"),
            };

            MockBinarySearchable searchable = new MockBinarySearchable(sample);

            List<ProgressReport> gotReports = new List<ProgressReport>();
            ProgressReporter reporter = new ProgressReporter((x) => {
                gotReports.Add(x);
            });

            Assert.IsTrue(searchable.TryFindIndex(3, out long index, reporter));
            Assert.AreEqual(4, index);
            long complexity = 4;
            CollectionAssert.AreEquivalent(new ProgressReport[] {
                new ProgressReport(1, complexity),
                new ProgressReport(complexity, complexity),//Jumps to full progress when found early
            }, gotReports);
        }

        [TestMethod]
        public void TryFindIndexCanBeCancelled()
        {
            KeyValuePair<long, string>[] sample = new KeyValuePair<long, string>[] {
                new KeyValuePair<long, string>(-3, "0"),
                new KeyValuePair<long, string>(-1, "C"),
                new KeyValuePair<long, string>(0, "B"),
                new KeyValuePair<long, string>(1, "A"),
                new KeyValuePair<long, string>(3, "Value"),
                new KeyValuePair<long, string>(4, "A"),
                new KeyValuePair<long, string>(400, "E"),
                new KeyValuePair<long, string>(401, "F"),
                new KeyValuePair<long, string>(405, "G"),
            };

            MockBinarySearchable searchable = new MockBinarySearchable(sample);
            CancellationTokenSource cancellation = new CancellationTokenSource();
            List<ProgressReport> gotReports = new List<ProgressReport>();
            ProgressReporter reporter = new ProgressReporter((x) => {
                gotReports.Add(x);
                if (gotReports.Count == 2)
                    cancellation.Cancel();
                if (gotReports.Count > 2)
                    Assert.Fail("Expected all progress to stop after the cancellation request.");
            });

            Assert.IsFalse(searchable.TryFindIndex(-3, out long index, reporter, cancellation.Token));
            Assert.AreEqual(-1, index);//Search was cancelled, index not found
            long complexity = 4;
            CollectionAssert.AreEquivalent(new ProgressReport[] {
                new ProgressReport(1, complexity),
                new ProgressReport(2, complexity),
                //Expect cancellation here, so no more progress reports
            }, gotReports);
        }

        [TestMethod]
        public void TryFindValueThrowsWhenArgIsNull()
        {
            Assert.ThrowsException<ArgumentNullException>(() => {
                BinarySearch.TryFindValue<long, string>(null, 0, out _);
            });
        }

        [DataTestMethod]
        [DataRow(0)]
        [DataRow(1)]
        [DataRow(2)]
        [DataRow(3)]
        [DataRow(4)]
        [DataRow(5)]
        [DataRow(6)]
        [DataRow(7)]
        [DataRow(8)]
        [DataRow(15)]
        [DataRow(16)]
        [DataRow(17)]
        public void TryFindValueFailsWhenNotKeyNotPresent(long count)
        {
            long[] badKeys = new long[] { 1, 2, 5, 6, 7, 10, long.MaxValue, long.MinValue };
            MockBinarySearchable searchable = new MockBinarySearchable(count, 5, badKeys);

            foreach (var badKey in badKeys)
            {
                Assert.IsFalse(BinarySearch.TryFindValue(searchable, badKey, out var value));
                Assert.IsNull(value);
            }
        }

        [DataTestMethod]
        [DataRow(1)]
        [DataRow(2)]
        [DataRow(3)]
        [DataRow(4)]
        [DataRow(5)]
        [DataRow(6)]
        [DataRow(7)]
        [DataRow(8)]
        [DataRow(14)]
        [DataRow(15)]
        [DataRow(16)]
        [DataRow(17)]
        [DataRow(31)]
        [DataRow(32)]
        [DataRow(33)]
        public void TryFindValueWorks(long count)
        {
            MockBinarySearchable searchable = new MockBinarySearchable(count, 5);
            foreach (var key in searchable.SortedList.Keys)
            {
                Assert.IsTrue(BinarySearch.TryFindValue(searchable, key, out var value));
                Assert.AreEqual(searchable.SortedList[key], value);
            }
        }

        [TestMethod]
        public void TryFindValueWorked_FixedSample1()
        {
            KeyValuePair<long, string>[] sample = new KeyValuePair<long, string>[] {
                new KeyValuePair<long, string>(-3, "0"),
                new KeyValuePair<long, string>(-1, "C"),
                new KeyValuePair<long, string>(0, "B"),
                new KeyValuePair<long, string>(1, "A"),
                new KeyValuePair<long, string>(3, "Value"),
                new KeyValuePair<long, string>(4, "A"),
                new KeyValuePair<long, string>(400, "E"),
                new KeyValuePair<long, string>(401, "F"),
                new KeyValuePair<long, string>(405, "G"),
            };

            MockBinarySearchable searchable = new MockBinarySearchable(sample);
            for (long i = 0; i < sample.LongLength; i++)
            {
                Assert.IsTrue(BinarySearch.TryFindValue(searchable, sample[i].Key, out var gotValue));
                Assert.AreEqual(sample[i].Value, gotValue);
            }
        }

        [TestMethod]
        public void TryFindValueWorked_FixedSample2()
        {
            KeyValuePair<long, string>[] sample = new KeyValuePair<long, string>[] {
                new KeyValuePair<long, string>(-102, "_"),
                new KeyValuePair<long, string>(0, "A"),
                new KeyValuePair<long, string>(1, "B"),
                new KeyValuePair<long, string>(2, "C"),
                new KeyValuePair<long, string>(4, "D"),
                new KeyValuePair<long, string>(8, "E"),
                new KeyValuePair<long, string>(10, "F"),
                new KeyValuePair<long, string>(13, "G"),
                new KeyValuePair<long, string>(1024, "H"),
            };

            MockBinarySearchable searchable = new MockBinarySearchable(sample);
            for (long i = 0; i < sample.LongLength; i++)
            {
                Assert.IsTrue(BinarySearch.TryFindValue(searchable, sample[i].Key, out var gotValue));
                Assert.AreEqual(sample[i].Value, gotValue);
            }
        }

        [TestMethod]
        public void TryFindValueReportsProgressWorstCase()
        {
            KeyValuePair<long, string>[] sample = new KeyValuePair<long, string>[] {
                new KeyValuePair<long, string>(-3, "0"),
                new KeyValuePair<long, string>(-1, "C"),
                new KeyValuePair<long, string>(0, "B"),
                new KeyValuePair<long, string>(1, "A"),
                new KeyValuePair<long, string>(3, "Value"),
                new KeyValuePair<long, string>(4, "A"),
                new KeyValuePair<long, string>(400, "E"),
                new KeyValuePair<long, string>(401, "F"),
                new KeyValuePair<long, string>(405, "G"),
            };

            MockBinarySearchable searchable = new MockBinarySearchable(sample);

            List<ProgressReport> gotReports = new List<ProgressReport>();
            ProgressReporter reporter = new ProgressReporter((x) => {
                gotReports.Add(x);
            });

            Assert.IsTrue(searchable.TryFindValue(-3, out var value, reporter));
            Assert.AreEqual(sample[0].Value, value);
            long complexity = 4;
            CollectionAssert.AreEquivalent(new ProgressReport[] {
                new ProgressReport(1, complexity),
                new ProgressReport(2, complexity),
                new ProgressReport(3, complexity),
                new ProgressReport(4, complexity),
            }, gotReports);
        }

        [TestMethod]
        public void TryFindValueReportsProgressBestCase()
        {
            KeyValuePair<long, string>[] sample = new KeyValuePair<long, string>[] {
                new KeyValuePair<long, string>(-3, "0"),
                new KeyValuePair<long, string>(-1, "C"),
                new KeyValuePair<long, string>(0, "B"),
                new KeyValuePair<long, string>(1, "A"),
                new KeyValuePair<long, string>(3, "Value"),
                new KeyValuePair<long, string>(4, "A"),
                new KeyValuePair<long, string>(400, "E"),
                new KeyValuePair<long, string>(401, "F"),
                new KeyValuePair<long, string>(405, "G"),
            };

            MockBinarySearchable searchable = new MockBinarySearchable(sample);

            List<ProgressReport> gotReports = new List<ProgressReport>();
            ProgressReporter reporter = new ProgressReporter((x) => {
                gotReports.Add(x);
            });

            Assert.IsTrue(searchable.TryFindValue(3, out var value, reporter));
            Assert.AreEqual(sample[4].Value, value);
            long complexity = 4;
            CollectionAssert.AreEquivalent(new ProgressReport[] {
                new ProgressReport(1, complexity),
                new ProgressReport(complexity, complexity),//Jumps to full progress when found early
            }, gotReports);
        }

        [TestMethod]
        public void TryFindValueCanBeCancelled()
        {
            KeyValuePair<long, string>[] sample = new KeyValuePair<long, string>[] {
                new KeyValuePair<long, string>(-3, "0"),
                new KeyValuePair<long, string>(-1, "C"),
                new KeyValuePair<long, string>(0, "B"),
                new KeyValuePair<long, string>(1, "A"),
                new KeyValuePair<long, string>(3, "Value"),
                new KeyValuePair<long, string>(4, "A"),
                new KeyValuePair<long, string>(400, "E"),
                new KeyValuePair<long, string>(401, "F"),
                new KeyValuePair<long, string>(405, "G"),
            };

            MockBinarySearchable searchable = new MockBinarySearchable(sample);
            CancellationTokenSource cancellation = new CancellationTokenSource();
            List<ProgressReport> gotReports = new List<ProgressReport>();
            ProgressReporter reporter = new ProgressReporter((x) => {
                gotReports.Add(x);
                if (gotReports.Count == 2)
                    cancellation.Cancel();
                if (gotReports.Count > 2)
                    Assert.Fail("Expected all progress to stop after the cancellation request.");
            });

            Assert.IsFalse(searchable.TryFindValue(-3, out var value, reporter, cancellation.Token));
            Assert.IsNull(value);//Search was cancelled, value not found
            long complexity = 4;
            CollectionAssert.AreEquivalent(new ProgressReport[] {
                new ProgressReport(1, complexity),
                new ProgressReport(2, complexity),
                //Expect cancellation here, so no more progress reports
            }, gotReports);
        }






















        [TestMethod]
        public void TryFindCeilingThrowsWhenArgIsNull()
        {
            Assert.ThrowsException<ArgumentNullException>(() => {
                BinarySearch.TryFindCeiling<long, string>(null, 0, out _, out _);
            });
        }

        [DataTestMethod]
        [DataRow(0)]
        [DataRow(1)]
        [DataRow(2)]
        [DataRow(3)]
        [DataRow(4)]
        [DataRow(5)]
        [DataRow(6)]
        [DataRow(7)]
        [DataRow(8)]
        [DataRow(9)]
        [DataRow(15)]
        [DataRow(16)]
        [DataRow(17)]
        [DataRow(1024)]
        public void TryFindCeilingFailsWhenNotFound(long count)
        {
            MockBinarySearchable searchable = new MockBinarySearchable(count, 5);

            //Find a 'bad' key (one that is lower than the lowest)
            long badKey = searchable.Count > 0 ? (searchable.SortedList.Keys.Max() + 1) : -1;

            Assert.IsFalse(BinarySearch.TryFindCeiling(searchable, badKey, out long index, out var key));
            Assert.AreEqual(-1, index);
            Assert.AreEqual(default(long), key);
        }

        [DataTestMethod]
        [DataRow(1)]
        [DataRow(2)]
        [DataRow(3)]
        [DataRow(4)]
        [DataRow(5)]
        [DataRow(6)]
        [DataRow(7)]
        [DataRow(8)]
        [DataRow(9)]
        [DataRow(15)]
        [DataRow(16)]
        [DataRow(17)]
        [DataRow(1022)]
        [DataRow(1023)]
        [DataRow(1024)]
        [DataRow(1025)]
        public void TryFindCeilingWorks(long count)
        {
            KeyValuePair<long, string>[] sample = new KeyValuePair<long, string>[count];
            for (long i = 0; i < count; i++)
                sample[i] = new KeyValuePair<long, string>((i - 5) * 100, "Value_" + i);

            MockBinarySearchable searchable = new MockBinarySearchable(sample);

            //Find the first key that is greater than a key less than the first one (basically: Find the first key)
            Assert.IsTrue(BinarySearch.TryFindCeiling<long, string>(searchable, sample[0].Key - 1, out long index_, out long gotKey_));
            Assert.AreEqual(0, index_);
            Assert.AreEqual(sample[0].Key, gotKey_);

            for (long i = 0; i < count; i++)
            {
                //Find an 'equal' key
                Assert.IsTrue(BinarySearch.TryFindCeiling<long, string>(searchable, sample[i].Key, out long index, out long gotKey));
                Assert.AreEqual(i, index);
                Assert.AreEqual(sample[i].Key, gotKey);

                if (i < count - 1)
                {
                    //Find the first 'greater' key
                    Assert.IsTrue(BinarySearch.TryFindCeiling<long, string>(searchable, sample[i].Key + 1, out index, out gotKey));
                    Assert.AreEqual(i + 1, index);
                    Assert.AreEqual(sample[i + 1].Key, gotKey);
                }
                else
                {
                    //There is no 'greater' key, so ensure it returns false
                    Assert.IsFalse(BinarySearch.TryFindCeiling<long, string>(searchable, sample[i].Key + 1, out index, out gotKey));
                    Assert.AreEqual(-1, index);
                    Assert.AreEqual(default(long), gotKey);
                }
            }
        }

        [TestMethod]
        public void TryFindCeilingReportsProgress_FixedSample1()
        {
            KeyValuePair<long, string>[] sample = new KeyValuePair<long, string>[] {
                new KeyValuePair<long, string>(-3, "0"),
                new KeyValuePair<long, string>(-1, "C"),
                new KeyValuePair<long, string>(0, "B"),
                new KeyValuePair<long, string>(1, "A"),
                new KeyValuePair<long, string>(3, "Value"),
                new KeyValuePair<long, string>(4, "A"),
                new KeyValuePair<long, string>(400, "E"),
                new KeyValuePair<long, string>(401, "F"),
                new KeyValuePair<long, string>(405, "G"),
            };

            MockBinarySearchable searchable = new MockBinarySearchable(sample);

            List<ProgressReport> gotReports = new List<ProgressReport>();
            ProgressReporter reporter = new ProgressReporter((x) => {
                gotReports.Add(x);
            });

            Assert.IsTrue(searchable.TryFindCeiling(-4, out long index, out long key, reporter));
            Assert.AreEqual(0, index);
            Assert.AreEqual(sample[0].Key, key);
            long complexity = 4;
            CollectionAssert.AreEquivalent(new ProgressReport[] {
                new ProgressReport(1, complexity),
                new ProgressReport(2, complexity),
                new ProgressReport(3, complexity),
                new ProgressReport(4, complexity),
            }, gotReports);
        }

        [TestMethod]
        public void TryFindCeilingReportsProgress_FixedSample2()
        {
            KeyValuePair<long, string>[] sample = new KeyValuePair<long, string>[] {
                new KeyValuePair<long, string>(-3, "0"),
                new KeyValuePair<long, string>(-1, "C"),
                new KeyValuePair<long, string>(0, "B"),
                new KeyValuePair<long, string>(1, "A"),
                new KeyValuePair<long, string>(3, "Value"),
                new KeyValuePair<long, string>(4, "A"),
                new KeyValuePair<long, string>(400, "E"),
                new KeyValuePair<long, string>(401, "F"),
                new KeyValuePair<long, string>(405, "G"),
            };

            MockBinarySearchable searchable = new MockBinarySearchable(sample);

            List<ProgressReport> gotReports = new List<ProgressReport>();
            ProgressReporter reporter = new ProgressReporter((x) => {
                gotReports.Add(x);
            });

            Assert.IsTrue(searchable.TryFindCeiling(2, out long index, out long key, reporter));
            Assert.AreEqual(4, index);
            Assert.AreEqual(sample[4].Key, key);
            long complexity = 4;
            CollectionAssert.AreEquivalent(new ProgressReport[] {
                new ProgressReport(1, complexity),
                new ProgressReport(2, complexity),
                new ProgressReport(3, complexity),
                new ProgressReport(4, complexity),
                new ProgressReport(4, complexity),
            }, gotReports);
        }

        [TestMethod]
        public void TryFindCeilingCanBeCancelled()
        {
            KeyValuePair<long, string>[] sample = new KeyValuePair<long, string>[] {
                new KeyValuePair<long, string>(-3, "0"),
                new KeyValuePair<long, string>(-1, "C"),
                new KeyValuePair<long, string>(0, "B"),
                new KeyValuePair<long, string>(1, "A"),
                new KeyValuePair<long, string>(3, "Value"),
                new KeyValuePair<long, string>(4, "A"),
                new KeyValuePair<long, string>(400, "E"),
                new KeyValuePair<long, string>(401, "F"),
                new KeyValuePair<long, string>(405, "G"),
            };

            MockBinarySearchable searchable = new MockBinarySearchable(sample);
            CancellationTokenSource cancellation = new CancellationTokenSource();
            List<ProgressReport> gotReports = new List<ProgressReport>();
            ProgressReporter reporter = new ProgressReporter((x) => {
                gotReports.Add(x);
                if (gotReports.Count == 2)
                    cancellation.Cancel();
                if (gotReports.Count > 2)
                    Assert.Fail("Expected all progress to stop after the cancellation request.");
            });

            Assert.IsFalse(searchable.TryFindCeiling(2, out var index, out var key, reporter, cancellation.Token));
            Assert.AreEqual(-1, index);//Search was cancelled, index not found
            Assert.AreEqual(default(long), key);//Search was cancelled, key not found
            long complexity = 4;
            CollectionAssert.AreEquivalent(new ProgressReport[] {
                new ProgressReport(1, complexity),
                new ProgressReport(2, complexity),
                //Expect cancellation here, so no more progress reports
            }, gotReports);
        }


































        [TestMethod]
        public void TryFindFloorThrowsWhenArgIsNull()
        {
            Assert.ThrowsException<ArgumentNullException>(() => {
                BinarySearch.TryFindFloor<long, string>(null, 0, out _, out _);
            });
        }

        [DataTestMethod]
        [DataRow(0)]
        [DataRow(1)]
        [DataRow(2)]
        [DataRow(3)]
        [DataRow(4)]
        [DataRow(5)]
        [DataRow(6)]
        [DataRow(7)]
        [DataRow(8)]
        [DataRow(9)]
        [DataRow(15)]
        [DataRow(16)]
        [DataRow(17)]
        [DataRow(1024)]
        public void TryFindFloorFailsWhenNotFound(long count)
        {
            MockBinarySearchable searchable = new MockBinarySearchable(count, 5);

            //Find a 'bad' key (one that is lower than the lowest)
            long badKey = searchable.Count > 0 ? (searchable.SortedList.Keys.Min() - 1) : -1;

            Assert.IsFalse(BinarySearch.TryFindFloor(searchable, badKey, out long index, out var key));
            Assert.AreEqual(-1, index);
            Assert.AreEqual(default(long), key);
        }

        [DataTestMethod]
        [DataRow(1)]
        [DataRow(2)]
        [DataRow(3)]
        [DataRow(4)]
        [DataRow(5)]
        [DataRow(6)]
        [DataRow(7)]
        [DataRow(8)]
        [DataRow(9)]
        [DataRow(15)]
        [DataRow(16)]
        [DataRow(17)]
        [DataRow(1022)]
        [DataRow(1023)]
        [DataRow(1024)]
        [DataRow(1025)]
        public void TryFindFloorWorks(long count)
        {
            KeyValuePair<long, string>[] sample = new KeyValuePair<long, string>[count];
            for (long i = 0; i < count; i++)
                sample[i] = new KeyValuePair<long, string>((i - 5) * 100, "Value_" + i);

            MockBinarySearchable searchable = new MockBinarySearchable(sample);
            
            //There is no 'lower' key than the first
            Assert.IsFalse(BinarySearch.TryFindFloor<long, string>(searchable, sample[0].Key - 1, out long index_, out long gotKey_));
            Assert.AreEqual(-1, index_);
            Assert.AreEqual(default(long), gotKey_);

            for (long i = 0; i < count; i++)
            {
                //Find an 'equal' key
                Assert.IsTrue(BinarySearch.TryFindFloor<long, string>(searchable, sample[i].Key, out long index, out long gotKey));
                Assert.AreEqual(i, index);
                Assert.AreEqual(sample[i].Key, gotKey);

                if (i > 0)
                {
                    //Find the first 'lower' key
                    Assert.IsTrue(BinarySearch.TryFindFloor<long, string>(searchable, sample[i].Key - 1, out index, out gotKey));
                    Assert.AreEqual(i - 1, index);
                    Assert.AreEqual(sample[i - 1].Key, gotKey);
                }
            }

            //Find a key lower than a key that is greater than the highest (basically: Find the last key)
            Assert.IsTrue(BinarySearch.TryFindFloor<long, string>(searchable, sample[count - 1].Key + 1, out index_, out gotKey_));
            Assert.AreEqual(count - 1, index_);
            Assert.AreEqual(sample[count - 1].Key, gotKey_);
        }

        [TestMethod]
        public void TryFindFloorReportsProgress_FixedSample1()
        {
            KeyValuePair<long, string>[] sample = new KeyValuePair<long, string>[] {
                new KeyValuePair<long, string>(-3, "0"),
                new KeyValuePair<long, string>(-1, "C"),
                new KeyValuePair<long, string>(0, "B"),
                new KeyValuePair<long, string>(1, "A"),
                new KeyValuePair<long, string>(3, "Value"),
                new KeyValuePair<long, string>(4, "A"),
                new KeyValuePair<long, string>(400, "E"),
                new KeyValuePair<long, string>(401, "F"),
                new KeyValuePair<long, string>(405, "G"),
            };

            MockBinarySearchable searchable = new MockBinarySearchable(sample);

            List<ProgressReport> gotReports = new List<ProgressReport>();
            ProgressReporter reporter = new ProgressReporter((x) => {
                gotReports.Add(x);
            });

            Assert.IsTrue(searchable.TryFindFloor(-2, out long index, out long key, reporter));
            Assert.AreEqual(0, index);
            Assert.AreEqual(sample[0].Key, key);
            long complexity = 4;
            CollectionAssert.AreEquivalent(new ProgressReport[] {
                new ProgressReport(1, complexity),
                new ProgressReport(2, complexity),
                new ProgressReport(3, complexity),
                new ProgressReport(4, complexity),
            }, gotReports);
        }

        [TestMethod]
        public void TryFindFloorReportsProgress_FixedSample2()
        {
            KeyValuePair<long, string>[] sample = new KeyValuePair<long, string>[] {
                new KeyValuePair<long, string>(-3, "0"),
                new KeyValuePair<long, string>(-1, "C"),
                new KeyValuePair<long, string>(0, "B"),
                new KeyValuePair<long, string>(1, "A"),
                new KeyValuePair<long, string>(3, "Value"),
                new KeyValuePair<long, string>(5, "A"),
                new KeyValuePair<long, string>(400, "E"),
                new KeyValuePair<long, string>(401, "F"),
                new KeyValuePair<long, string>(405, "G"),
            };

            MockBinarySearchable searchable = new MockBinarySearchable(sample);

            List<ProgressReport> gotReports = new List<ProgressReport>();
            ProgressReporter reporter = new ProgressReporter((x) => {
                gotReports.Add(x);
            });

            Assert.IsTrue(searchable.TryFindFloor(4, out long index, out long key, reporter));
            Assert.AreEqual(4, index);
            Assert.AreEqual(sample[4].Key, key);
            long complexity = 4;
            CollectionAssert.AreEquivalent(new ProgressReport[] {
                new ProgressReport(1, complexity),
                new ProgressReport(2, complexity),
                new ProgressReport(3, complexity),
                new ProgressReport(4, complexity),
            }, gotReports);
        }

        [TestMethod]
        public void TryFindFloorCanBeCancelled()
        {
            KeyValuePair<long, string>[] sample = new KeyValuePair<long, string>[] {
                new KeyValuePair<long, string>(-3, "0"),
                new KeyValuePair<long, string>(-1, "C"),
                new KeyValuePair<long, string>(0, "B"),
                new KeyValuePair<long, string>(1, "A"),
                new KeyValuePair<long, string>(3, "Value"),
                new KeyValuePair<long, string>(4, "A"),
                new KeyValuePair<long, string>(400, "E"),
                new KeyValuePair<long, string>(401, "F"),
                new KeyValuePair<long, string>(405, "G"),
            };

            MockBinarySearchable searchable = new MockBinarySearchable(sample);
            CancellationTokenSource cancellation = new CancellationTokenSource();
            List<ProgressReport> gotReports = new List<ProgressReport>();
            ProgressReporter reporter = new ProgressReporter((x) => {
                gotReports.Add(x);
                if (gotReports.Count == 2)
                    cancellation.Cancel();
                if (gotReports.Count > 2)
                    Assert.Fail("Expected all progress to stop after the cancellation request.");
            });

            Assert.IsFalse(searchable.TryFindFloor(2, out var index, out var key, reporter, cancellation.Token));
            Assert.AreEqual(-1, index);//Search was cancelled, index not found
            Assert.AreEqual(default(long), key);//Search was cancelled, key not found
            long complexity = 4;
            CollectionAssert.AreEquivalent(new ProgressReport[] {
                new ProgressReport(1, complexity),
                new ProgressReport(2, complexity),
                //Expect cancellation here, so no more progress reports
            }, gotReports);
        }
    }
}
