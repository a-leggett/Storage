using Microsoft.VisualStudio.TestTools.UnitTesting;
using Storage;
using Storage.Data;
using StorageTest.Mocks;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;

namespace StorageTest
{
    [TestClass]
    public class CachedPageStorageTest
    {
        [TestMethod]
        public void ConstructorThrowsWhenBaseStorageIsNull()
        {
            Assert.ThrowsException<ArgumentNullException>(()=> {
                CachedPageStorage storage = new CachedPageStorage(null, CachedPageStorage.CacheWriteMode.WriteBack, 1, false);
            });
        }

        [TestMethod]
        public void ConstructorThrowsWhenBaseStoragePageSizeIsTooLarge()
        {
            using (MemoryStream ms = new MemoryStream())
            {
                using (var baseStorage = StreamingPageStorage.Create(ms, (long)int.MaxValue + 1, 0/*No way we want to allocate that many bytes for a test*/, null, new CancellationToken(false), true))
                {
                    Assert.ThrowsException<ArgumentException>(() =>
                    {
                        CachedPageStorage cached = new CachedPageStorage(baseStorage, CachedPageStorage.CacheWriteMode.ReadOnly, 1, false);
                    });
                }
            }
        }

        [DataTestMethod]
        [DataRow(CachedPageStorage.CacheWriteMode.WriteBack)]
        [DataRow(CachedPageStorage.CacheWriteMode.WriteThrough)]
        public void ConstructorThrowsWhenBaseStorageIsReadOnlyButWriteModeIsNot(CachedPageStorage.CacheWriteMode writeMode)
        {
            using (MemoryStream ms = new MemoryStream())
            {
                StreamingPageStorage.Create(ms, 1024, 1, null, new CancellationToken(false), true).Dispose();
                using (var baseStorage = StreamingPageStorage.Load(ms, true, true, true))
                {
                    Assert.ThrowsException<ArgumentException>(() =>
                    {
                        CachedPageStorage cached = new CachedPageStorage(baseStorage, writeMode, 1, false);
                    });
                }
            }
        }

        [DataTestMethod]
        [DataRow(-1)]
        [DataRow(-2)]
        [DataRow(-3)]
        public void ConstructorThrowsWhenCapacityIsOutOfRange(int capacity)
        {
            using (MemoryStream ms = new MemoryStream())
            {
                using (var baseStorage = StreamingPageStorage.Create(ms, 1024, 1, null, new CancellationToken(false), true))
                {
                    Assert.ThrowsException<ArgumentOutOfRangeException>(() =>
                    {
                        CachedPageStorage cached = new CachedPageStorage(baseStorage, CachedPageStorage.CacheWriteMode.WriteThrough, capacity, false);
                    });
                }
            }
        }

        [DataTestMethod]
        [DataRow(true, true, CachedPageStorage.CacheWriteMode.ReadOnly, 0, true)]
        [DataRow(true, true, CachedPageStorage.CacheWriteMode.ReadOnly, 1, true)]
        [DataRow(true, true, CachedPageStorage.CacheWriteMode.ReadOnly, 2, true)]
        [DataRow(true, true, CachedPageStorage.CacheWriteMode.ReadOnly, 16, true)]
        [DataRow(false, true, CachedPageStorage.CacheWriteMode.ReadOnly, 0, true)]
        [DataRow(false, true, CachedPageStorage.CacheWriteMode.ReadOnly, 16, true)]
        [DataRow(false, true, CachedPageStorage.CacheWriteMode.WriteBack, 0, true)]
        [DataRow(false, true, CachedPageStorage.CacheWriteMode.WriteBack, 16, true)]
        [DataRow(false, true, CachedPageStorage.CacheWriteMode.WriteThrough, 0, true)]
        [DataRow(false, true, CachedPageStorage.CacheWriteMode.WriteThrough, 16, true)]
        [DataRow(false, false, CachedPageStorage.CacheWriteMode.ReadOnly, 0, true)]
        [DataRow(false, false, CachedPageStorage.CacheWriteMode.ReadOnly, 16, true)]
        [DataRow(false, false, CachedPageStorage.CacheWriteMode.WriteBack, 0, true)]
        [DataRow(false, false, CachedPageStorage.CacheWriteMode.WriteBack, 16, true)]
        [DataRow(false, false, CachedPageStorage.CacheWriteMode.WriteThrough, 0, true)]
        [DataRow(false, false, CachedPageStorage.CacheWriteMode.WriteThrough, 16, true)]

        [DataRow(true, true, CachedPageStorage.CacheWriteMode.ReadOnly, 0, false)]
        [DataRow(true, true, CachedPageStorage.CacheWriteMode.ReadOnly, 1, false)]
        [DataRow(true, true, CachedPageStorage.CacheWriteMode.ReadOnly, 2, false)]
        [DataRow(true, true, CachedPageStorage.CacheWriteMode.ReadOnly, 16, false)]
        [DataRow(false, true, CachedPageStorage.CacheWriteMode.ReadOnly, 0, false)]
        [DataRow(false, true, CachedPageStorage.CacheWriteMode.ReadOnly, 16, false)]
        [DataRow(false, true, CachedPageStorage.CacheWriteMode.WriteBack, 0, false)]
        [DataRow(false, true, CachedPageStorage.CacheWriteMode.WriteBack, 16, false)]
        [DataRow(false, true, CachedPageStorage.CacheWriteMode.WriteThrough, 0, false)]
        [DataRow(false, true, CachedPageStorage.CacheWriteMode.WriteThrough, 16, false)]
        [DataRow(false, false, CachedPageStorage.CacheWriteMode.ReadOnly, 0, false)]
        [DataRow(false, false, CachedPageStorage.CacheWriteMode.ReadOnly, 16, false)]
        [DataRow(false, false, CachedPageStorage.CacheWriteMode.WriteBack, 0, false)]
        [DataRow(false, false, CachedPageStorage.CacheWriteMode.WriteBack, 16, false)]
        [DataRow(false, false, CachedPageStorage.CacheWriteMode.WriteThrough, 0, false)]
        [DataRow(false, false, CachedPageStorage.CacheWriteMode.WriteThrough, 16, false)]
        public void ConstructorWorks(bool isBaseStorageReadOnly, bool isBaseStorageFixedCapacity, CachedPageStorage.CacheWriteMode writeMode, int capacity, bool leaveBasePageStorageOpen)
        {
            using (MemoryStream ms = new MemoryStream())
            {
                StreamingPageStorage.Create(ms, 1024, 1, null, new CancellationToken(false), true).Dispose();
                using (var baseStorage = StreamingPageStorage.Load(ms, isBaseStorageReadOnly, isBaseStorageFixedCapacity, true))
                {
                    using (CachedPageStorage cached = new CachedPageStorage(baseStorage, writeMode, capacity, leaveBasePageStorageOpen))
                    {
                        Assert.AreEqual(leaveBasePageStorageOpen, cached.WillLeaveBasePageStorageOpen);
                        Assert.AreSame(baseStorage, cached.PageStorage);
                        Assert.AreEqual(capacity, cached.CachedPageCapacity);
                        Assert.AreEqual(0, cached.CachedPageIndices.Count());
                        Assert.AreEqual(writeMode, cached.Mode);
                        Assert.AreEqual(writeMode == CachedPageStorage.CacheWriteMode.ReadOnly, cached.IsReadOnly);
                        Assert.AreEqual(isBaseStorageFixedCapacity || writeMode == CachedPageStorage.CacheWriteMode.ReadOnly, cached.IsCapacityFixed);
                    }
                }
            }
        }

        [DataTestMethod]
        [DataRow(true, true, CachedPageStorage.CacheWriteMode.ReadOnly, 0)]
        [DataRow(true, true, CachedPageStorage.CacheWriteMode.ReadOnly, 1)]
        [DataRow(true, true, CachedPageStorage.CacheWriteMode.ReadOnly, 2)]
        [DataRow(true, true, CachedPageStorage.CacheWriteMode.ReadOnly, 16)]
        [DataRow(false, true, CachedPageStorage.CacheWriteMode.ReadOnly, 0)]
        [DataRow(false, true, CachedPageStorage.CacheWriteMode.ReadOnly, 16)]
        [DataRow(false, true, CachedPageStorage.CacheWriteMode.WriteBack, 0)]
        [DataRow(false, true, CachedPageStorage.CacheWriteMode.WriteBack, 16)]
        [DataRow(false, true, CachedPageStorage.CacheWriteMode.WriteThrough, 0)]
        [DataRow(false, true, CachedPageStorage.CacheWriteMode.WriteThrough, 16)]
        [DataRow(false, false, CachedPageStorage.CacheWriteMode.ReadOnly, 0)]
        [DataRow(false, false, CachedPageStorage.CacheWriteMode.ReadOnly, 16)]
        [DataRow(false, false, CachedPageStorage.CacheWriteMode.WriteBack, 0)]
        [DataRow(false, false, CachedPageStorage.CacheWriteMode.WriteBack, 16)]
        [DataRow(false, false, CachedPageStorage.CacheWriteMode.WriteThrough, 0)]
        [DataRow(false, false, CachedPageStorage.CacheWriteMode.WriteThrough, 16)]
        public void ConstructorLoadsExistingDataFromBase(bool isBaseStorageReadOnly, bool isBaseStorageFixedCapacity, CachedPageStorage.CacheWriteMode writeMode, int capacity)
        {
            using (MemoryStream ms = new MemoryStream())
            {
                const long expectedPageCapacity = 5;
                const long pageSize = 1024;
                const long expectedEPI = 7;

                using (var init = StreamingPageStorage.Create(ms, pageSize, expectedPageCapacity, null, new CancellationToken(false), true))
                {
                    init.EntryPageIndex = expectedEPI;

                    //Allocate two pages
                    Assert.IsTrue(init.TryAllocatePage(out _));
                    Assert.IsTrue(init.TryAllocatePage(out _));
                }

                using (var baseStorage = StreamingPageStorage.Load(ms, isBaseStorageReadOnly, isBaseStorageFixedCapacity, true))
                {
                    using (CachedPageStorage cached = new CachedPageStorage(baseStorage, writeMode, capacity, true))
                    {
                        Assert.AreEqual(expectedEPI, cached.EntryPageIndex);
                        Assert.AreEqual(pageSize, cached.PageSize);
                        Assert.AreEqual(expectedPageCapacity, cached.PageCapacity);
                        Assert.AreEqual(2, cached.AllocatedPageCount);
                    }
                }
            }
        }

        [TestMethod]
        public void EntryPageIndexThrowsWhenAssigningWhileReadOnly()
        {
            using (MemoryStream ms = new MemoryStream())
            {
                using (var baseStorage = StreamingPageStorage.Create(ms, 1024, 1, null, new CancellationToken(false), true))
                {
                    using (CachedPageStorage cached = new CachedPageStorage(baseStorage, CachedPageStorage.CacheWriteMode.ReadOnly, 3, true))
                    {
                        Assert.ThrowsException<InvalidOperationException>(() =>
                        {
                            cached.EntryPageIndex = 3;
                        });

                        Assert.ThrowsException<InvalidOperationException>(() =>
                        {
                            cached.EntryPageIndex = null;
                        });
                    }
                }
            }
        }

        [DataTestMethod]
        [DataRow(CachedPageStorage.CacheWriteMode.WriteBack, -1)]
        [DataRow(CachedPageStorage.CacheWriteMode.WriteThrough, -1)]
        [DataRow(CachedPageStorage.CacheWriteMode.WriteBack, -2)]
        [DataRow(CachedPageStorage.CacheWriteMode.WriteThrough, -2)]
        [DataRow(CachedPageStorage.CacheWriteMode.WriteBack, -3)]
        [DataRow(CachedPageStorage.CacheWriteMode.WriteThrough, -3)]
        public void EntryPageIndexThrowsWhenAssigningNegative(CachedPageStorage.CacheWriteMode writeMode, long badIndex)
        {
            using (MemoryStream ms = new MemoryStream())
            {
                const long expectedPageCapacity = 5;
                const long pageSize = 1024;

                using (var baseStorage = StreamingPageStorage.Create(ms, pageSize, expectedPageCapacity, null, new CancellationToken(false), true))
                {
                    using (CachedPageStorage cached = new CachedPageStorage(baseStorage, writeMode, 1, true))
                    {
                        //Make sure init value is null
                        Assert.IsNull(cached.EntryPageIndex);

                        //Attempt to assign an invalid (negative) index
                        Assert.ThrowsException<ArgumentOutOfRangeException>(()=> {
                            cached.EntryPageIndex = badIndex;
                        });

                        //Make sure the cached and base value remains unchanged
                        Assert.IsNull(cached.EntryPageIndex);
                        Assert.IsNull(baseStorage.EntryPageIndex);
                        
                        //Assign a non-null value
                        cached.EntryPageIndex = 5;
                        Assert.AreEqual(5, cached.EntryPageIndex);

                        //Attempt to assign an invalid (negative) index
                        Assert.ThrowsException<ArgumentOutOfRangeException>(() => {
                            cached.EntryPageIndex = badIndex;
                        });

                        //Make sure the cached and base value remains unchanged
                        Assert.AreEqual(5, cached.EntryPageIndex);
                        Assert.AreEqual(5, baseStorage.EntryPageIndex);
                    }
                }
            }
        }

        [DataTestMethod]
        [DataRow(CachedPageStorage.CacheWriteMode.WriteBack)]
        [DataRow(CachedPageStorage.CacheWriteMode.WriteThrough)]
        public void EntryPageIndexWorks(CachedPageStorage.CacheWriteMode writeMode)
        {
            using (MemoryStream ms = new MemoryStream())
            {
                const long pageCapacity = 5;
                const long pageSize = 1024;

                using (var baseStorage = StreamingPageStorage.Create(ms, pageSize, pageCapacity, null, new CancellationToken(false), true))
                {
                    using (CachedPageStorage cached = new CachedPageStorage(baseStorage, writeMode, 1, true))
                    {
                        //Make sure init value is null
                        Assert.IsNull(cached.EntryPageIndex);

                        //Assign a non-null value
                        cached.EntryPageIndex = 5;
                        Assert.AreEqual(5, cached.EntryPageIndex);

                        //Make sure the assignment is write-through regardless of 'writeMode'
                        Assert.AreEqual(5, baseStorage.EntryPageIndex);

                        //Assign null
                        cached.EntryPageIndex = null;
                        Assert.IsNull(cached.EntryPageIndex);
                        Assert.IsNull(baseStorage.EntryPageIndex);

                        //Assign non-null
                        cached.EntryPageIndex = 134;
                        Assert.AreEqual(134, cached.EntryPageIndex);
                        Assert.AreEqual(134, baseStorage.EntryPageIndex);

                        //Assign zero
                        cached.EntryPageIndex = 0;
                        Assert.AreEqual(0, cached.EntryPageIndex);
                        Assert.AreEqual(0, baseStorage.EntryPageIndex);
                    }
                }
            }
        }

        [TestMethod]
        public void IsPageAllocatedWorks()
        {
            using (MemoryStream ms = new MemoryStream())
            {
                const long pageCapacity = 5;
                const long pageSize = 1024;

                using (var baseStorage = StreamingPageStorage.Create(ms, pageSize, pageCapacity, null, new CancellationToken(false), true))
                {
                    //Allocate one page
                    Assert.IsTrue(baseStorage.TryAllocatePage(out long index));

                    using (CachedPageStorage cached = new CachedPageStorage(baseStorage, CachedPageStorage.CacheWriteMode.WriteThrough, 1, true))
                    {
                        //Make sure the IsPageAllocated method works
                        for(long i = 0; i < pageCapacity; i++)
                        {
                            if (i == index)
                                Assert.IsTrue(cached.IsPageAllocated(i));
                            else
                                Assert.IsFalse(cached.IsPageAllocated(i));
                        }

                        //Read from the allocated page (since this may affect cache storage)
                        byte[] buffer = new byte[1];
                        cached.ReadFrom(index, 0, buffer, 0, buffer.Length);

                        //Write to the allocated page
                        cached.WriteTo(index, 0, buffer, 0, buffer.Length);

                        //Allocate another page
                        Assert.IsTrue(cached.TryAllocatePage(out long index2));

                        //Make sure the IsPageAllocated method works after possible disruption
                        for (long i = 0; i < pageCapacity; i++)
                        {
                            if (i == index || i == index2)
                                Assert.IsTrue(cached.IsPageAllocated(i));
                            else
                                Assert.IsFalse(cached.IsPageAllocated(i));
                        }
                    }
                }
            }
        }

        [TestMethod]
        public void IsPageAllocatedReturnsFalseWhenNotOnStorage()
        {
            using (MemoryStream ms = new MemoryStream())
            {
                const long pageCapacity = 5;
                const long pageSize = 1024;

                using (var baseStorage = StreamingPageStorage.Create(ms, pageSize, pageCapacity, null, new CancellationToken(false), true))
                {
                    //Allocate one page
                    Assert.IsTrue(baseStorage.TryAllocatePage(out long index));

                    using (CachedPageStorage cached = new CachedPageStorage(baseStorage, CachedPageStorage.CacheWriteMode.WriteThrough, 1, true))
                    {
                        Assert.IsFalse(cached.IsPageAllocated(-1));
                        Assert.IsFalse(cached.IsPageAllocated(-2));
                        Assert.IsFalse(cached.IsPageAllocated(pageCapacity));
                        Assert.IsFalse(cached.IsPageAllocated(pageCapacity + 1));
                    }
                }
            }
        }

        [TestMethod]
        public void IsPageAllocatedUsesCache()
        {
            using (MemoryStream ms = new MemoryStream())
            {
                const long pageCapacity = 5;
                const long pageSize = 1024;

                using (var baseStorage = new MockPageStorage(ms, pageSize, pageCapacity, false, false))
                {
                    //Allocate one page
                    Assert.IsTrue(baseStorage.TryAllocatePage(out long index));

                    using (CachedPageStorage cached = new CachedPageStorage(baseStorage, CachedPageStorage.CacheWriteMode.WriteThrough, 1, true))
                    {
                        //Read from the page (to cause the page to be cached)
                        byte[] buffer = new byte[1];
                        cached.ReadFrom(index, 0, buffer, 0, buffer.Length);

                        long baseCallCount = 0;
                        //Count the number of times the base IPageStorage has to check IsPageAllocated
                        baseStorage.OnIsPageAllocated += (strg, indx) =>
                        {
                            baseCallCount++;
                        };

                        Assert.IsTrue(cached.IsPageAllocated(index));

                        //Make sure the base did not have to check (since the page is cached)
                        Assert.AreEqual(0, baseCallCount);

                        //Now evict the cached page
                        cached.EvictPageFromCache(index);

                        Assert.IsTrue(cached.IsPageAllocated(index));

                        //Make sure the base did check since the page was evicted from cache
                        Assert.AreEqual(1, baseCallCount);
                    }
                }
            }
        }

        [TestMethod]
        public void IsPageOnStorageWorks()
        {
            using (MemoryStream ms = new MemoryStream())
            {
                const long pageCapacity = 5;
                const long pageSize = 1024;

                using (var baseStorage = new MockPageStorage(ms, pageSize, pageCapacity, false, false))
                {
                    //Allocate one page
                    Assert.IsTrue(baseStorage.TryAllocatePage(out long index));

                    using (CachedPageStorage cached = new CachedPageStorage(baseStorage, CachedPageStorage.CacheWriteMode.WriteThrough, 1, true))
                    {
                        Assert.IsFalse(cached.IsPageOnStorage(-1));
                        Assert.IsFalse(cached.IsPageOnStorage(pageCapacity));
                        Assert.IsFalse(cached.IsPageOnStorage(pageCapacity + 1));

                        for (long i = 0; i < pageCapacity; i++)
                            Assert.IsTrue(cached.IsPageOnStorage(index));
                    }
                }
            }
        }

        [DataTestMethod]
        [DataRow(-1)]
        [DataRow(-2)]
        public void TryInflateThrowsWhenAdditionalPageCountIsOutOfRange(long badAmount)
        {
            using (MemoryStream ms = new MemoryStream())
            {
                const long pageCapacity = 5;
                const long pageSize = 1024;

                using (var baseStorage = new MockPageStorage(ms, pageSize, pageCapacity, false, false))
                {
                    baseStorage.OnTryInflate += (stg, count) => {
                        Assert.Fail("The base method must not be called.");
                    };

                    using (CachedPageStorage cached = new CachedPageStorage(baseStorage, CachedPageStorage.CacheWriteMode.WriteThrough, 1, true))
                    {
                        Assert.ThrowsException<ArgumentOutOfRangeException>(() => {
                            cached.TryInflate(badAmount, null, new CancellationToken(false));
                        });

                        Assert.AreEqual(pageCapacity, cached.PageCapacity);
                    }
                }
            }
        }

        [TestMethod]
        public void TryInflateThrowsWhenFixedCapacity()
        {
            using (MemoryStream ms = new MemoryStream())
            {
                const long pageCapacity = 5;
                const long pageSize = 1024;

                using (var baseStorage = new MockPageStorage(ms, pageSize, pageCapacity, false, true))
                {
                    baseStorage.OnTryInflate += (stg, count) => {
                        Assert.Fail("The base method must not be called.");
                    };

                    using (CachedPageStorage cached = new CachedPageStorage(baseStorage, CachedPageStorage.CacheWriteMode.WriteThrough, 1, true))
                    {
                        Assert.ThrowsException<InvalidOperationException>(() => {
                            cached.TryInflate(5, null, new CancellationToken(false));
                        });

                        Assert.AreEqual(pageCapacity, cached.PageCapacity);
                    }
                }
            }
        }

        [DataTestMethod]
        [DataRow(0, 0)]
        [DataRow(1, 0)]
        [DataRow(2, 0)]
        [DataRow(0, 1)]
        [DataRow(0, 2)]
        [DataRow(0, 3)]
        [DataRow(0, 1024)]
        [DataRow(2, 0)]
        [DataRow(2, 1)]
        [DataRow(2, 2)]
        [DataRow(2, 3)]
        [DataRow(0, 1024)]
        [DataRow(1, 1024)]
        [DataRow(1024, 0)]
        [DataRow(1024, 1)]
        [DataRow(1024, 2048)]
        public void TryInflateWorks(long initCapacity, long additionalAmount)
        {
            using (MemoryStream ms = new MemoryStream())
            {
                const long pageSize = 1024;

                using (var baseStorage = new MockPageStorage(ms, pageSize, initCapacity, false, false))
                {
                    long baseCallCount = 0;
                    baseStorage.OnTryInflate += (stg, count) => {
                        Assert.AreEqual(additionalAmount, count);
                        baseCallCount++;
                    };

                    using (CachedPageStorage cached = new CachedPageStorage(baseStorage, CachedPageStorage.CacheWriteMode.WriteThrough, 1, true))
                    {
                        Assert.AreEqual(initCapacity, cached.PageCapacity);

                        Assert.AreEqual(additionalAmount, cached.TryInflate(additionalAmount, null, new CancellationToken(false)));

                        Assert.AreEqual(initCapacity + additionalAmount, cached.PageCapacity);
                    }

                    Assert.AreEqual(1, baseCallCount);
                }
            }
        }

        [DataTestMethod]
        [DataRow(-1)]
        [DataRow(-2)]
        public void TryDeflateThrowsWhenAdditionalPageCountIsOutOfRange(long badAmount)
        {
            using (MemoryStream ms = new MemoryStream())
            {
                const long pageCapacity = 5;
                const long pageSize = 1024;

                using (var baseStorage = new MockPageStorage(ms, pageSize, pageCapacity, false, false))
                {
                    baseStorage.OnTryDeflate += (stg, count) => {
                        Assert.Fail("The base method must not be called.");
                    };

                    using (CachedPageStorage cached = new CachedPageStorage(baseStorage, CachedPageStorage.CacheWriteMode.WriteThrough, 1, true))
                    {
                        Assert.ThrowsException<ArgumentOutOfRangeException>(() => {
                            cached.TryDeflate(badAmount, null, new CancellationToken(false));
                        });

                        Assert.AreEqual(pageCapacity, cached.PageCapacity);
                    }
                }
            }
        }

        [TestMethod]
        public void TryDeflateThrowsWhenFixedCapacity()
        {
            using (MemoryStream ms = new MemoryStream())
            {
                const long pageCapacity = 5;
                const long pageSize = 1024;

                using (var baseStorage = new MockPageStorage(ms, pageSize, pageCapacity, false, true))
                {
                    baseStorage.OnTryDeflate += (stg, count) => {
                        Assert.Fail("The base method must not be called.");
                    };

                    using (CachedPageStorage cached = new CachedPageStorage(baseStorage, CachedPageStorage.CacheWriteMode.WriteThrough, 1, true))
                    {
                        Assert.ThrowsException<InvalidOperationException>(() => {
                            cached.TryDeflate(5, null, new CancellationToken(false));
                        });

                        Assert.AreEqual(pageCapacity, cached.PageCapacity);
                    }
                }
            }
        }

        [DataTestMethod]
        [DataRow(0, 0, 0, 0)]
        [DataRow(1, 0, 0, 1)]
        [DataRow(2, 0, 0, 2)]
        [DataRow(0, 1, 0, 0)]
        [DataRow(0, 2, 0, 0)]
        [DataRow(0, 3, 0, 0)]
        [DataRow(0, 1024, 0, 0)]
        [DataRow(2, 0, 0, 2)]
        [DataRow(2, 1, 1, 1)]
        [DataRow(2, 2, 2, 0)]
        [DataRow(2, 3, 2, 0)]
        [DataRow(2, 4, 2, 0)]
        [DataRow(0, 1024, 0, 0)]
        [DataRow(1, 1024, 1, 0)]
        [DataRow(1024, 0, 0, 1024)]
        [DataRow(1024, 1, 1, 1023)]
        [DataRow(1024, 2048, 1024, 0)]
        public void TryDeflateWorks(long initCapacity, long removeAmount, long expectedActualRemoveAmount, long expectedResultingCapacity)
        {
            using (MemoryStream ms = new MemoryStream())
            {
                const long pageSize = 1024;

                using (var baseStorage = new MockPageStorage(ms, pageSize, initCapacity, false, false))
                {
                    long baseCallCount = 0;
                    baseStorage.OnTryDeflate += (stg, count) => {
                        Assert.AreEqual(removeAmount, count);
                        baseCallCount++;
                    };

                    using (CachedPageStorage cached = new CachedPageStorage(baseStorage, CachedPageStorage.CacheWriteMode.WriteThrough, 1, true))
                    {
                        Assert.AreEqual(initCapacity, cached.PageCapacity);

                        Assert.AreEqual(expectedActualRemoveAmount, cached.TryDeflate(removeAmount, null, new CancellationToken(false)));

                        Assert.AreEqual(expectedResultingCapacity, cached.PageCapacity);
                    }

                    Assert.AreEqual(1, baseCallCount);
                }
            }
        }

        [TestMethod]
        public void TryAllocatePageThrowsWhenReadOnly()
        {
            using (MemoryStream ms = new MemoryStream())
            {
                const long pageCapacity = 5;
                const long pageSize = 1024;

                using (var baseStorage = new MockPageStorage(ms, pageSize, pageCapacity, false, false))
                {
                    //Allocate a page successfully on the base (writable) storage
                    Assert.IsTrue(baseStorage.TryAllocatePage(out long successfullyAllocatedIndex));
                    Assert.AreEqual(1, baseStorage.AllocatedPageCount);

                    baseStorage.OnTryAllocatePage += (stg) => {
                        Assert.Fail("The base method must not be called.");
                    };

                    using (CachedPageStorage cached = new CachedPageStorage(baseStorage, CachedPageStorage.CacheWriteMode.ReadOnly, 1, true))
                    {
                        Assert.ThrowsException<InvalidOperationException>(() => {
                            cached.TryAllocatePage(out _);
                        });

                        //Make sure no additional pages were allocated
                        Assert.AreEqual(1, cached.AllocatedPageCount);
                        Assert.AreEqual(1, baseStorage.AllocatedPageCount);
                    }
                }
            }
        }

        [DataTestMethod]
        [DataRow(CachedPageStorage.CacheWriteMode.WriteBack)]
        [DataRow(CachedPageStorage.CacheWriteMode.WriteThrough)]
        public void TryAllocatePageWorks(CachedPageStorage.CacheWriteMode writeMode)
        {
            using (MemoryStream ms = new MemoryStream())
            {
                const long pageCapacity = 5;
                const long pageSize = 1024;

                using (var baseStorage = new MockPageStorage(ms, pageSize, pageCapacity, false, false))
                {
                    long baseCallCount = 0;
                    baseStorage.OnTryAllocatePage += (stg) => {
                        baseCallCount++;
                    };

                    using (CachedPageStorage cached = new CachedPageStorage(baseStorage, writeMode, 3, true))
                    {
                        Assert.IsTrue(cached.TryAllocatePage(out long index));
                        Assert.IsTrue(cached.IsPageAllocated(index));
                        Assert.IsTrue(baseStorage.IsPageAllocated(index));
                        Assert.AreEqual(1, baseCallCount);
                        Assert.AreEqual(1, cached.AllocatedPageCount);
                        Assert.AreEqual(1, baseStorage.AllocatedPageCount);
                    }
                }
            }
        }

        [DataTestMethod]
        [DataRow(0, CachedPageStorage.CacheWriteMode.WriteBack)]
        [DataRow(0, CachedPageStorage.CacheWriteMode.WriteThrough)]
        [DataRow(1, CachedPageStorage.CacheWriteMode.WriteBack)]
        [DataRow(1, CachedPageStorage.CacheWriteMode.WriteThrough)]
        [DataRow(2, CachedPageStorage.CacheWriteMode.WriteBack)]
        [DataRow(2, CachedPageStorage.CacheWriteMode.WriteThrough)]
        [DataRow(1024, CachedPageStorage.CacheWriteMode.WriteBack)]
        [DataRow(1024, CachedPageStorage.CacheWriteMode.WriteThrough)]
        public void TryAllocatePageFailsWhenBaseFails(long capacity, CachedPageStorage.CacheWriteMode writeMode)
        {
            using (MemoryStream ms = new MemoryStream())
            {
                const long pageSize = 1024;

                using (var baseStorage = new MockPageStorage(ms, pageSize, capacity, false, false))
                {
                    long baseCallCount = 0;
                    baseStorage.OnTryAllocatePage += (stg) => {
                        baseCallCount++;
                    };

                    using (CachedPageStorage cached = new CachedPageStorage(baseStorage, writeMode, 3, true))
                    {
                        //Allocate to full capacity
                        for(long i = 0; i < capacity; i++)
                        {
                            Assert.IsTrue(cached.TryAllocatePage(out long index));
                            Assert.IsTrue(cached.IsPageAllocated(index));
                            Assert.IsTrue(baseStorage.IsPageAllocated(index));
                            Assert.AreEqual(i + 1, baseCallCount);
                            Assert.AreEqual(i + 1, cached.AllocatedPageCount);
                            Assert.AreEqual(i + 1, baseStorage.AllocatedPageCount);
                        }

                        Assert.AreEqual(capacity, cached.AllocatedPageCount);
                        Assert.AreEqual(capacity, baseStorage.AllocatedPageCount);
                        Assert.AreEqual(capacity, baseCallCount);

                        //Ensure another allocation will fail
                        Assert.IsFalse(cached.TryAllocatePage(out long notAllocated));
                        Assert.AreEqual(-1, notAllocated);
                        Assert.AreEqual(capacity, cached.AllocatedPageCount);
                        Assert.AreEqual(capacity, baseStorage.AllocatedPageCount);
                    }
                }
            }
        }

        [TestMethod]
        public void FreePageThrowsWhenReadOnly()
        {
            using (MemoryStream ms = new MemoryStream())
            {
                const long pageCapacity = 5;
                const long pageSize = 1024;

                using (var baseStorage = new MockPageStorage(ms, pageSize, pageCapacity, false, false))
                {
                    baseStorage.OnFreePage += (stg, indx) => {
                        Assert.Fail("This method must not be called.");
                    };

                    long index;
                    using (CachedPageStorage cached = new CachedPageStorage(baseStorage, CachedPageStorage.CacheWriteMode.WriteBack, 3, true))
                    {
                        Assert.IsTrue(cached.TryAllocatePage(out index));
                        Assert.IsTrue(cached.IsPageAllocated(index));
                        Assert.IsTrue(baseStorage.IsPageAllocated(index));
                        Assert.AreEqual(1, cached.AllocatedPageCount);
                        Assert.AreEqual(1, baseStorage.AllocatedPageCount);
                    }

                    using (CachedPageStorage cached = new CachedPageStorage(baseStorage, CachedPageStorage.CacheWriteMode.ReadOnly, 3, true))
                    {
                        Assert.ThrowsException<InvalidOperationException>(()=> {
                            cached.FreePage(index);
                        });

                        //Ensure the page is still allocated
                        Assert.IsTrue(cached.IsPageAllocated(index));
                        Assert.IsTrue(baseStorage.IsPageAllocated(index));
                        Assert.AreEqual(1, cached.AllocatedPageCount);
                        Assert.AreEqual(1, baseStorage.AllocatedPageCount);
                    }
                }
            }
        }

        [TestMethod]
        public void FreePageThrowsWhenIndexNotOnStorage()
        {
            using (MemoryStream ms = new MemoryStream())
            {
                const long pageCapacity = 5;
                const long pageSize = 1024;

                using (var baseStorage = new MockPageStorage(ms, pageSize, pageCapacity, false, false))
                {
                    baseStorage.OnFreePage += (stg, indx) => {
                        Assert.Fail("This method must not be called.");
                    };
                    using (CachedPageStorage cached = new CachedPageStorage(baseStorage, CachedPageStorage.CacheWriteMode.WriteBack, 3, true))
                    {
                        Assert.IsTrue(cached.TryAllocatePage(out long index));
                        Assert.IsTrue(cached.IsPageAllocated(index));
                        Assert.IsTrue(baseStorage.IsPageAllocated(index));
                        Assert.AreEqual(1, cached.AllocatedPageCount);
                        Assert.AreEqual(1, baseStorage.AllocatedPageCount);

                        Assert.ThrowsException<ArgumentOutOfRangeException>(() =>
                        {
                            cached.FreePage(-1);
                        });
                        Assert.ThrowsException<ArgumentOutOfRangeException>(() =>
                        {
                            cached.FreePage(pageCapacity);
                        });
                        Assert.ThrowsException<ArgumentOutOfRangeException>(() =>
                        {
                            cached.FreePage(pageCapacity + 1);
                        });

                        //Ensure the original page is still allocated
                        Assert.IsTrue(cached.IsPageAllocated(index));
                        Assert.IsTrue(baseStorage.IsPageAllocated(index));
                        Assert.AreEqual(1, cached.AllocatedPageCount);
                        Assert.AreEqual(1, baseStorage.AllocatedPageCount);
                    }
                }
            }
        }

        [TestMethod]
        public void FreePageWorks()
        {
            using (MemoryStream ms = new MemoryStream())
            {
                const long pageCapacity = 5;
                const long pageSize = 1024;

                using (var baseStorage = new MockPageStorage(ms, pageSize, pageCapacity, false, false))
                {
                    long index = -1;

                    long baseCallCount = 0;
                    baseStorage.OnFreePage += (stg, indx) => {
                        Assert.AreEqual(index, indx);
                        baseCallCount++;
                    };

                    using (CachedPageStorage cached = new CachedPageStorage(baseStorage, CachedPageStorage.CacheWriteMode.WriteBack, 3, true))
                    {
                        //Allocate the page
                        Assert.IsTrue(cached.TryAllocatePage(out index));
                        Assert.IsTrue(cached.IsPageAllocated(index));
                        Assert.IsTrue(baseStorage.IsPageAllocated(index));
                        Assert.AreEqual(1, cached.AllocatedPageCount);
                        Assert.AreEqual(1, baseStorage.AllocatedPageCount);

                        //Free it
                        Assert.IsTrue(cached.FreePage(index));
                        
                        //Ensure the page was freed
                        Assert.IsFalse(cached.IsPageAllocated(index));
                        Assert.IsFalse(baseStorage.IsPageAllocated(index));
                        Assert.AreEqual(0, cached.AllocatedPageCount);
                        Assert.AreEqual(0, baseStorage.AllocatedPageCount);
                    }
                }
            }
        }

        [TestMethod]
        public void FreePageCausesCachedWritesToBeSentToBasePageStorage()
        {
            using (MemoryStream ms = new MemoryStream())
            {
                const long pageCapacity = 5;
                const long pageSize = 1024;

                using (var baseStorage = new MockPageStorage(ms, pageSize, pageCapacity, false, false))
                {
                    long index = -1;

                    long baseWriteCallCount = 0;
                    baseStorage.OnWriteTo += (srg, pgIndx, dstOff, buff, srcOff, len) => {
                        Assert.AreEqual(index, pgIndx);
                        baseWriteCallCount++;
                    };

                    long baseFreeCallCount = 0;
                    baseStorage.OnFreePage += (stg, indx) => {
                        Assert.AreEqual(1, baseWriteCallCount);//Make sure data was written before free
                        Assert.AreEqual(index, indx);
                        baseFreeCallCount++;
                    };

                    using (CachedPageStorage cached = new CachedPageStorage(baseStorage, CachedPageStorage.CacheWriteMode.WriteBack, 3, true))
                    {
                        //Allocate the page
                        Assert.IsTrue(cached.TryAllocatePage(out index));
                        Assert.IsTrue(cached.IsPageAllocated(index));
                        Assert.IsTrue(baseStorage.IsPageAllocated(index));
                        Assert.AreEqual(1, cached.AllocatedPageCount);
                        Assert.AreEqual(1, baseStorage.AllocatedPageCount);

                        //Write some data to cached page
                        byte[] buffer = new byte[pageSize];
                        for (long i = 0; i < buffer.Length; i++)
                            buffer[i] = (byte)i;
                        cached.WriteTo(index, 0, buffer, 0, buffer.Length);

                        //Make sure the data was not yet sent to the base IPageStorage
                        Assert.AreEqual(0, baseWriteCallCount);

                        //Free the page
                        Assert.IsTrue(cached.FreePage(index));

                        //Ensure the page was freed
                        Assert.IsFalse(cached.IsPageAllocated(index));
                        Assert.IsFalse(baseStorage.IsPageAllocated(index));
                        Assert.AreEqual(0, cached.AllocatedPageCount);
                        Assert.AreEqual(0, baseStorage.AllocatedPageCount);

                        //Ensure the payload was written before it was freed
                        Assert.AreEqual(1, baseWriteCallCount);
                        Assert.AreEqual(1, baseFreeCallCount);
                    }
                }
            }
        }

        [TestMethod]
        public void FreePageHasNoEffectWhenPageIsAlreadyUnallocated()
        {
            using (MemoryStream ms = new MemoryStream())
            {
                const long pageCapacity = 5;
                const long pageSize = 1024;

                using (var baseStorage = new MockPageStorage(ms, pageSize, pageCapacity, false, false))
                {
                    long index = -1;
                    
                    long baseFreeCallCount = 0;
                    baseStorage.OnFreePage += (stg, indx) => {
                        Assert.AreEqual(index, indx);
                        baseFreeCallCount++;
                    };

                    using (CachedPageStorage cached = new CachedPageStorage(baseStorage, CachedPageStorage.CacheWriteMode.WriteBack, 3, true))
                    {
                        //Allocate the page
                        Assert.IsTrue(cached.TryAllocatePage(out index));
                        Assert.IsTrue(cached.IsPageAllocated(index));
                        Assert.IsTrue(baseStorage.IsPageAllocated(index));
                        Assert.AreEqual(1, cached.AllocatedPageCount);
                        Assert.AreEqual(1, baseStorage.AllocatedPageCount);
                        
                        //Free the page
                        Assert.IsTrue(cached.FreePage(index));

                        //Ensure the page was freed
                        Assert.IsFalse(cached.IsPageAllocated(index));
                        Assert.IsFalse(baseStorage.IsPageAllocated(index));
                        Assert.AreEqual(0, cached.AllocatedPageCount);
                        Assert.AreEqual(0, baseStorage.AllocatedPageCount);
                        Assert.AreEqual(1, baseFreeCallCount);

                        //Free the page again
                        Assert.IsFalse(cached.FreePage(index));
                        Assert.IsFalse(baseStorage.IsPageAllocated(index));
                        Assert.AreEqual(0, cached.AllocatedPageCount);
                        Assert.AreEqual(0, baseStorage.AllocatedPageCount);
                        Assert.AreEqual(2, baseFreeCallCount);
                    }
                }
            }
        }

        [TestMethod]
        public void ReadFromThrowsWhenBufferIsNull()
        {
            using (MemoryStream ms = new MemoryStream())
            {
                const long pageCapacity = 5;
                const long pageSize = 1024;

                using (var baseStorage = new MockPageStorage(ms, pageSize, pageCapacity, false, false))
                {
                    long index = -1;

                    long baseFreeCallCount = 0;
                    baseStorage.OnFreePage += (stg, indx) => {
                        Assert.AreEqual(index, indx);
                        baseFreeCallCount++;
                    };

                    using (CachedPageStorage cached = new CachedPageStorage(baseStorage, CachedPageStorage.CacheWriteMode.WriteBack, 3, true))
                    {
                        //Allocate the page
                        Assert.IsTrue(cached.TryAllocatePage(out index));
                        Assert.IsTrue(cached.IsPageAllocated(index));
                        Assert.IsTrue(baseStorage.IsPageAllocated(index));
                        Assert.AreEqual(1, cached.AllocatedPageCount);
                        Assert.AreEqual(1, baseStorage.AllocatedPageCount);

                        //Try to read using null buffer
                        Assert.ThrowsException<ArgumentNullException>(()=> {
                            cached.ReadFrom(index, 0, null, 0, 1);
                        });
                    }
                }
            }
        }

        [DataTestMethod]
        [DataRow(StreamingPageStorage.MinPageSize, 0, -1, 0, 0)]
        [DataRow(StreamingPageStorage.MinPageSize, 0, 0, -1, 0)]
        [DataRow(StreamingPageStorage.MinPageSize, 0, 0, 0, -1)]
        [DataRow(StreamingPageStorage.MinPageSize, 1, -1, 0, 0)]
        [DataRow(StreamingPageStorage.MinPageSize, 1, 0, -1, 0)]
        [DataRow(StreamingPageStorage.MinPageSize, 1, 0, 0, -1)]
        [DataRow(StreamingPageStorage.MinPageSize, 2, -1, 0, 0)]
        [DataRow(StreamingPageStorage.MinPageSize, 2, 0, -1, 0)]
        [DataRow(StreamingPageStorage.MinPageSize, 2, 0, 0, -1)]
        [DataRow(StreamingPageStorage.MinPageSize, 2, StreamingPageStorage.MinPageSize, 0, 1)]
        [DataRow(StreamingPageStorage.MinPageSize, 2, 0, 2, 1)]
        [DataRow(StreamingPageStorage.MinPageSize, 2, 0, 0, 3)]
        public void ReadFromThrowsWhenArgIsOutOfRange(long pageSize, long bufferSize, long srcOffset, long dstOffset, long length)
        {
            using (MemoryStream ms = new MemoryStream())
            {
                const long pageCapacity = 5;
                
                using (var baseStorage = new MockPageStorage(ms, pageSize, pageCapacity, false, false))
                {
                    long index = -1;

                    using (CachedPageStorage cached = new CachedPageStorage(baseStorage, CachedPageStorage.CacheWriteMode.WriteBack, 3, true))
                    {
                        //Allocate the page
                        Assert.IsTrue(cached.TryAllocatePage(out index));
                        Assert.IsTrue(cached.IsPageAllocated(index));
                        Assert.IsTrue(baseStorage.IsPageAllocated(index));
                        Assert.AreEqual(1, cached.AllocatedPageCount);
                        Assert.AreEqual(1, baseStorage.AllocatedPageCount);

                        byte[] buffer = new byte[bufferSize];

                        //Try to read using bad arguments
                        Assert.ThrowsException<ArgumentOutOfRangeException>(()=> {
                            cached.ReadFrom(index, srcOffset, buffer, dstOffset, length);
                        });
                    }
                }
            }
        }

        [TestMethod]
        public void ReadFromThrowsWhenPageIsNotAllocated()
        {
            using (MemoryStream ms = new MemoryStream())
            {
                const long pageCapacity = 5;
                const long pageSize = 1024;

                using (var baseStorage = new MockPageStorage(ms, pageSize, pageCapacity, false, false))
                {
                    using (CachedPageStorage cached = new CachedPageStorage(baseStorage, CachedPageStorage.CacheWriteMode.WriteBack, 3, true))
                    {
                        Assert.ThrowsException<InvalidOperationException>(() => {
                            cached.ReadFrom(-1, 0, new byte[1], 0, 1);
                        });

                        Assert.ThrowsException<InvalidOperationException>(() => {
                            cached.ReadFrom(pageCapacity, 0, new byte[1], 0, 1);
                        });

                        Assert.ThrowsException<InvalidOperationException>(() => {
                            cached.ReadFrom(pageCapacity + 1, 0, new byte[1], 0, 1);
                        });

                        for(long i = 0; i < pageCapacity; i++)
                        {
                            Assert.ThrowsException<InvalidOperationException>(() => {
                                cached.ReadFrom(pageCapacity, i, new byte[1], 0, 1);
                            });
                        }
                    }
                }
            }
        }

        private void ReadWorks(MockPageStorage mockStorage, CachedPageStorage cached, long pageIndex, DataRegion[] requestRegions, DataRegion[] expectedToReadFromBaseRegions)
        {
            List<DataRegion> requestsSentToBaseStorage = new List<DataRegion>();

            //Monitor the read calls sent to the base storage
            mockStorage.OnRead += (stg, pgIndx, srcOff, buf, bufOff, len) => {
                requestsSentToBaseStorage.Add(new DataRegion(srcOff, (srcOff + len) - 1/*-1 to go from count to index*/));
            };

            //Send the read requests to the cache storage
            foreach(var region in requestRegions)
            {
                const long bufferOffset = 5;
                byte[] buffer = new byte[region.Length + bufferOffset];
                cached.ReadFrom(pageIndex, region.FirstIndex, buffer, bufferOffset, region.Length);

                //Make sure the correct payload was read
                for(int i = 0; i < region.Length; i++)
                {
                    byte expected = (byte)(i + region.FirstIndex);
                    Assert.AreEqual(expected, buffer[i + bufferOffset]);
                }
            }

            //Make sure the base storage only received read requests for the non-cached regions
            CollectionAssert.AreEquivalent(expectedToReadFromBaseRegions.ToArray(), requestsSentToBaseStorage.ToArray());
        }

        [TestMethod]
        public void ReadWorks()
        {
            using (MemoryStream ms = new MemoryStream())
            {
                using (MockSafeResizableStorage mockStorage = new MockSafeResizableStorage(ms, true, true, true, null, 1024))
                {
                    using (MockPageStorage baseStorage = new MockPageStorage(mockStorage, 1024, 3, false, true))
                    {
                        Assert.IsTrue(baseStorage.TryAllocatePage(out long pageA));
                        Assert.IsTrue(baseStorage.TryAllocatePage(out long pageB));
                        Assert.IsTrue(baseStorage.TryAllocatePage(out long pageC));

                        //Initialize payload to each page
                        byte[] page = new byte[1024];
                        for (int i = 0; i < page.Length; i++)
                            page[i] = (byte)i;
                        baseStorage.WriteTo(pageA, 0, page, 0, page.Length);
                        baseStorage.WriteTo(pageB, 0, page, 0, page.Length);
                        baseStorage.WriteTo(pageC, 0, page, 0, page.Length);

                        using (CachedPageStorage cached = new CachedPageStorage(baseStorage, CachedPageStorage.CacheWriteMode.WriteBack, 3, true))
                        {
                            ReadWorks(baseStorage, cached, pageA,
                            new DataRegion[] {
                            new DataRegion(1, 2),
                            new DataRegion(5, 10),
                            new DataRegion(5, 10),//Will be entirely cached
                            new DataRegion(5, 15),//(5, 10) will be cached, (11, 15) will be read
                            new DataRegion(0, 100),//(1,2) and (5, 15) will be cached, (0,0), (3, 4), and (16, 100) will be read
                            new DataRegion(0, 0),//Will be entirely cached
                            new DataRegion(1, 1),//Will be entirely cached
                            new DataRegion(100, 100),//Will be entirely cached
                            new DataRegion(101, 101),
                            new DataRegion(103, 103),
                            new DataRegion(102, 102),
                            new DataRegion(105, 105),
                            new DataRegion(0, 105),//(0, 103) and (105, 105) will be cached, (104, 104) will be read
                            },
                            new DataRegion[] {
                            new DataRegion(1, 2),
                            new DataRegion(5, 10),
                            //(5, 10) is entirely cached
                            new DataRegion(11, 15),
                            new DataRegion(0, 0), new DataRegion(3, 4), new DataRegion(16, 100),
                            //(0, 0) is entirely cached
                            //(1, 1) is entirely cached
                            //(100, 100) is entirely cached
                            new DataRegion(101, 101),
                            new DataRegion(103, 103),
                            new DataRegion(102, 102),
                            new DataRegion(105, 105),
                            new DataRegion(104, 104),
                            });

                            ReadWorks(baseStorage, cached, pageB,
                                new DataRegion[] {
                            new DataRegion(100, 150),
                            new DataRegion(99, 151),//(100, 150) will be cached, (99, 99) and (151, 151) will be read
                            new DataRegion(90, 175),//(99, 151) will be cached, (90, 98) and (152, 175) will be read
                            new DataRegion(75, 80),
                            new DataRegion(200, 250),
                            new DataRegion(77, 225),//(77, 80) and (90, 175) and (200, 225) will be cached, (81, 89) and (176, 199) will be read
                                },
                                new DataRegion[] {
                            new DataRegion(100, 150),
                            new DataRegion(99, 99), new DataRegion(151, 151),
                            new DataRegion(90, 98), new DataRegion(152, 175),
                            new DataRegion(75, 80),
                            new DataRegion(200, 250),
                            new DataRegion(81, 89), new DataRegion(176, 199),
                                });

                            ReadWorks(baseStorage, cached, pageC,
                                new DataRegion[] {
                            new DataRegion(1, 1),
                            new DataRegion(3, 3),
                            new DataRegion(5, 5),
                            new DataRegion(7, 7),
                            new DataRegion(0, 8),//Will only read (0, 0), (2, 2), (4, 4), (6, 6), and (8, 8)
                                },
                                new DataRegion[] {
                            new DataRegion(1, 1),
                            new DataRegion(3, 3),
                            new DataRegion(5, 5),
                            new DataRegion(7, 7),
                            new DataRegion(0, 0), new DataRegion(2, 2), new DataRegion(4, 4), new DataRegion(6, 6), new DataRegion(8, 8),
                                });
                        }
                    }
                }
            }
        }

        [TestMethod]
        public void ReadWorksWhenCacheCapacityIsZero()
        {
            using (MemoryStream ms = new MemoryStream())
            {
                using (MockSafeResizableStorage mockStorage = new MockSafeResizableStorage(ms, true, true, true, null, 1024))
                {
                    using (MockPageStorage baseStorage = new MockPageStorage(mockStorage, 1024, 3, false, true))
                    {
                        Assert.IsTrue(baseStorage.TryAllocatePage(out long pageA));
                        Assert.IsTrue(baseStorage.TryAllocatePage(out long pageB));

                        //Initialize payload to each page
                        byte[] page = new byte[1024];
                        for (int i = 0; i < page.Length; i++)
                            page[i] = (byte)i;
                        baseStorage.WriteTo(pageA, 0, page, 0, page.Length);
                        baseStorage.WriteTo(pageB, 0, page, 0, page.Length);

                        using (CachedPageStorage cached = new CachedPageStorage(baseStorage, CachedPageStorage.CacheWriteMode.WriteBack, 0, true))
                        {
                            ReadWorks(baseStorage, cached, pageA,
                            new DataRegion[] {
                            new DataRegion(1, 2),
                            new DataRegion(5, 10),
                            new DataRegion(5, 10),
                            new DataRegion(5, 15),
                            new DataRegion(0, 100),
                            new DataRegion(0, 0),
                            new DataRegion(1, 1),
                            new DataRegion(100, 100),
                            new DataRegion(101, 101),
                            new DataRegion(103, 103),
                            new DataRegion(102, 102),
                            new DataRegion(105, 105),
                            new DataRegion(0, 105),
                            },
                            new DataRegion[] {
                            new DataRegion(1, 2),
                            new DataRegion(5, 10),
                            new DataRegion(5, 10),
                            new DataRegion(5, 15),
                            new DataRegion(0, 100),
                            new DataRegion(0, 0),
                            new DataRegion(1, 1),
                            new DataRegion(100, 100),
                            new DataRegion(101, 101),
                            new DataRegion(103, 103),
                            new DataRegion(102, 102),
                            new DataRegion(105, 105),
                            new DataRegion(0, 105),
                            });

                            ReadWorks(baseStorage, cached, pageB,
                                new DataRegion[] {
                            new DataRegion(100, 150),
                            new DataRegion(99, 151),
                            new DataRegion(90, 175),
                            new DataRegion(75, 80),
                            new DataRegion(200, 250),
                            new DataRegion(77, 225),
                                },
                                new DataRegion[] {
                            new DataRegion(100, 150),
                            new DataRegion(99, 151),
                            new DataRegion(90, 175),
                            new DataRegion(75, 80),
                            new DataRegion(200, 250),
                            new DataRegion(77, 225),
                                });
                        }
                    }
                }
            }
        }

#if DEBUG
        [TestMethod]
        public void ReadWorksWhenCacheFails()
        {
            using (MemoryStream ms = new MemoryStream())
            {
                using (MockSafeResizableStorage mockStorage = new MockSafeResizableStorage(ms, true, true, true, null, 1024))
                {
                    using (MockPageStorage baseStorage = new MockPageStorage(mockStorage, 1024, 3, false, true))
                    {
                        Assert.IsTrue(baseStorage.TryAllocatePage(out long pageA));
                        Assert.IsTrue(baseStorage.TryAllocatePage(out long pageB));

                        //Initialize payload to each page
                        byte[] page = new byte[1024];
                        for (int i = 0; i < page.Length; i++)
                            page[i] = (byte)i;
                        baseStorage.WriteTo(pageA, 0, page, 0, page.Length);
                        baseStorage.WriteTo(pageB, 0, page, 0, page.Length);

                        using (CachedPageStorage cached = new CachedPageStorage(baseStorage, CachedPageStorage.CacheWriteMode.WriteBack, 100, true))
                        {
                            cached.SimulateOutOfMemory = true;

                            ReadWorks(baseStorage, cached, pageA,
                                new DataRegion[] {
                            new DataRegion(1, 2),
                            new DataRegion(5, 10),
                            new DataRegion(5, 10),
                            new DataRegion(5, 15),
                            new DataRegion(0, 100),
                            new DataRegion(0, 0),
                            new DataRegion(1, 1),
                            new DataRegion(100, 100),
                            new DataRegion(101, 101),
                            new DataRegion(103, 103),
                            new DataRegion(102, 102),
                            new DataRegion(105, 105),
                            new DataRegion(0, 105),
                                },
                                new DataRegion[] {
                            new DataRegion(1, 2),
                            new DataRegion(5, 10),
                            new DataRegion(5, 10),
                            new DataRegion(5, 15),
                            new DataRegion(0, 100),
                            new DataRegion(0, 0),
                            new DataRegion(1, 1),
                            new DataRegion(100, 100),
                            new DataRegion(101, 101),
                            new DataRegion(103, 103),
                            new DataRegion(102, 102),
                            new DataRegion(105, 105),
                            new DataRegion(0, 105),
                                });

                            ReadWorks(baseStorage, cached, pageB,
                                new DataRegion[] {
                            new DataRegion(100, 150),
                            new DataRegion(99, 151),
                            new DataRegion(90, 175),
                            new DataRegion(75, 80),
                            new DataRegion(200, 250),
                            new DataRegion(77, 225),
                                },
                                new DataRegion[] {
                            new DataRegion(100, 150),
                            new DataRegion(99, 151),
                            new DataRegion(90, 175),
                            new DataRegion(75, 80),
                            new DataRegion(200, 250),
                            new DataRegion(77, 225),
                                });
                        }
                    }
                }
            }
        }
#endif

        [DataTestMethod]
        [DataRow(0)]
        [DataRow(1)]
        [DataRow(2)]
        [DataRow(3)]
        [DataRow(5)]
        [DataRow(16)]
        [DataRow(17)]
        [DataRow(18)]
        [DataRow(128)]
        public void ReadFromUncachedPageMayCauseLRUPageToBeEvicted(int cacheCapacity)
        {
            using (MemoryStream ms = new MemoryStream())
            {
                using (MockSafeResizableStorage mockStorage = new MockSafeResizableStorage(ms, true, true, true, null, 1024))
                {
                    using (MockPageStorage baseStorage = new MockPageStorage(mockStorage, 1024, cacheCapacity + 1, false, true))
                    {
                        using (CachedPageStorage cached = new CachedPageStorage(baseStorage, CachedPageStorage.CacheWriteMode.WriteBack, cacheCapacity, true))
                        {
                            //Allocate the to-be oldest page (the page that should be evicted)
                            Assert.IsTrue(cached.TryAllocatePage(out long oldestIndex));

                            //Write some data to its cache (but don't flush)
                            byte[] buffer = new byte[cached.PageSize];
                            for (int i = 0; i < buffer.Length; i++)
                                buffer[i] = (byte)i;
                            cached.WriteTo(oldestIndex, 0, buffer, 0, buffer.Length);

                            //Allocate the newer pages
                            long[] newIndices = new long[cacheCapacity];
                            for (long i = 0; i < newIndices.Length; i++)
                                Assert.IsTrue(cached.TryAllocatePage(out newIndices[i]));

                            //Monitor for the base write call
                            long baseWriteCallCount = 0;
                            baseStorage.OnWriteTo += (stg, indx, dstOff, buff, srcOff, len) => {
                                baseWriteCallCount++;
                            };

                            //First, read from the oldest page
                            byte[] read = new byte[1];
                            cached.ReadFrom(oldestIndex, 0, read, 0, read.Length);

                            //Now read from all of the newer pages
                            for (long i = 0; i < cacheCapacity; i++)
                            {
                                Assert.IsTrue(cached.IsPageCached(oldestIndex));
                                Assert.AreEqual(0, baseWriteCallCount);
                                cached.ReadFrom(newIndices[i], 0, read, 0, read.Length);

                                if (i == cacheCapacity - 1)
                                {
                                    Assert.AreEqual(1, baseWriteCallCount);
                                    Assert.IsFalse(cached.IsPageCached(oldestIndex));
                                }
                            }

                            //Read from the evicted page, make sure its payload was correctly written
                            read = new byte[buffer.Length];
                            cached.ReadFrom(oldestIndex, 0, read, 0, read.Length);
                            for (long i = 0; i < buffer.Length; i++)
                                Assert.AreEqual(buffer[i], read[i]);
                        }
                    }
                }
            }
        }

        [DataTestMethod]
        [DataRow(1)]
        [DataRow(2)]
        [DataRow(3)]
        [DataRow(4)]
        [DataRow(5)]
        [DataRow(8)]
        [DataRow(24)]
        [DataRow(32)]
        [DataRow(64)]
        [DataRow(256)]
        public void ReadCausesPageToBeCountedAsRecentlyUsed(int cacheCapacity)
        {
            using (MemoryStream ms = new MemoryStream())
            {
                using (MockSafeResizableStorage mockStorage = new MockSafeResizableStorage(ms, true, true, true, null, 1024))
                {
                    using (MockPageStorage baseStorage = new MockPageStorage(mockStorage, 1024, cacheCapacity + 1, false, true))
                    {
                        using (CachedPageStorage cached = new CachedPageStorage(baseStorage, CachedPageStorage.CacheWriteMode.WriteBack, cacheCapacity, true))
                        {
                            //Allocate the pages
                            long[] newIndices = new long[cacheCapacity];
                            for (long i = 0; i < newIndices.Length; i++)
                                Assert.IsTrue(cached.TryAllocatePage(out newIndices[i]));

                            byte[] read = new byte[1];

                            //Read all pages into cache
                            for (long i = 0; i < cacheCapacity; i++)
                                cached.ReadFrom(newIndices[i], 0, read, 0, read.Length);

                            //Now ensure that cache order is what was expected
                            CollectionAssert.AreEquivalent(newIndices/*correct order*/, cached.CachedPageIndices.ToArray());

                            List<long> indices = new List<long>();
                            indices.AddRange(newIndices);

                            //Read pages again, this time monitoring the 'recent use' position
                            for (long i = 0; i < cacheCapacity; i++)
                            {
                                indices.Remove(newIndices[i]);
                                indices.Insert(0, newIndices[i]);

                                cached.ReadFrom(newIndices[i], 0, read, 0, read.Length);

                                CollectionAssert.AreEquivalent(indices, cached.CachedPageIndices.ToList());
                            }
                        }
                    }
                }
            }
        }

        [TestMethod]
        public void WriteToThrowsWhenBufferIsNull()
        {
            using (MemoryStream ms = new MemoryStream())
            {
                const long pageCapacity = 5;
                const long pageSize = 1024;

                using (var baseStorage = new MockPageStorage(ms, pageSize, pageCapacity, false, false))
                {
                    long index = -1;
                    using (CachedPageStorage cached = new CachedPageStorage(baseStorage, CachedPageStorage.CacheWriteMode.WriteBack, 3, true))
                    {
                        //Allocate the page
                        Assert.IsTrue(cached.TryAllocatePage(out index));
                        Assert.IsTrue(cached.IsPageAllocated(index));
                        Assert.IsTrue(baseStorage.IsPageAllocated(index));
                        Assert.AreEqual(1, cached.AllocatedPageCount);
                        Assert.AreEqual(1, baseStorage.AllocatedPageCount);

                        //Try to write using null buffer
                        Assert.ThrowsException<ArgumentNullException>(() => {
                            cached.WriteTo(index, 0, null, 0, 1);
                        });
                    }
                }
            }
        }
        
        [TestMethod]
        public void WriteToThrowsWhenReadOnly()
        {
            using (MemoryStream ms = new MemoryStream())
            {
                const long pageCapacity = 5;
                const long pageSize = 1024;

                using (var baseStorage = new MockPageStorage(ms, pageSize, pageCapacity, false, false))
                {
                    long index = -1;
                    using (CachedPageStorage cached = new CachedPageStorage(baseStorage, CachedPageStorage.CacheWriteMode.WriteBack, 3, true))
                    {
                        //Allocate the page
                        Assert.IsTrue(cached.TryAllocatePage(out index));
                        Assert.IsTrue(cached.IsPageAllocated(index));
                        Assert.IsTrue(baseStorage.IsPageAllocated(index));
                        Assert.AreEqual(1, cached.AllocatedPageCount);
                        Assert.AreEqual(1, baseStorage.AllocatedPageCount);

                        //Make sure the base write method is not called
                        baseStorage.OnWriteTo += (stg, pgIndx, dstOff, buf, srcOff, len) => {
                            Assert.Fail("The base method must not be called.");
                        };

                        //Try to write using null buffer
                        Assert.ThrowsException<ArgumentNullException>(() => {
                            cached.WriteTo(index, 0, null, 0, 1);
                        });
                    }
                }
            }
        }

        [DataTestMethod]
        [DataRow(StreamingPageStorage.MinPageSize, 0, -1, 0, 0)]
        [DataRow(StreamingPageStorage.MinPageSize, 0, 0, -1, 0)]
        [DataRow(StreamingPageStorage.MinPageSize, 0, 0, 0, -1)]
        [DataRow(StreamingPageStorage.MinPageSize, 1, -1, 0, 0)]
        [DataRow(StreamingPageStorage.MinPageSize, 1, 0, -1, 0)]
        [DataRow(StreamingPageStorage.MinPageSize, 1, 0, 0, -1)]
        [DataRow(StreamingPageStorage.MinPageSize, 2, -1, 0, 0)]
        [DataRow(StreamingPageStorage.MinPageSize, 2, 0, -1, 0)]
        [DataRow(StreamingPageStorage.MinPageSize, 2, 0, 0, -1)]
        [DataRow(StreamingPageStorage.MinPageSize, 2, StreamingPageStorage.MinPageSize, 0, 1)]
        [DataRow(StreamingPageStorage.MinPageSize, 2, 0, 2, 1)]
        [DataRow(StreamingPageStorage.MinPageSize, 2, 0, 0, 3)]
        public void WriteToThrowsWhenArgIsOutOfRange(long pageSize, long bufferSize, long dstOffset, long srcOffset, long length)
        {
            using (MemoryStream ms = new MemoryStream())
            {
                const long pageCapacity = 5;

                using (var baseStorage = new MockPageStorage(ms, pageSize, pageCapacity, false, false))
                {
                    long index = -1;

                    using (CachedPageStorage cached = new CachedPageStorage(baseStorage, CachedPageStorage.CacheWriteMode.WriteBack, 3, true))
                    {
                        //Allocate the page
                        Assert.IsTrue(cached.TryAllocatePage(out index));
                        Assert.IsTrue(cached.IsPageAllocated(index));
                        Assert.IsTrue(baseStorage.IsPageAllocated(index));
                        Assert.AreEqual(1, cached.AllocatedPageCount);
                        Assert.AreEqual(1, baseStorage.AllocatedPageCount);

                        byte[] buffer = new byte[bufferSize];

                        //Try to read using bad arguments
                        Assert.ThrowsException<ArgumentOutOfRangeException>(() => {
                            cached.WriteTo(index, dstOffset, buffer, srcOffset, length);
                        });
                    }
                }
            }
        }

        [TestMethod]
        public void WriteToThrowsWhenPageIsNotAllocated()
        {
            using (MemoryStream ms = new MemoryStream())
            {
                const long pageCapacity = 5;
                const long pageSize = 1024;

                using (var baseStorage = new MockPageStorage(ms, pageSize, pageCapacity, false, false))
                {
                    using (CachedPageStorage cached = new CachedPageStorage(baseStorage, CachedPageStorage.CacheWriteMode.WriteBack, 3, true))
                    {
                        baseStorage.OnWriteTo += (stg, idx, dstOff, buf, srcOff, len) => {
                            Assert.Fail("The base method must not be called.");
                        };

                        Assert.ThrowsException<InvalidOperationException>(() => {
                            cached.ReadFrom(-1, 0, new byte[1], 0, 1);
                        });

                        Assert.ThrowsException<InvalidOperationException>(() => {
                            cached.ReadFrom(pageCapacity, 0, new byte[1], 0, 1);
                        });

                        Assert.ThrowsException<InvalidOperationException>(() => {
                            cached.ReadFrom(pageCapacity + 1, 0, new byte[1], 0, 1);
                        });

                        for (long i = 0; i < pageCapacity; i++)
                        {
                            Assert.ThrowsException<InvalidOperationException>(() => {
                                cached.WriteTo(pageCapacity, i, new byte[1], 0, 1);
                            });
                        }
                    }
                }
            }
        }

        private void WriteDataRegionTo(CachedPageStorage cached, long pageIndex, DataRegion region)
        {
            long bufferOffset = 5;
            byte[] buffer = new byte[region.Length + bufferOffset];
            for (long i = 0; i < region.Length; i++)
                buffer[i + bufferOffset] = (byte)(i + region.FirstIndex);

            cached.WriteTo(pageIndex, region.FirstIndex, buffer, bufferOffset, region.Length);
        }

        private void ValidateDataRegionAt(IPageStorage storage, long pageIndex, DataRegion region)
        {
            long bufferOffset = 7;
            byte[] buffer = new byte[region.Length + bufferOffset];
            storage.ReadFrom(pageIndex, region.FirstIndex, buffer, bufferOffset, region.Length);

            for (long i = 0; i < region.Length; i++)
                Assert.AreEqual((byte)(i + region.FirstIndex), buffer[i + bufferOffset]);
        }

        [TestMethod]
        public void WriteWorksUsingWriteBackMode()
        {
            using (MemoryStream ms = new MemoryStream())
            {
                const long pageCapacity = 5;
                const long pageSize = 1024;

                using (var baseStorage = new MockPageStorage(ms, pageSize, pageCapacity, false, false))
                {
                    using (CachedPageStorage cached = new CachedPageStorage(baseStorage, CachedPageStorage.CacheWriteMode.WriteBack, 3, true))
                    {
                        Assert.IsTrue(cached.TryAllocatePage(out long index));

                        long baseWriteCallCount = 0;
                        baseStorage.OnWriteTo += (srg, pgIndx, dstOff, buf, srcOff, len) => {
                            baseWriteCallCount++;
                        };

                        WriteDataRegionTo(cached, index, new DataRegion(0, 0));
                        WriteDataRegionTo(cached, index, new DataRegion(1, 1));
                        WriteDataRegionTo(cached, index, new DataRegion(3, 3));
                        WriteDataRegionTo(cached, index, new DataRegion(3, 5));
                        WriteDataRegionTo(cached, index, new DataRegion(6, 10));
                        WriteDataRegionTo(cached, index, new DataRegion(15, 20));
                        WriteDataRegionTo(cached, index, new DataRegion(14, 14));
                        WriteDataRegionTo(cached, index, new DataRegion(14, 15));
                        WriteDataRegionTo(cached, index, new DataRegion(15, 16));
                        WriteDataRegionTo(cached, index, new DataRegion(9, 10));
                        WriteDataRegionTo(cached, index, new DataRegion(9, 12));

                        //Make sure nothing was sent to base storage yet
                        Assert.AreEqual(0, baseWriteCallCount);

                        //Make sure the data was stored in cache correctly
                        ValidateDataRegionAt(cached, index, new DataRegion(0, 1));
                        ValidateDataRegionAt(cached, index, new DataRegion(3, 10));
                        ValidateDataRegionAt(cached, index, new DataRegion(14, 20));

                        //Flush data to the base storage
                        cached.Flush();

                        //Make sure it was sent to base storage correctly
                        Assert.AreEqual(3, baseWriteCallCount);
                        ValidateDataRegionAt(baseStorage, index, new DataRegion(0, 1));
                        ValidateDataRegionAt(baseStorage, index, new DataRegion(3, 10));
                        ValidateDataRegionAt(baseStorage, index, new DataRegion(14, 20));
                    }
                }
            }
        }

        [TestMethod]
        public void WriteWorksUsingWriteThroughMode()
        {
            using (MemoryStream ms = new MemoryStream())
            {
                const long pageCapacity = 5;
                const long pageSize = 1024;

                using (var baseStorage = new MockPageStorage(ms, pageSize, pageCapacity, false, false))
                {
                    using (CachedPageStorage cached = new CachedPageStorage(baseStorage, CachedPageStorage.CacheWriteMode.WriteThrough, 3, true))
                    {
                        Assert.IsTrue(cached.TryAllocatePage(out long index));

                        DataRegion previousRegion = new DataRegion(0,0);

                        long baseWriteCallCount = 0;
                        baseStorage.OnWriteTo += (srg, pgIndx, dstOff, buf, srcOff, len) => {
                            Assert.AreEqual(pgIndx, index);
                            Assert.AreEqual(previousRegion.FirstIndex, dstOff);
                            Assert.AreEqual(previousRegion.Length, len);
                            baseWriteCallCount++;
                        };

                        previousRegion = new DataRegion(0, 0);
                        WriteDataRegionTo(cached, index, previousRegion);
                        previousRegion = new DataRegion(1, 1);
                        WriteDataRegionTo(cached, index, previousRegion);
                        previousRegion = new DataRegion(3, 3);
                        WriteDataRegionTo(cached, index, previousRegion);
                        previousRegion = new DataRegion(3, 5);
                        WriteDataRegionTo(cached, index, previousRegion);
                        previousRegion = new DataRegion(6, 10);
                        WriteDataRegionTo(cached, index, previousRegion);
                        previousRegion = new DataRegion(15, 20);
                        WriteDataRegionTo(cached, index, previousRegion);
                        previousRegion = new DataRegion(14, 14);
                        WriteDataRegionTo(cached, index, previousRegion);
                        previousRegion = new DataRegion(14, 15);
                        WriteDataRegionTo(cached, index, previousRegion);
                        previousRegion = new DataRegion(15, 16);
                        WriteDataRegionTo(cached, index, previousRegion);
                        previousRegion = new DataRegion(9, 10);
                        WriteDataRegionTo(cached, index, previousRegion);
                        previousRegion = new DataRegion(9, 12);
                        WriteDataRegionTo(cached, index, previousRegion);

                        Assert.AreEqual(11, baseWriteCallCount);

                        ValidateDataRegionAt(baseStorage, index, new DataRegion(0, 1));
                        ValidateDataRegionAt(baseStorage, index, new DataRegion(3, 10));
                        ValidateDataRegionAt(baseStorage, index, new DataRegion(14, 20));
                    }
                }
            }
        }

        [DataTestMethod]
        [DataRow(CachedPageStorage.CacheWriteMode.WriteBack)]
        [DataRow(CachedPageStorage.CacheWriteMode.WriteThrough)]
        public void WriteWorksWhenCacheFails(CachedPageStorage.CacheWriteMode writeMode)
        {
            using (MemoryStream ms = new MemoryStream())
            {
                const long pageCapacity = 5;
                const long pageSize = 1024;

                using (var baseStorage = new MockPageStorage(ms, pageSize, pageCapacity, false, false))
                {
                    using (CachedPageStorage cached = new CachedPageStorage(baseStorage, writeMode, 3, true))
                    {
                        Assert.IsTrue(cached.TryAllocatePage(out long index));

                        DataRegion previousRegion = new DataRegion(0, 0);

                        long baseWriteCallCount = 0;
                        baseStorage.OnWriteTo += (srg, pgIndx, dstOff, buf, srcOff, len) => {
                            Assert.AreEqual(pgIndx, index);
                            Assert.AreEqual(previousRegion.FirstIndex, dstOff);
                            Assert.AreEqual(previousRegion.Length, len);
                            baseWriteCallCount++;
                        };

                        cached.SimulateOutOfMemory = true;

                        //When cache fails, we expect to default to write-through
                        previousRegion = new DataRegion(0, 0);
                        WriteDataRegionTo(cached, index, previousRegion);
                        previousRegion = new DataRegion(1, 1);
                        WriteDataRegionTo(cached, index, previousRegion);
                        previousRegion = new DataRegion(3, 3);
                        WriteDataRegionTo(cached, index, previousRegion);
                        previousRegion = new DataRegion(3, 5);
                        WriteDataRegionTo(cached, index, previousRegion);
                        previousRegion = new DataRegion(6, 10);
                        WriteDataRegionTo(cached, index, previousRegion);
                        previousRegion = new DataRegion(15, 20);
                        WriteDataRegionTo(cached, index, previousRegion);
                        previousRegion = new DataRegion(14, 14);
                        WriteDataRegionTo(cached, index, previousRegion);
                        previousRegion = new DataRegion(14, 15);
                        WriteDataRegionTo(cached, index, previousRegion);
                        previousRegion = new DataRegion(15, 16);
                        WriteDataRegionTo(cached, index, previousRegion);
                        previousRegion = new DataRegion(9, 10);
                        WriteDataRegionTo(cached, index, previousRegion);
                        previousRegion = new DataRegion(9, 12);
                        WriteDataRegionTo(cached, index, previousRegion);

                        Assert.AreEqual(11, baseWriteCallCount);

                        ValidateDataRegionAt(baseStorage, index, new DataRegion(0, 1));
                        ValidateDataRegionAt(baseStorage, index, new DataRegion(3, 10));
                        ValidateDataRegionAt(baseStorage, index, new DataRegion(14, 20));
                    }
                }
            }
        }

        [DataTestMethod]
        [DataRow(CachedPageStorage.CacheWriteMode.WriteBack)]
        [DataRow(CachedPageStorage.CacheWriteMode.WriteThrough)]
        public void WriteWorksWhenCachePageCapacityIsZero(CachedPageStorage.CacheWriteMode writeMode)
        {
            using (MemoryStream ms = new MemoryStream())
            {
                const long pageCapacity = 5;
                const long pageSize = 1024;

                using (var baseStorage = new MockPageStorage(ms, pageSize, pageCapacity, false, false))
                {
                    using (CachedPageStorage cached = new CachedPageStorage(baseStorage, writeMode, 0, true))
                    {
                        Assert.IsTrue(cached.TryAllocatePage(out long index));

                        DataRegion previousRegion = new DataRegion(0, 0);

                        long baseWriteCallCount = 0;
                        baseStorage.OnWriteTo += (srg, pgIndx, dstOff, buf, srcOff, len) => {
                            Assert.AreEqual(pgIndx, index);
                            Assert.AreEqual(previousRegion.FirstIndex, dstOff);
                            Assert.AreEqual(previousRegion.Length, len);
                            baseWriteCallCount++;
                        };

                        //When cache capacity is zero, we expect to default to write-through
                        previousRegion = new DataRegion(0, 0);
                        WriteDataRegionTo(cached, index, previousRegion);
                        previousRegion = new DataRegion(1, 1);
                        WriteDataRegionTo(cached, index, previousRegion);
                        previousRegion = new DataRegion(3, 3);
                        WriteDataRegionTo(cached, index, previousRegion);
                        previousRegion = new DataRegion(3, 5);
                        WriteDataRegionTo(cached, index, previousRegion);
                        previousRegion = new DataRegion(6, 10);
                        WriteDataRegionTo(cached, index, previousRegion);
                        previousRegion = new DataRegion(15, 20);
                        WriteDataRegionTo(cached, index, previousRegion);
                        previousRegion = new DataRegion(14, 14);
                        WriteDataRegionTo(cached, index, previousRegion);
                        previousRegion = new DataRegion(14, 15);
                        WriteDataRegionTo(cached, index, previousRegion);
                        previousRegion = new DataRegion(15, 16);
                        WriteDataRegionTo(cached, index, previousRegion);
                        previousRegion = new DataRegion(9, 10);
                        WriteDataRegionTo(cached, index, previousRegion);
                        previousRegion = new DataRegion(9, 12);
                        WriteDataRegionTo(cached, index, previousRegion);

                        Assert.IsFalse(cached.IsPageCached(index));//Should not be cached since cache capacity is zero

                        Assert.AreEqual(11, baseWriteCallCount);

                        ValidateDataRegionAt(baseStorage, index, new DataRegion(0, 1));
                        ValidateDataRegionAt(baseStorage, index, new DataRegion(3, 10));
                        ValidateDataRegionAt(baseStorage, index, new DataRegion(14, 20));
                    }
                }
            }
        }

        [DataTestMethod]
        [DataRow(CachedPageStorage.CacheWriteMode.WriteBack)]
        [DataRow(CachedPageStorage.CacheWriteMode.WriteThrough)]
        public void WrittenDataIsCachedForFutureReads(CachedPageStorage.CacheWriteMode writeMode)
        {
            using (MemoryStream ms = new MemoryStream())
            {
                const long pageCapacity = 5;
                const long pageSize = 1024;

                using (var baseStorage = new MockPageStorage(ms, pageSize, pageCapacity, false, false))
                {
                    using (CachedPageStorage cached = new CachedPageStorage(baseStorage, writeMode, 3, true))
                    {
                        Assert.IsTrue(cached.TryAllocatePage(out long index));

                        DataRegion lastReadDataRegion = new DataRegion();

                        long baseReadCallCount = 0;
                        baseStorage.OnRead += (srg, pgIndx, srcOff, buf, dstOff, len) => {
                            baseReadCallCount++;
                            lastReadDataRegion = new DataRegion(srcOff, srcOff + len - 1);
                        };

                        //Write some data
                        WriteDataRegionTo(cached, index, new DataRegion(0, 0));
                        WriteDataRegionTo(cached, index, new DataRegion(2, 2));
                        WriteDataRegionTo(cached, index, new DataRegion(3, 5));
                        WriteDataRegionTo(cached, index, new DataRegion(10, 15));

                        //Read (directly from cache)
                        ValidateDataRegionAt(cached, index, new DataRegion(0, 0));
                        ValidateDataRegionAt(cached, index, new DataRegion(2, 5));
                        ValidateDataRegionAt(cached, index, new DataRegion(10, 15));

                        //Make sure those reads didn't call to base storage since data was cached
                        Assert.AreEqual(0, baseReadCallCount);

                        //Read some data that was written, and some that wasn't
                        byte[] buffer = new byte[5];
                        cached.ReadFrom(index, 0, buffer, 0, 4);

                        //Make sure base storage only received a read request for (1,1)
                        Assert.AreEqual(new DataRegion(1, 1), lastReadDataRegion);
                    }
                }
            }
        }

        [DataTestMethod]
        [DataRow(1)]
        [DataRow(2)]
        [DataRow(3)]
        [DataRow(5)]
        public void WrittenDataIsFlushedWhenPageIsEvicted(int cacheCapacity)
        {
            using (MemoryStream ms = new MemoryStream())
            {
                const long pageSize = 1024;

                using (var baseStorage = new MockPageStorage(ms, pageSize, cacheCapacity + 1, false, false))
                {
                    using (CachedPageStorage cached = new CachedPageStorage(baseStorage, CachedPageStorage.CacheWriteMode.WriteBack, cacheCapacity, true))
                    {
                        long[] readIndices = new long[cacheCapacity];
                        for (long i = 0; i < cacheCapacity; i++)
                            Assert.IsTrue(cached.TryAllocatePage(out readIndices[i]));

                        Assert.IsTrue(cached.TryAllocatePage(out long index));

                        long baseWriteCount = 0;
                        baseStorage.OnWriteTo += (stg, pgIndx, dstOff, buf, srcOff, len) => {
                            baseWriteCount++;
                        };

                        //Write some data
                        WriteDataRegionTo(cached, index, new DataRegion(0, 25));

                        //Read enough pages to cause the written data to be flushed (due to page eviction)
                        for(int i = 0; i < cacheCapacity; i++)
                        {
                            Assert.IsTrue(cached.IsPageCached(index));
                            Assert.AreEqual(0, baseWriteCount);
                            cached.ReadFrom(readIndices[i], 0, new byte[1], 0, 1);
                        }

                        //Enusre that the page was evicted
                        Assert.IsFalse(cached.IsPageCached(index));

                        //Read the data that we wrote, ensure it was written to base storage
                        Assert.AreEqual(1, baseWriteCount);
                        ValidateDataRegionAt(baseStorage, index, new DataRegion(0, 25));
                    }
                }
            }
        }

        [TestMethod]
        public void WrittenDataIsFlushedWhenFlushIsCalled()
        {
            using (MemoryStream ms = new MemoryStream())
            {
                const long pageSize = 1024;

                using (var baseStorage = new MockPageStorage(ms, pageSize, 5, false, false))
                {
                    using (CachedPageStorage cached = new CachedPageStorage(baseStorage, CachedPageStorage.CacheWriteMode.WriteBack, 4, true))
                    {
                        Assert.IsTrue(cached.TryAllocatePage(out long index));

                        long baseWriteCount = 0;
                        baseStorage.OnWriteTo += (stg, pgIndx, dstOff, buf, srcOff, len) => {
                            baseWriteCount++;
                        };

                        //Write some data
                        WriteDataRegionTo(cached, index, new DataRegion(0, 25));

                        //Make sure the data was not yet written to base storage
                        Assert.AreEqual(0, baseWriteCount);

                        //Flush the cached write data
                        cached.Flush();

                        //Ensure data was correctly sent to base storage
                        Assert.AreEqual(1, baseWriteCount);
                        ValidateDataRegionAt(baseStorage, index, new DataRegion(0, 25));
                    }
                }
            }
        }

        [TestMethod]
        public void WrittenDataIsFlushedWhenDisposeIsCalled()
        {
            using (MemoryStream ms = new MemoryStream())
            {
                const long pageSize = 1024;

                using (var baseStorage = new MockPageStorage(ms, pageSize, 5, false, false))
                {
                    long baseWriteCount = 0;
                    baseStorage.OnWriteTo += (stg, pgIndx, dstOff, buf, srcOff, len) => {
                        baseWriteCount++;
                    };
                    Assert.IsTrue(baseStorage.TryAllocatePage(out long index));

                    using (CachedPageStorage cached = new CachedPageStorage(baseStorage, CachedPageStorage.CacheWriteMode.WriteBack, 4, true))
                    {
                        //Write some data
                        WriteDataRegionTo(cached, index, new DataRegion(0, 25));

                        //Make sure the data was not yet written to base storage
                        Assert.AreEqual(0, baseWriteCount);
                    }

                    //Ensure cached data was correctly sent to base storage after disposal
                    Assert.AreEqual(1, baseWriteCount);
                    ValidateDataRegionAt(baseStorage, index, new DataRegion(0, 25));
                }
            }
        }

        [DataTestMethod]
        [DataRow(1)]
        [DataRow(2)]
        [DataRow(3)]
        [DataRow(5)]
        public void WriteToUncachedPageMayCauseLRUPageToBeEvicted(int cacheCapacity)
        {
            using (MemoryStream ms = new MemoryStream())
            {
                const long pageSize = 1024;

                using (var baseStorage = new MockPageStorage(ms, pageSize, cacheCapacity + 1, false, false))
                {
                    long[] readIndices = new long[cacheCapacity];
                    for (int i = 0; i < cacheCapacity; i++)
                        Assert.IsTrue(baseStorage.TryAllocatePage(out readIndices[i]));
                    Assert.IsTrue(baseStorage.TryAllocatePage(out long index));

                    using (CachedPageStorage cached = new CachedPageStorage(baseStorage, CachedPageStorage.CacheWriteMode.WriteBack, cacheCapacity, true))
                    {
                        //Fill the cache
                        for (int i = 0; i < cacheCapacity; i++)
                            cached.ReadFrom(readIndices[i], 0, new byte[1], 0, 1);

                        //Write to an uncached page
                        cached.WriteTo(index, 0, new byte[1], 0, 1);

                        //Ensure that LRU page was evicted
                        Assert.IsFalse(cached.IsPageCached(readIndices[0]));

                        //Ensure that the written page is most recent
                        Assert.AreEqual(index, cached.CachedPageIndices.First());
                    }
                }
            }
        }

        [DataTestMethod]
        [DataRow(1)]
        [DataRow(2)]
        [DataRow(3)]
        [DataRow(4)]
        [DataRow(5)]
        [DataRow(8)]
        [DataRow(24)]
        [DataRow(32)]
        [DataRow(64)]
        [DataRow(256)]
        public void WriteCausesPageToBeCountedAsRecentlyUsed(int cacheCapacity)
        {
            using (MemoryStream ms = new MemoryStream())
            {
                using (MockSafeResizableStorage mockStorage = new MockSafeResizableStorage(ms, true, true, true, null, 1024))
                {
                    using (MockPageStorage baseStorage = new MockPageStorage(mockStorage, 1024, cacheCapacity + 1, false, true))
                    {
                        using (CachedPageStorage cached = new CachedPageStorage(baseStorage, CachedPageStorage.CacheWriteMode.WriteBack, cacheCapacity, true))
                        {
                            //Allocate the pages
                            long[] newIndices = new long[cacheCapacity];
                            for (long i = 0; i < newIndices.Length; i++)
                                Assert.IsTrue(cached.TryAllocatePage(out newIndices[i]));

                            byte[] read = new byte[1];

                            //Read all pages into cache
                            for (long i = 0; i < cacheCapacity; i++)
                                cached.ReadFrom(newIndices[i], 0, read, 0, read.Length);

                            //Now ensure that cache order is what was expected
                            CollectionAssert.AreEquivalent(newIndices/*correct order*/, cached.CachedPageIndices.ToArray());

                            List<long> indices = new List<long>();
                            indices.AddRange(newIndices);

                            //Write all pages, while monitoring the 'recent use' position
                            for (long i = 0; i < cacheCapacity; i++)
                            {
                                indices.Remove(newIndices[i]);
                                indices.Insert(0, newIndices[i]);

                                cached.WriteTo(newIndices[i], 0, read, 0, read.Length);

                                CollectionAssert.AreEquivalent(indices, cached.CachedPageIndices.ToList());
                            }
                        }
                    }
                }
            }
        }

        [DataTestMethod]
        [DataRow(0)]
        [DataRow(1)]
        [DataRow(2)]
        [DataRow(3)]
        [DataRow(5)]
        [DataRow(1024)]
        public void IsPageCachedWorks(int cacheCapacity)
        {
            using (MemoryStream ms = new MemoryStream())
            {
                using (MockSafeResizableStorage mockStorage = new MockSafeResizableStorage(ms, true, true, true, null, 1024))
                {
                    using (MockPageStorage baseStorage = new MockPageStorage(mockStorage, 1024, cacheCapacity + 100, false, true))
                    {
                        using (CachedPageStorage cached = new CachedPageStorage(baseStorage, CachedPageStorage.CacheWriteMode.WriteBack, cacheCapacity, true))
                        {
                            //Allocate the pages
                            long[] pageIndices = new long[cacheCapacity];
                            for (long i = 0; i < pageIndices.Length; i++)
                                Assert.IsTrue(cached.TryAllocatePage(out pageIndices[i]));
                            
                            //Evict all pages from cache
                            for (long i = 0; i < cacheCapacity; i++)
                                cached.EvictPageFromCache(pageIndices[i]);
                            
                            //Gradually load pages into cache
                            for(int i = 0; i < cacheCapacity; i++)
                            {
                                //Make sure all prior pages are in cache
                                for (int j = 0; j < i; j++)
                                    Assert.IsTrue(cached.IsPageCached(pageIndices[j]));

                                //Make sure all pages at and beyond 'i' are not in cache
                                for(int j = i; j < cacheCapacity; j++)
                                    Assert.IsFalse(cached.IsPageCached(pageIndices[j]));

                                //Read from the page, causing it to be loaded into cache
                                cached.ReadFrom(pageIndices[i], 0, new byte[1], 0, 1);

                                //Make sure the page is in cache
                                Assert.IsTrue(cached.IsPageCached(pageIndices[i]));
                            }

                            //Check indices of some non-allocated pages
                            Assert.IsFalse(cached.IsPageAllocated(cached.PageCapacity - 1));//Make sure the page was not allocated (otherwise invalid test)
                            Assert.IsFalse(cached.IsPageCached(cached.PageCapacity - 1));

                            //Check indices of non-existing pages
                            Assert.IsFalse(cached.IsPageAllocated(-1));
                            Assert.IsFalse(cached.IsPageAllocated(cached.PageCapacity));
                            Assert.IsFalse(cached.IsPageAllocated(cached.PageCapacity + 1));
                        }
                    }
                }
            }
        }

        [DataTestMethod]
        [DataRow(1)]
        [DataRow(2)]
        [DataRow(3)]
        [DataRow(5)]
        [DataRow(1024)]
        public void EvictPageFromCacheWorks(int cacheCapacity)
        {
            using (MemoryStream ms = new MemoryStream())
            {
                using (MockSafeResizableStorage mockStorage = new MockSafeResizableStorage(ms, true, true, true, null, 1024))
                {
                    using (MockPageStorage baseStorage = new MockPageStorage(mockStorage, 1024, cacheCapacity + 1, false, true))
                    {
                        using (CachedPageStorage cached = new CachedPageStorage(baseStorage, CachedPageStorage.CacheWriteMode.WriteBack, cacheCapacity, true))
                        {
                            //Allocate the pages
                            long[] pageIndices = new long[cacheCapacity];
                            for (long i = 0; i < pageIndices.Length; i++)
                                Assert.IsTrue(cached.TryAllocatePage(out pageIndices[i]));

                            //Load all pages into cache
                            for (int i = 0; i < cacheCapacity; i++)
                                cached.ReadFrom(pageIndices[i], 0, new byte[1], 0, 1);

                            //Write (to cache) data to each page
                            for (int i = 0; i < cacheCapacity; i++)
                                WriteDataRegionTo(cached, pageIndices[i], new DataRegion(0, 1023));

                            //Gradually evict the pages
                            for(int i = 0; i < cacheCapacity; i++)
                            {
                                Assert.IsTrue(cached.IsPageCached(pageIndices[i]));
                                Assert.IsTrue(cached.CachedPageIndices.Contains(pageIndices[i]));
                                Assert.IsTrue(cached.EvictPageFromCache(pageIndices[i]));
                                Assert.IsFalse(cached.IsPageCached(pageIndices[i]));
                                Assert.IsFalse(cached.CachedPageIndices.Contains(pageIndices[i]));

                                //Make sure data was written to base storage
                                ValidateDataRegionAt(baseStorage, pageIndices[i], new DataRegion(0, 1023));
                            }
                        }
                    }
                }
            }
        }

        [DataTestMethod]
        [DataRow(1)]
        [DataRow(2)]
        [DataRow(3)]
        [DataRow(5)]
        [DataRow(1024)]
        public void EvictPageFromCacheChangesNothingWhenPageWasNotCached(int cacheCapacity)
        {
            using (MemoryStream ms = new MemoryStream())
            {
                using (MockSafeResizableStorage mockStorage = new MockSafeResizableStorage(ms, true, true, true, null, 1024))
                {
                    using (MockPageStorage baseStorage = new MockPageStorage(mockStorage, 1024, cacheCapacity + 1, false, true))
                    {
                        using (CachedPageStorage cached = new CachedPageStorage(baseStorage, CachedPageStorage.CacheWriteMode.WriteBack, cacheCapacity, true))
                        {
                            //Allocate a page that will not be on cache
                            Assert.IsTrue(cached.TryAllocatePage(out long notOnCache));

                            //Make sure the page is not cached
                            cached.EvictPageFromCache(notOnCache);
                            Assert.IsFalse(cached.IsPageCached(notOnCache));

                            //Allocate the pages
                            long[] pageIndices = new long[cacheCapacity];
                            for (long i = 0; i < pageIndices.Length; i++)
                                Assert.IsTrue(cached.TryAllocatePage(out pageIndices[i]));

                            //Load full cache
                            for (int i = 0; i < cacheCapacity; i++)
                                cached.ReadFrom(pageIndices[i], 0, new byte[1], 0, 1);

                            //Evict the non-cached page
                            Assert.IsFalse(cached.EvictPageFromCache(notOnCache));

                            //Evict non-existing pages
                            Assert.IsFalse(cached.EvictPageFromCache(-1));
                            Assert.IsFalse(cached.EvictPageFromCache(cached.PageCapacity));
                            Assert.IsFalse(cached.EvictPageFromCache(cached.PageCapacity + 1));

                            //Ensure nothing changed
                            CollectionAssert.AreEquivalent(pageIndices, cached.CachedPageIndices.ToArray());
                        }
                    }
                }
            }
        }
        
        [TestMethod]
        public void DisposeWillDisposeBasePageStorageIfNotRequestedToLeaveOpen()
        {
            using (MemoryStream ms = new MemoryStream())
            {
                using (MockSafeResizableStorage mockStorage = new MockSafeResizableStorage(ms, true, true, true, null, 1024))
                {
                    MockPageStorage baseStorage = new MockPageStorage(mockStorage, 1024, 5, false, true);
                    long baseCallCount = 0;
                    baseStorage.OnDispose += (stg) => {
                        baseCallCount++;
                    };

                    using (CachedPageStorage cached = new CachedPageStorage(baseStorage, CachedPageStorage.CacheWriteMode.WriteBack, 3, false))
                    {
                        Assert.AreEqual(0, baseCallCount);
                    }

                    //Ensure that the CachedPageStorage disposed the MockPageStorage
                    Assert.AreEqual(1, baseCallCount);
                }
            }
        }

        [TestMethod]
        public void DisposeWillLeaveBasePageStorageOpenIfRequested()
        {
            using (MemoryStream ms = new MemoryStream())
            {
                using (MockSafeResizableStorage mockStorage = new MockSafeResizableStorage(ms, true, true, true, null, 1024))
                {
                    using (MockPageStorage baseStorage = new MockPageStorage(mockStorage, 1024, 5, false, true))
                    {
                        long baseCallCount = 0;
                        baseStorage.OnDispose += (stg) => {
                            baseCallCount++;
                        };

                        using (CachedPageStorage cached = new CachedPageStorage(baseStorage, CachedPageStorage.CacheWriteMode.WriteBack, 3, true))
                        {
                            Assert.AreEqual(0, baseCallCount);
                        }

                        //Ensure that the CachedPageStorage did not dispose the MockPageStorage
                        Assert.AreEqual(0, baseCallCount);
                    }
                }
            }
        }

        [TestMethod]
        public void DisposeAfterDisposedHasNoEffect()
        {
            using (MemoryStream ms = new MemoryStream())
            {
                using (MockSafeResizableStorage mockStorage = new MockSafeResizableStorage(ms, true, true, true, null, 1024))
                {
                    MockPageStorage baseStorage = new MockPageStorage(mockStorage, 1024, 5, false, true);
                    long baseCallCount = 0;
                    baseStorage.OnDispose += (stg) => {
                        baseCallCount++;
                    };

                    CachedPageStorage cached = new CachedPageStorage(baseStorage, CachedPageStorage.CacheWriteMode.WriteBack, 3, false);

                    //Dispose the CachedPageStorage
                    Assert.IsFalse(cached.IsDisposed);
                    cached.Dispose();
                    Assert.IsTrue(cached.IsDisposed);

                    //Ensure that the CachedPageStorage disposed the MockPageStorage
                    Assert.AreEqual(1, baseCallCount);

                    //Repeat the dispose call
                    Assert.IsTrue(cached.IsDisposed);
                    cached.Dispose();
                    Assert.IsTrue(cached.IsDisposed);

                    //Ensure that nothing changed
                    Assert.AreEqual(1, baseCallCount);
                }
            }
        }
    }
}
