using Microsoft.VisualStudio.TestTools.UnitTesting;
using Storage;
using Storage.Data;
using StorageTest.Mocks;
using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;

namespace StorageTest
{
    [TestClass]
    public class StreamingPageStorageTest
    {
        [TestMethod]
        public void CreateThrowsIfStreamIsNull()
        {
            Assert.ThrowsException<ArgumentNullException>(() => {
                StreamingPageStorage storage = StreamingPageStorage.Create(null, 1024, 64, null, new CancellationToken(false), true);
            });
        }

        [DataTestMethod]
        [DataRow(StreamingPageStorage.MinPageSize - 1, 64, 1024)]
        [DataRow(-1, 64, 1024)]
        [DataRow(1024, -1, 1024)]
        [DataRow(1024, 64, 0)]
        [DataRow(1024, 64, -1)]
        public void CreateThrowsIfArgsOutOfRange(long pageSize, long initCapacity, long resizeIncrement)
        {
            using (MemoryStream ms = new MemoryStream())
            {
                Assert.ThrowsException<ArgumentOutOfRangeException>(() => {
                    StreamingPageStorage storage = StreamingPageStorage.Create(ms, pageSize, initCapacity, null, new CancellationToken(false), true, resizeIncrement);
                });
            }
        }

        [DataTestMethod]
        [DataRow(false, true, true)]
        [DataRow(true, false, true)]
        [DataRow(true, true, false)]
        public void CreateThrowsIfStreamIsInvalid(bool canRead, bool canWrite, bool canSeek)
        {
            using (MemoryStream ms = new MemoryStream())
            {
                using (MockStream stream = new MockStream(ms, canRead, canWrite, canSeek))
                {
                    Assert.ThrowsException<ArgumentException>(() => {
                        StreamingPageStorage storage = StreamingPageStorage.Create(stream, 1024, 64, null, new CancellationToken(false), true, 1024);
                    });
                }
            }
        }

        [TestMethod]
        public void CreateCancellationWorks()
        {
            using (MemoryStream stream = new MemoryStream())
            {
                CancellationTokenSource cancellation = new CancellationTokenSource();
                ProgressReporter reporter = new ProgressReporter((x) => {
                    if (x.Current == 10)//Cancel after some progress
                        cancellation.Cancel();
                    else if (x.Current > 10)
                        Assert.Fail("Expected the creation to terminate immediately after cancellation, so no progress should have been reported.");
                });

                Assert.ThrowsException<OperationCanceledException>(() => {
                    StreamingPageStorage storage = StreamingPageStorage.Create(stream, 1024, 64, reporter, cancellation.Token, false, 1024);
                });
            }
        }

        [TestMethod]
        public void CreateReportsProgress()
        {
            using (MemoryStream stream = new MemoryStream())
            {
                long? targetProgress = null;
                long? previousValue = null;
                ProgressReporter reporter = new ProgressReporter((x) => {
                    if (targetProgress == null)
                        targetProgress = x.Target;
                    Assert.IsNotNull(targetProgress);

                    Assert.IsTrue(x.Current <= targetProgress.Value);

                    if (previousValue != null)
                        Assert.IsTrue(previousValue.Value <= x.Current);
                    previousValue = x.Current;
                });

                using (StreamingPageStorage storage = StreamingPageStorage.Create(stream, 1024, 64, reporter, new CancellationToken(false), true, 1/*To ensure resize is reported in progress too*/))
                {
                    Assert.AreEqual(targetProgress.Value, previousValue.Value);
                }
            }
        }

        [DataTestMethod]
        [DataRow(StreamingPageStorage.MinPageSize, 0)]
        [DataRow(StreamingPageStorage.MinPageSize, 1)]
        [DataRow(StreamingPageStorage.MinPageSize, 64)]
        [DataRow(StreamingPageStorage.MinPageSize + 1, 0)]
        [DataRow(StreamingPageStorage.MinPageSize + 1, 1)]
        [DataRow(StreamingPageStorage.MinPageSize + 1, 64)]
        [DataRow(1024, 0)]
        [DataRow(1024, 1)]
        [DataRow(1024, 64)]
        public void CreateWorks(long pageSize, long initCapacity)
        {
            using (MemoryStream ms = new MemoryStream())
            {
                using (MockStream stream = new MockStream(ms, true, true, true))
                {
                    using (StreamingPageStorage storage = StreamingPageStorage.Create(stream, pageSize, initCapacity, null, new CancellationToken(false), true, 128))
                    {
                        Assert.IsFalse(storage.IsDisposed);
                        Assert.AreEqual(0, storage.AllocatedPageCount);
                        Assert.AreEqual(initCapacity, storage.PageCapacity);
                        Assert.IsNull(storage.EntryPageIndex);
                        Assert.IsFalse(storage.IsCapacityFixed);
                        Assert.IsFalse(storage.IsReadOnly);
                        Assert.AreEqual(pageSize, storage.PageSize);
                        storage.Validate(null, new CancellationToken(false));

                        for (long i = 0; i < initCapacity; i++)
                            Assert.IsFalse(storage.IsPageAllocated(i));
                    }
                }
            }
        }

        [DataTestMethod]
        [DataRow(0, true, (byte)0x00)]
        [DataRow(1, true, (byte)0x00)]
        [DataRow(1024 * 1024, false, (byte)0x00)]
        [DataRow(0, true, (byte)0xFF)]
        [DataRow(1, true, (byte)0xFF)]
        [DataRow(1024 * 1024, false, (byte)0xFF)]
        [DataRow(0, true, (byte)0xCC)]
        [DataRow(1, true, (byte)0xCC)]
        [DataRow(1024 * 1024, false, (byte)0xCC)]
        public void CreateWillIncrementallResizeIfNecessary(long initStreamSize, bool isIncreasing, byte initPayload)
        {
            using (MemoryStream ms = new MemoryStream())
            {
                using (MockStream stream = new MockStream(ms, true, true, true))
                {
                    stream.SetLength(initStreamSize);

                    //Write the initial payload (to test that Create works with initial garbage payload in the stream)
                    for (long i = 0; i < stream.Length; i++)
                        stream.Write(new byte[] { initPayload }, 0, 1);
                    stream.Flush();

                    long? targetProgress = null;
                    long? previousValue = null;
                    ProgressReporter reporter = new ProgressReporter((x) => {
                        if (targetProgress == null)
                            targetProgress = x.Target;
                        Assert.IsNotNull(targetProgress);

                        Assert.IsTrue(x.Current <= targetProgress.Value);

                        if (previousValue != null)
                            Assert.IsTrue(previousValue.Value <= x.Current);
                        previousValue = x.Current;
                    });

                    const long increment = 64;
                    long previousLength = stream.Length;
                    stream.OnSetLength += (strm, length) => {
                        Assert.AreSame(stream, strm);
                        if (isIncreasing)
                        {
                            Assert.IsTrue(length > previousLength);
                            Assert.IsTrue(length <= previousLength + increment);
                        }
                        else
                        {
                            Assert.IsTrue(length < previousLength);
                            Assert.IsTrue(length >= previousLength - increment);
                        }

                        previousLength = length;
                    };

                    const long initCapacity = 1;
                    const long pageSize = 512;
                    using (StreamingPageStorage storage = StreamingPageStorage.Create(stream, pageSize, initCapacity, reporter, new CancellationToken(false), true, increment))
                    {
                        storage.Validate(null, new CancellationToken(false));
                        Assert.AreEqual(targetProgress.Value, previousValue.Value);
                        if (isIncreasing)
                            Assert.IsTrue(stream.Length > initStreamSize);
                        else
                            Assert.IsTrue(stream.Length < initStreamSize);
                    }
                }
            }
        }

        [TestMethod]
        public void CreateWillNotResizeIfUnnecessary()
        {
            using (MemoryStream ms = new MemoryStream())
            {
                using (MockStream stream = new MockStream(ms, true, true, true))
                {
                    const long capacity = 5;
                    const long pageSize = 1024;
                    long requiredSize = StreamingPageStorage.GetRequiredStreamSize(pageSize, capacity);
                    stream.SetLength(requiredSize);

                    stream.OnSetLength += (strm, length) => {
                        Assert.Fail("Stream.SetLength must not be called for this test.");
                    };

                    using (StreamingPageStorage storage = StreamingPageStorage.Create(stream, pageSize, capacity, null, new CancellationToken(false), true, 1))
                    {
                        Assert.AreEqual(requiredSize, stream.Length);
                    }
                }
            }
        }

        [TestMethod]
        public void CreateFixedThrowsIfStreamIsNull()
        {
            Assert.ThrowsException<ArgumentNullException>(() => {
                StreamingPageStorage storage = StreamingPageStorage.CreateFixed(null, 1024, null, new CancellationToken(false), true);
            });
        }

        [DataTestMethod]
        [DataRow(0)]
        [DataRow(1)]
        [DataRow(StreamingPageStorage.HeaderSize - 1)]
        public void CreateFixedThrowsIfStreamIsTooSmall(long streamSize)
        {
            using (MemoryStream stream = new MemoryStream())
            {
                Assert.ThrowsException<ArgumentException>(() => {
                    StreamingPageStorage storage = StreamingPageStorage.CreateFixed(stream, 1024, null, new CancellationToken(false), true);
                });
            }
        }

        [DataTestMethod]
        [DataRow(0)]
        [DataRow(1)]
        [DataRow(StreamingPageStorage.MinPageSize - 1)]
        public void CreateFixedThrowsIfArgsOutOfRange(long pageSize)
        {
            using (MemoryStream stream = new MemoryStream())
            {
                Assert.ThrowsException<ArgumentOutOfRangeException>(() => {
                    StreamingPageStorage storage = StreamingPageStorage.CreateFixed(stream, pageSize, null, new CancellationToken(false), true);
                });
            }
        }

        [DataTestMethod]
        [DataRow(false, true, true)]
        [DataRow(true, false, true)]
        [DataRow(true, true, false)]
        public void CreateFixedThrowsIfStreamIsInvalid(bool canRead, bool canWrite, bool canSeek)
        {
            using (MemoryStream ms = new MemoryStream())
            {
                using (MockStream stream = new MockStream(ms, canRead, canWrite, canSeek))
                {
                    Assert.ThrowsException<ArgumentException>(() => {
                        StreamingPageStorage storage = StreamingPageStorage.CreateFixed(stream, 1024, null, new CancellationToken(false), true);
                    });
                }
            }
        }

        [TestMethod]
        public void CreateFixedCancellationWorks()
        {
            using (MemoryStream stream = new MemoryStream())
            {
                const long pageSize = 1024;
                stream.SetLength(StreamingPageStorage.GetRequiredStreamSize(pageSize, 64));
                CancellationTokenSource cancellation = new CancellationTokenSource();
                ProgressReporter reporter = new ProgressReporter((x) => {
                    if (x.Current == 10)//Cancel after some progress
                        cancellation.Cancel();
                    else if (x.Current > 10)
                        Assert.Fail("Expected the creation to terminate immediately after cancellation, so no progress should have been reported.");
                });

                Assert.ThrowsException<OperationCanceledException>(() => {
                    StreamingPageStorage storage = StreamingPageStorage.CreateFixed(stream, pageSize, reporter, cancellation.Token, true);
                });
            }
        }

        [TestMethod]
        public void CreateFixedReportsProgress()
        {
            using (MemoryStream stream = new MemoryStream())
            {
                long pageSize = 1024;
                long capacity = 128;
                stream.SetLength(StreamingPageStorage.GetRequiredStreamSize(pageSize, capacity));
                long? targetProgress = null;
                long? previousValue = null;
                ProgressReporter reporter = new ProgressReporter((x) => {
                    if (targetProgress == null)
                        targetProgress = x.Target;
                    Assert.IsNotNull(targetProgress);

                    Assert.IsTrue(x.Current <= targetProgress.Value);

                    if (previousValue != null)
                        Assert.IsTrue(previousValue.Value <= x.Current);
                    previousValue = x.Current;
                });

                using (StreamingPageStorage storage = StreamingPageStorage.CreateFixed(stream, pageSize, reporter, new CancellationToken(false), true))
                {
                    Assert.AreEqual(targetProgress.Value, previousValue.Value);
                }
            }
        }

        [DataTestMethod]
        [DataRow(0, (byte)0x00)]
        [DataRow(1, (byte)0x00)]
        [DataRow(2, (byte)0x00)]
        [DataRow(0, (byte)0xCC)]
        [DataRow(1, (byte)0xCC)]
        [DataRow(2, (byte)0xCC)]
        [DataRow(0, (byte)0xFF)]
        [DataRow(1, (byte)0xFF)]
        [DataRow(2, (byte)0xFF)]
        public void CreateFixedWorks(long extraBytesAtEnd, byte initPayload)
        {
            const long pageSize = 1024;
            const long capacity = 128;
            long requiredSize = StreamingPageStorage.GetRequiredStreamSize(pageSize, capacity);
            using (MemoryStream ms = new MemoryStream())
            {
                ms.SetLength(requiredSize + extraBytesAtEnd);
                using (MockStream stream = new MockStream(ms, true, true, true))
                {
                    //Make sure the CreateFixed method never calls Stream.SetLength
                    stream.OnSetLength += (strm, length) => {
                        Assert.Fail("The " + nameof(StreamingPageStorage.CreateFixed) + " method must not call " + nameof(Stream) + "." + nameof(Stream.SetLength) + ".");
                    };

                    //Write the initial payload (to test that CreateFixed works with initial garbage payload in the stream)
                    for (int i = 0; i < stream.Length; i++)
                        stream.Write(new byte[] { initPayload }, 0, 1);
                    stream.Flush();

                    using (StreamingPageStorage storage = StreamingPageStorage.CreateFixed(stream, pageSize, null, new CancellationToken(false), true))
                    {
                        Assert.IsFalse(storage.IsDisposed);
                        Assert.AreEqual(capacity, storage.PageCapacity);
                        Assert.AreEqual(0, storage.AllocatedPageCount);
                        Assert.IsNull(storage.EntryPageIndex);
                        Assert.IsTrue(storage.IsCapacityFixed);
                        Assert.IsFalse(storage.IsReadOnly);
                        Assert.AreEqual(pageSize, storage.PageSize);
                        Assert.AreEqual(requiredSize + extraBytesAtEnd, stream.Length);
                        storage.Validate(null, new CancellationToken(false));

                        for (long i = 0; i < capacity; i++)
                            Assert.IsFalse(storage.IsPageAllocated(i));
                    }
                }
            }
        }

        [DataTestMethod]
        [DataRow(StreamingPageStorage.MinPageSize, 0)]
        [DataRow(StreamingPageStorage.MinPageSize, 1)]
        [DataRow(StreamingPageStorage.MinPageSize, 64)]

        [DataRow(StreamingPageStorage.MinPageSize + 1, 0)]
        [DataRow(StreamingPageStorage.MinPageSize + 1, 1)]
        [DataRow(StreamingPageStorage.MinPageSize + 1, 64)]

        [DataRow(1024, 0)]
        [DataRow(1024, 1)]
        [DataRow(1024, 64)]
        public void CreateThenLoadWorks(long pageSize, long pageCapacity)
        {
            long requiredSize = StreamingPageStorage.GetRequiredStreamSize(pageSize, pageCapacity);
            using (MemoryStream ms = new MemoryStream())
            {
                using (MockStream stream = new MockStream(ms, true, true, true))
                {
                    //Create the storage
                    StreamingPageStorage.Create(stream, pageSize, pageCapacity, null, new CancellationToken(false), true).Dispose();

                    //Load it
                    using (var storage = StreamingPageStorage.Load(stream, false, false, true))
                    {
                        Assert.IsFalse(storage.IsDisposed);
                        Assert.AreEqual(pageCapacity, storage.PageCapacity);
                        Assert.AreEqual(0, storage.AllocatedPageCount);
                        Assert.IsNull(storage.EntryPageIndex);
                        Assert.IsFalse(storage.IsCapacityFixed);
                        Assert.IsFalse(storage.IsReadOnly);
                        Assert.AreEqual(pageSize, storage.PageSize);
                        Assert.AreEqual(requiredSize, stream.Length);

                        for (long i = 0; i < pageCapacity; i++)
                            Assert.IsFalse(storage.IsPageAllocated(i));
                    }
                }
            }
        }

        [DataTestMethod]
        [DataRow(StreamingPageStorage.MinPageSize, 0, 0)]
        [DataRow(StreamingPageStorage.MinPageSize, 0, 1)]
        [DataRow(StreamingPageStorage.MinPageSize, 1, 0)]
        [DataRow(StreamingPageStorage.MinPageSize, 1, 1)]
        [DataRow(StreamingPageStorage.MinPageSize, 64, 0)]
        [DataRow(StreamingPageStorage.MinPageSize, 64, 1)]

        [DataRow(StreamingPageStorage.MinPageSize + 1, 0, 0)]
        [DataRow(StreamingPageStorage.MinPageSize + 1, 0, 1)]
        [DataRow(StreamingPageStorage.MinPageSize + 1, 1, 0)]
        [DataRow(StreamingPageStorage.MinPageSize + 1, 1, 1)]
        [DataRow(StreamingPageStorage.MinPageSize + 1, 64, 0)]
        [DataRow(StreamingPageStorage.MinPageSize + 1, 64, 1)]

        [DataRow(1024, 0, 0)]
        [DataRow(1024, 0, 1)]
        [DataRow(1024, 1, 0)]
        [DataRow(1024, 1, 1)]
        [DataRow(1024, 64, 0)]
        [DataRow(1024, 64, 1)]
        public void CreateFixedThenLoadWorks(long pageSize, long pageCapacity, long extraBytesAtEnd)
        {
            long requiredSize = StreamingPageStorage.GetRequiredStreamSize(pageSize, pageCapacity);
            using (MemoryStream ms = new MemoryStream())
            {
                using (MockStream stream = new MockStream(ms, true, true, true))
                {
                    stream.SetLength(requiredSize + extraBytesAtEnd);

                    //Create the storage
                    StreamingPageStorage.CreateFixed(stream, pageSize, null, new CancellationToken(false), true).Dispose();

                    //Load it
                    using (var storage = StreamingPageStorage.Load(stream, false, false, true))
                    {
                        Assert.AreEqual(pageCapacity, storage.PageCapacity);
                        Assert.AreEqual(0, storage.AllocatedPageCount);
                        Assert.IsNull(storage.EntryPageIndex);
                        Assert.IsFalse(storage.IsCapacityFixed);
                        Assert.IsFalse(storage.IsReadOnly);
                        Assert.AreEqual(pageSize, storage.PageSize);
                        Assert.AreEqual(requiredSize + extraBytesAtEnd, stream.Length);

                        for (long i = 0; i < pageCapacity; i++)
                            Assert.IsFalse(storage.IsPageAllocated(i));
                    }
                }
            }
        }

        [TestMethod]
        public void LoadThrowsWhenStreamIsNull()
        {
            Assert.ThrowsException<ArgumentNullException>(() => {
                StreamingPageStorage storage = StreamingPageStorage.Load(null, true, true, true);
            });
        }

        [DataTestMethod]
        [DataRow(true, true, false)]//If stream is readonly, capacity must be fixed
        [DataRow(true, false, true)]//If stream is readonly, storage must be readonly
        [DataRow(false, true, false)]//If storage is readonly, capacity must be fixed
        public void LoadThrowsWhenReadOnlyArgsAreInvalid(bool isStreamReadOnly, bool isStorageReadOnly, bool isStorageFixedCapacity)
        {
            using (MemoryStream ms = new MemoryStream())
            {
                StreamingPageStorage.Create(ms, 1024, 64, null, new CancellationToken(false), true, 1024);
                using (MockStream stream = new MockStream(ms, true, !isStreamReadOnly, true))
                {
                    Assert.ThrowsException<ArgumentException>(() => {
                        StreamingPageStorage storage = StreamingPageStorage.Load(stream, isStorageReadOnly, isStorageFixedCapacity, true);
                    });
                }
            }
        }

        [DataTestMethod]
        [DataRow(true, false, 1024)]
        [DataRow(false, true, 1024)]
        [DataRow(true, true, StreamingPageStorage.HeaderSize - 1)]//Stream is too small
        public void LoadThrowsWhenStreamIsInvalid(bool canRead, bool canSeek, long streamSize)
        {
            using (MemoryStream ms = new MemoryStream())
            {
                ms.SetLength(streamSize);
                using (MockStream stream = new MockStream(ms, canRead, false, canSeek))
                {
                    Assert.ThrowsException<ArgumentException>(() => {
                        StreamingPageStorage storage = StreamingPageStorage.Load(stream, true, true, true);
                    });
                }
            }
        }

        [TestMethod]
        public void LoadAsReadOnlyWhenStreamIsWritable()
        {
            using (MemoryStream stream = new MemoryStream())
            {
                const long pageSize = 1024;
                const long capacity = 64;
                StreamingPageStorage.Create(stream, pageSize, capacity, null, new CancellationToken(false), true, 128).Dispose();
                using (StreamingPageStorage storage = StreamingPageStorage.Load(stream, true, true, true))
                {
                    Assert.IsTrue(storage.IsReadOnly);
                    Assert.IsTrue(storage.IsCapacityFixed);
                    Assert.ThrowsException<InvalidOperationException>(() => {
                        storage.EntryPageIndex = 2;
                    });

                    Assert.ThrowsException<InvalidOperationException>(() => {
                        storage.TryAllocatePage(out long ignore);
                    });

                    Assert.ThrowsException<InvalidOperationException>(() => {
                        storage.WriteTo(1, 0, new byte[10], 0, 1);
                    });
                }
            }
        }

        [DataTestMethod]
        [DataRow(StreamingPageStorage.MinPageSize, 1)]
        [DataRow(StreamingPageStorage.MinPageSize, 2)]
        [DataRow(StreamingPageStorage.MinPageSize, 3)]
        [DataRow(StreamingPageStorage.MinPageSize, 64)]
        [DataRow(StreamingPageStorage.MinPageSize + 1, 1)]
        [DataRow(StreamingPageStorage.MinPageSize + 1, 2)]
        [DataRow(StreamingPageStorage.MinPageSize + 1, 3)]
        [DataRow(StreamingPageStorage.MinPageSize + 1, 64)]
        [DataRow(1024, 1)]
        [DataRow(1024, 2)]
        [DataRow(1024, 3)]
        [DataRow(1024, 64)]
        public void AllocateAndFreeWorks(long pageSize, long capacity)
        {
            using (MemoryStream stream = new MemoryStream())
            {
                using (StreamingPageStorage storage = StreamingPageStorage.Create(stream, pageSize, capacity, null, new CancellationToken(false), true, 1024))
                {
                    //Allocate all pages
                    HashSet<long> allocatedPages = new HashSet<long>();
                    for (long i = 0; i < capacity; i++)
                    {
                        Assert.IsTrue(storage.TryAllocatePage(out long index));
                        Assert.IsTrue(storage.IsPageAllocated(index));
                        Assert.IsTrue(allocatedPages.Add(index));
                        Assert.AreEqual(i + 1, storage.AllocatedPageCount);
                    }

                    //Try to allocate another page
                    Assert.IsFalse(storage.TryAllocatePage(out long badIndex));
                    Assert.AreEqual(-1, badIndex);

                    //Ensure that nothing changed after allocation failed
                    Assert.AreEqual(capacity, storage.PageCapacity);
                    Assert.AreEqual(capacity, storage.AllocatedPageCount);
                    storage.Validate(null, new CancellationToken(false));

                    //Free an arbitrary page in the middle
                    long indexToFree = capacity / 2;
                    Assert.IsTrue(storage.FreePage(indexToFree));
                    Assert.IsFalse(storage.IsPageAllocated(indexToFree));

                    //Redundant free call
                    Assert.IsFalse(storage.FreePage(indexToFree));

                    //Ensure that the page was unallocated
                    Assert.AreEqual(capacity - 1, storage.AllocatedPageCount);
                    Assert.AreEqual(capacity, storage.PageCapacity);//Must remain unchanged
                    storage.Validate(null, new CancellationToken(false));

                    //Allocate a new page (expect it to recycle the one we just freed)
                    Assert.IsTrue(storage.TryAllocatePage(out long recycledIndex));
                    Assert.AreEqual(indexToFree, recycledIndex);
                    Assert.IsTrue(storage.IsPageAllocated(recycledIndex));
                    Assert.AreEqual(capacity, storage.AllocatedPageCount);
                    storage.Validate(null, new CancellationToken(false));
                }
            }
        }

        [DataTestMethod]
        [DataRow(StreamingPageStorage.MinPageSize, 1, true)]
        [DataRow(StreamingPageStorage.MinPageSize, 2, true)]
        [DataRow(StreamingPageStorage.MinPageSize, 3, true)]
        [DataRow(StreamingPageStorage.MinPageSize, 64, true)]
        [DataRow(StreamingPageStorage.MinPageSize + 1, 1, true)]
        [DataRow(StreamingPageStorage.MinPageSize + 1, 2, true)]
        [DataRow(StreamingPageStorage.MinPageSize + 1, 3, true)]
        [DataRow(StreamingPageStorage.MinPageSize + 1, 64, true)]
        [DataRow(1024, 1, true)]
        [DataRow(1024, 2, true)]
        [DataRow(1024, 3, true)]
        [DataRow(1024, 64, true)]
        [DataRow(StreamingPageStorage.MinPageSize, 1, false)]
        [DataRow(StreamingPageStorage.MinPageSize, 2, false)]
        [DataRow(StreamingPageStorage.MinPageSize, 3, false)]
        [DataRow(StreamingPageStorage.MinPageSize, 64, false)]
        [DataRow(StreamingPageStorage.MinPageSize + 1, 1, false)]
        [DataRow(StreamingPageStorage.MinPageSize + 1, 2, false)]
        [DataRow(StreamingPageStorage.MinPageSize + 1, 3, false)]
        [DataRow(StreamingPageStorage.MinPageSize + 1, 64, false)]
        [DataRow(1024, 1, false)]
        [DataRow(1024, 2, false)]
        [DataRow(1024, 3, false)]
        [DataRow(1024, 64, false)]
        public void AllocateAndFreeConsecutivelyWorks(long pageSize, long capacity, bool forward)
        {
            using (MemoryStream stream = new MemoryStream())
            {
                using (StreamingPageStorage storage = StreamingPageStorage.Create(stream, pageSize, capacity, null, new CancellationToken(false), true, 1024))
                {
                    //Allocate all pages
                    HashSet<long> allocatedPages = new HashSet<long>();
                    for (long i = 0; i < capacity; i++)
                    {
                        Assert.IsTrue(storage.TryAllocatePage(out long index));
                        Assert.IsTrue(storage.IsPageAllocated(index));
                        Assert.IsTrue(allocatedPages.Add(index));
                        Assert.AreEqual(i + 1, storage.AllocatedPageCount);
                    }

                    //Try to allocate another page
                    Assert.IsFalse(storage.TryAllocatePage(out long badIndex));
                    Assert.AreEqual(-1, badIndex);

                    //Ensure that nothing changed after allocation failed
                    Assert.AreEqual(capacity, storage.PageCapacity);
                    Assert.AreEqual(capacity, storage.AllocatedPageCount);
                    storage.Validate(null, new CancellationToken(false));

                    //Free all pages consecutively
                    for (long i = 0; i < capacity; i++)
                    {
                        long indexToFree = i;
                        if (!forward)
                            indexToFree = capacity - (i + 1);

                        //Free the page
                        Assert.IsTrue(storage.FreePage(indexToFree));
                        Assert.IsFalse(storage.IsPageAllocated(indexToFree));
                        Assert.AreEqual(capacity - (i + 1), storage.AllocatedPageCount);
                    }

                    Assert.AreEqual(0, storage.AllocatedPageCount);
                    Assert.AreEqual(capacity, storage.PageCapacity);
                    storage.Validate(null, new CancellationToken(false));

                    //Recycle the pages that were just freed
                    for (long i = 0; i < capacity; i++)
                    {
                        Assert.IsTrue(storage.TryAllocatePage(out long recycledIndex));
                        Assert.IsTrue(storage.IsPageAllocated(recycledIndex));
                        Assert.AreEqual(i + 1, storage.AllocatedPageCount);
                    }

                    Assert.AreEqual(capacity, storage.AllocatedPageCount);
                    Assert.AreEqual(capacity, storage.PageCapacity);
                    storage.Validate(null, new CancellationToken(false));
                    for (long i = 0; i < capacity; i++)
                        Assert.IsTrue(storage.IsPageAllocated(i));
                }
            }
        }

        [DataTestMethod]
        [DataRow(StreamingPageStorage.MinPageSize, 1)]
        [DataRow(StreamingPageStorage.MinPageSize, 2)]
        [DataRow(StreamingPageStorage.MinPageSize, 3)]
        [DataRow(StreamingPageStorage.MinPageSize, 64)]
        [DataRow(StreamingPageStorage.MinPageSize + 1, 1)]
        [DataRow(StreamingPageStorage.MinPageSize + 1, 2)]
        [DataRow(StreamingPageStorage.MinPageSize + 1, 3)]
        [DataRow(StreamingPageStorage.MinPageSize + 1, 64)]
        [DataRow(1024, 1)]
        [DataRow(1024, 2)]
        [DataRow(1024, 3)]
        [DataRow(1024, 64)]
        public void AllocateAndFreeRandomlyWorks(long pageSize, long capacity)
        {
            using (MemoryStream stream = new MemoryStream())
            {
                using (StreamingPageStorage storage = StreamingPageStorage.Create(stream, pageSize, capacity, null, new CancellationToken(false), true, 1024))
                {
                    //Allocate all pages
                    List<long> allocatedPages = new List<long>();
                    for (long i = 0; i < capacity; i++)
                    {
                        Assert.IsTrue(storage.TryAllocatePage(out long index));
                        Assert.IsTrue(storage.IsPageAllocated(index));
                        Assert.IsFalse(allocatedPages.Contains(index));
                        allocatedPages.Add(index);
                        Assert.AreEqual(i + 1, storage.AllocatedPageCount);
                    }

                    //Try to allocate another page
                    Assert.IsFalse(storage.TryAllocatePage(out long badIndex));
                    Assert.AreEqual(-1, badIndex);

                    //Ensure that nothing changed after allocation failed
                    Assert.AreEqual(capacity, storage.PageCapacity);
                    Assert.AreEqual(capacity, storage.AllocatedPageCount);
                    storage.Validate(null, new CancellationToken(false));

                    //Constant-seeded Random for consistent tests
                    Random r = new Random(5);

                    //Free all pages consecutively
                    for (long i = 0; i < capacity; i++)
                    {
                        long indexToFree = allocatedPages[r.Next() % allocatedPages.Count];

                        //Free the page
                        Assert.IsTrue(storage.FreePage(indexToFree));
                        Assert.IsFalse(storage.IsPageAllocated(indexToFree));
                        Assert.AreEqual(capacity - (i + 1), storage.AllocatedPageCount);
                        allocatedPages.Remove(indexToFree);
                    }

                    Assert.AreEqual(0, storage.AllocatedPageCount);
                    Assert.AreEqual(capacity, storage.PageCapacity);
                    storage.Validate(null, new CancellationToken(false));

                    //Recycle the pages that were just freed
                    for (long i = 0; i < capacity; i++)
                    {
                        Assert.IsTrue(storage.TryAllocatePage(out long recycledIndex));
                        Assert.IsTrue(storage.IsPageAllocated(recycledIndex));
                        Assert.AreEqual(i + 1, storage.AllocatedPageCount);
                    }

                    Assert.AreEqual(capacity, storage.AllocatedPageCount);
                    Assert.AreEqual(capacity, storage.PageCapacity);
                    storage.Validate(null, new CancellationToken(false));
                    for (long i = 0; i < capacity; i++)
                        Assert.IsTrue(storage.IsPageAllocated(i));
                }
            }
        }

        [TestMethod]
        public void AllocateWhenReadOnlyThrows()
        {
            using (MemoryStream stream = new MemoryStream())
            {
                StreamingPageStorage.Create(stream, 1024, 64, null, new CancellationToken(false), true, 1024).Dispose();
                using (StreamingPageStorage storage = StreamingPageStorage.Load(stream, true, true, true))
                {
                    long ignore = 0;
                    Assert.ThrowsException<InvalidOperationException>(() => {
                        storage.TryAllocatePage(out ignore);
                    });
                    storage.Validate(null, new CancellationToken(false));
                    Assert.AreEqual(0, storage.AllocatedPageCount);
                }
            }
        }

        [TestMethod]
        public void FreeUnallocatedPageReturnsFalse()
        {
            using (MemoryStream stream = new MemoryStream())
            {
                long index;
                using (var storage = StreamingPageStorage.Create(stream, 1024, 64, null, new CancellationToken(false), true, 1024))
                {
                    Assert.IsTrue(storage.TryAllocatePage(out index));
                    Assert.IsTrue(storage.FreePage(index));
                }

                using (var storage = StreamingPageStorage.Load(stream, false, false, true))
                {
                    Assert.IsFalse(storage.FreePage(index));
                    Assert.AreEqual(0, storage.AllocatedPageCount);
                }
            }
        }

        [TestMethod]
        public void FreePageWhenReadOnlyThrows()
        {
            using (MemoryStream stream = new MemoryStream())
            {
                long index;
                using (var storage = StreamingPageStorage.Create(stream, 1024, 64, null, new CancellationToken(false), true, 1024))
                {
                    Assert.IsTrue(storage.TryAllocatePage(out index));
                }

                //Reload as readonly
                using (var storage = StreamingPageStorage.Load(stream, true, true, true))
                {
                    Assert.ThrowsException<InvalidOperationException>(() =>
                    {
                        storage.FreePage(index);
                    });
                    Assert.AreEqual(1, storage.AllocatedPageCount);
                    Assert.IsTrue(storage.IsPageAllocated(index));
                }
            }
        }

        [DataTestMethod]
        [DataRow(64, -1)]
        [DataRow(64, -2)]
        [DataRow(64, 64)]
        [DataRow(64, 65)]
        public void FreePageOffStorageThrows(long capacity, long index)
        {
            using (MemoryStream stream = new MemoryStream())
            {
                using (StreamingPageStorage storage = StreamingPageStorage.Create(stream, 1024, 64, null, new CancellationToken(false), true, 1024))
                {
                    Assert.ThrowsException<ArgumentOutOfRangeException>(() =>
                    {
                        storage.FreePage(index);
                    });
                }
            }
        }

        [TestMethod]
        public void WriteThrowsWhenBufferIsNull()
        {
            using (MemoryStream stream = new MemoryStream())
            {
                using (StreamingPageStorage storage = StreamingPageStorage.Create(stream, 1024, 64, null, new CancellationToken(false), true, 1024))
                {
                    Assert.IsTrue(storage.TryAllocatePage(out long index));
                    Assert.ThrowsException<ArgumentNullException>(() => {
                        storage.WriteTo(index, 0, null, 0, 1);
                    });
                }
            }
        }

        [DataTestMethod]
        [DataRow(2048, 1024, -1, 0, 1)]
        [DataRow(2048, 1024, 0, -1, 1)]
        [DataRow(2048, 1024, 0, 0, -1)]
        [DataRow(2048, 1024, 1, 0, 1024)]
        [DataRow(2048, 4096, 0, 1, 2048)]
        [DataRow(2048, 4096, (long)int.MaxValue + 1, 0, 2048)]
        [DataRow(2048, 4096, 0, 0, (long)int.MaxValue + 1)]
        public void WriteThrowsWhenArgsOutOfRange(long pageSize, long bufferSize, long srcOffset, long dstOffset, long length)
        {
            using (MemoryStream stream = new MemoryStream())
            {
                using (StreamingPageStorage storage = StreamingPageStorage.Create(stream, pageSize, 64, null, new CancellationToken(false), true, 1024))
                {
                    Assert.IsTrue(storage.TryAllocatePage(out long index));
                    Assert.ThrowsException<ArgumentOutOfRangeException>(() => {
                        storage.WriteTo(index, dstOffset, new byte[bufferSize], srcOffset, length);
                    });
                }
            }
        }

        [TestMethod]
        public void WriteThrowsWhenPageIsNotAllocated()
        {
            using (MemoryStream stream = new MemoryStream())
            {
                using (StreamingPageStorage storage = StreamingPageStorage.Create(stream, 1024, 64, null, new CancellationToken(false), true, 1024))
                {
                    Assert.IsTrue(storage.TryAllocatePage(out long index));
                    Assert.IsTrue(storage.FreePage(index));
                    Assert.ThrowsException<InvalidOperationException>(() => {
                        storage.WriteTo(index, 0, new byte[1024], 0, 128);
                    });
                }
            }
        }

        [DataTestMethod]
        [DataRow(64, -1)]
        [DataRow(64, -2)]
        [DataRow(64, 64)]
        [DataRow(64, 65)]
        public void WriteThrowsWhenPageIndexIsOffStorage(long capacity, long index)
        {
            using (MemoryStream stream = new MemoryStream())
            {
                using (StreamingPageStorage storage = StreamingPageStorage.Create(stream, 1024, capacity, null, new CancellationToken(false), true, 1024))
                {
                    Assert.ThrowsException<ArgumentOutOfRangeException>(() =>
                    {
                        storage.WriteTo(index, 0, new byte[1024], 0, 128);
                    });
                }
            }
        }

        [TestMethod]
        public void WriteThrowsWhenReadOnly()
        {
            using (MemoryStream stream = new MemoryStream())
            {
                long index;
                using (var storage = StreamingPageStorage.Create(stream, 1024, 64, null, new CancellationToken(false), true, 1024))
                {
                    Assert.IsTrue(storage.TryAllocatePage(out index));
                }

                //Load as read-only
                using (var storage = StreamingPageStorage.Load(stream, true, true, true))
                {
                    Assert.ThrowsException<InvalidOperationException>(() =>
                    {
                        storage.WriteTo(index, 0, new byte[1024], 0, 128);
                    });
                }
            }
        }

        [TestMethod]
        public void ReadThrowsWhenBufferIsNull()
        {
            using (MemoryStream stream = new MemoryStream())
            {
                using (StreamingPageStorage storage = StreamingPageStorage.Create(stream, 1024, 64, null, new CancellationToken(false), true, 1024))
                {
                    Assert.IsTrue(storage.TryAllocatePage(out long index));
                    Assert.ThrowsException<ArgumentNullException>(() => {
                        storage.ReadFrom(index, 0, null, 0, 1);
                    });
                }
            }
        }

        [DataTestMethod]
        [DataRow(2048, 1024, -1, 0, 1)]
        [DataRow(2048, 1024, 0, -1, 1)]
        [DataRow(2048, 1024, 0, 0, -1)]
        [DataRow(2048, 1024, 0, 1, 1024)]
        [DataRow(2048, 4096, 1, 0, 2048)]
        [DataRow(2048, 4096, 0, (long)int.MaxValue + 1, 2048)]
        [DataRow(2048, 4096, 0, 0, (long)int.MaxValue + 1)]
        public void ReadThrowsWhenArgsOutOfRange(long pageSize, long bufferSize, long srcOffset, long dstOffset, long length)
        {
            using (MemoryStream stream = new MemoryStream())
            {
                using (StreamingPageStorage storage = StreamingPageStorage.Create(stream, pageSize, 64, null, new CancellationToken(false), true, 1024))
                {
                    Assert.IsTrue(storage.TryAllocatePage(out long index));
                    Assert.ThrowsException<ArgumentOutOfRangeException>(() => {
                        storage.ReadFrom(index, srcOffset, new byte[bufferSize], dstOffset, length);
                    });
                }
            }
        }

        [TestMethod]
        public void ReadThrowsWhenPageIsNotAllocated()
        {
            using (MemoryStream stream = new MemoryStream())
            {
                using (StreamingPageStorage storage = StreamingPageStorage.Create(stream, 1024, 64, null, new CancellationToken(false), true, 1024))
                {
                    Assert.IsTrue(storage.TryAllocatePage(out long index));
                    Assert.IsTrue(storage.FreePage(index));
                    Assert.ThrowsException<InvalidOperationException>(() => {
                        storage.ReadFrom(index, 0, new byte[1024], 0, 128);
                    });
                }
            }
        }

        [DataTestMethod]
        [DataRow(64, -1)]
        [DataRow(64, -2)]
        [DataRow(64, 64)]
        [DataRow(64, 65)]
        public void ReadThrowsWhenPageIndexIsOffStorage(long capacity, long index)
        {
            using (MemoryStream stream = new MemoryStream())
            {
                using (StreamingPageStorage storage = StreamingPageStorage.Create(stream, 1024, capacity, null, new CancellationToken(false), true, 1024))
                {
                    Assert.ThrowsException<ArgumentOutOfRangeException>(() =>
                    {
                        storage.ReadFrom(index, 0, new byte[1024], 0, 128);
                    });
                }
            }
        }

        [TestMethod]
        public void WriteZeroBytesHasNoEffect()
        {
            using (MemoryStream ms = new MemoryStream())
            {
                const long pageSize = 1024;
                const long capacity = 64;
                using (var storage = StreamingPageStorage.Create(ms, pageSize, capacity, null, new CancellationToken(false), true))
                {
                    Assert.IsTrue(storage.TryAllocatePage(out long index));

                    //Write some actual data
                    byte[] toStore = new byte[pageSize];
                    for (int i = 0; i < toStore.Length; i++)
                        toStore[i] = (byte)i;
                    storage.WriteTo(index, 0, toStore, 0, toStore.Length);

                    //Now write a zero-length data
                    storage.WriteTo(index, 0, new byte[0], 0, 0);

                    //Ensure that nothing changed
                    byte[] read = new byte[pageSize];
                    storage.ReadFrom(index, 0, read, 0, read.Length);

                    for (int i = 0; i < pageSize; i++)
                        Assert.AreEqual(toStore[i], read[i]);
                }
            }
        }

        [TestMethod]
        public void ReadZeroBytesHasNoEffect()
        {
            using (MemoryStream ms = new MemoryStream())
            {
                const long pageSize = 1024;
                const long capacity = 64;
                using (var storage = StreamingPageStorage.Create(ms, pageSize, capacity, null, new CancellationToken(false), true))
                {
                    Assert.IsTrue(storage.TryAllocatePage(out long index));

                    //Write some actual data
                    byte[] toStore = new byte[pageSize];
                    for (int i = 0; i < toStore.Length; i++)
                        toStore[i] = (byte)i;
                    storage.WriteTo(index, 0, toStore, 0, toStore.Length);

                    //Read to a zero-length buffer
                    storage.ReadFrom(index, 0, new byte[0], 0, 0);
                }
            }
        }

        [TestMethod]
        public void WriteReadWorks()
        {
            using (MemoryStream stream = new MemoryStream())
            {
                const long pageSize = 1024;
                const long capacity = 64;
                using (var storage = StreamingPageStorage.Create(stream, pageSize, capacity, null, new CancellationToken(false), true, 1024))
                {
                    //Write the pages
                    for (long i = 0; i < capacity; i++)
                    {
                        Assert.IsTrue(storage.TryAllocatePage(out long index));
                        const long bufferOffset = 5;
                        const long increment = 8;
                        byte[] buffer = new byte[pageSize + bufferOffset];
                        for (int j = 0; j < buffer.Length; j++)
                            buffer[j] = (byte)((i + j) - bufferOffset);

                        for (long j = 0; j < pageSize; j += increment)
                            storage.WriteTo(index, j, buffer, j + bufferOffset, increment);
                    }

                    //Read the pages
                    for (long i = 0; i < capacity; i++)
                    {
                        const long bufferOffset = 7;
                        const long increment = 2;
                        byte[] buffer = new byte[pageSize + bufferOffset];
                        for (long j = 0; j < pageSize; j += increment)
                            storage.ReadFrom(i, j, buffer, j + bufferOffset, increment);

                        for (long j = 0; j < pageSize; j++)
                            Assert.AreEqual((byte)(j + i), buffer[j + bufferOffset]);
                    }
                }

                //Reload the storage
                using (var storage = StreamingPageStorage.Load(stream, true, true, true))
                {
                    //Read the pages
                    for (long i = 0; i < capacity; i++)
                    {
                        const long bufferOffset = 7;
                        const long increment = 2;
                        byte[] buffer = new byte[pageSize + bufferOffset];
                        for (long j = 0; j < pageSize; j += increment)
                            storage.ReadFrom(i, j, buffer, j + bufferOffset, increment);

                        for (long j = 0; j < pageSize; j++)
                            Assert.AreEqual((byte)(j + i), buffer[j + bufferOffset]);
                    }

                    storage.Validate(null, new CancellationToken(false));
                }
            }
        }

        [DataTestMethod]
        [DataRow(1024, -1)]
        [DataRow(1024, -2)]
        [DataRow(1024, 1024)]
        [DataRow(1024, 1025)]
        public void IsPageAllocatedReturnsFalseForInvalidIndex(long capacity, long index)
        {
            using (MemoryStream stream = new MemoryStream())
            {
                const long pageSize = 1024;
                long requiredSize = StreamingPageStorage.GetRequiredStreamSize(pageSize, capacity);
                using (StreamingPageStorage storage = StreamingPageStorage.Create(stream, pageSize, capacity, null, new CancellationToken(false), true, 1024))
                {
                    for (long i = 0; i < capacity; i++)
                        Assert.IsTrue(storage.TryAllocatePage(out long ignore));

                    Assert.IsFalse(storage.IsPageAllocated(index));
                }
            }
        }

        [TestMethod]
        public void IsPageOnStreamWorks()
        {
            using (MemoryStream stream = new MemoryStream())
            {
                const long capacity = 64;
                const long pageSize = 1024;
                using (StreamingPageStorage storage = StreamingPageStorage.Create(stream, pageSize, capacity, null, new CancellationToken(false), true, 1024))
                {
                    for (long i = 0; i < capacity; i++)
                        Assert.IsTrue(storage.IsPageOnStorage(i));

                    Assert.IsFalse(storage.IsPageOnStorage(-1));
                    Assert.IsFalse(storage.IsPageOnStorage(-2));
                    Assert.IsFalse(storage.IsPageOnStorage(capacity));
                    Assert.IsFalse(storage.IsPageOnStorage(capacity + 1));
                }
            }
        }

        [TestMethod]
        public void EntryPageSetThrowsExceptionWhenReadOnly()
        {
            using (MemoryStream stream = new MemoryStream())
            {
                const long capacity = 64;
                const long pageSize = 1024;
                using (var storage = StreamingPageStorage.Create(stream, pageSize, capacity, null, new CancellationToken(false), true, 1024))
                {
                    storage.EntryPageIndex = 3;
                }

                //Load as read-only
                using (var storage = StreamingPageStorage.Load(stream, true, true, true))
                {
                    Assert.AreEqual(3, storage.EntryPageIndex);

                    //Attempt to assign a value
                    Assert.ThrowsException<InvalidOperationException>(() => {
                        storage.EntryPageIndex = 5;
                    });

                    //Ensure that it remains at the original value
                    Assert.AreEqual(3, storage.EntryPageIndex);
                }
            }
        }

        [TestMethod]
        public void EntryPageSetThrowsExceptionWhenInvalid()
        {
            using (MemoryStream stream = new MemoryStream())
            {
                const long capacity = 64;
                const long pageSize = 1024;
                using (var storage = StreamingPageStorage.Create(stream, pageSize, capacity, null, new CancellationToken(false), true, 1024))
                {
                    storage.EntryPageIndex = 3;

                    //Attempt to assign an invalid value
                    Assert.ThrowsException<ArgumentOutOfRangeException>(() => {
                        storage.EntryPageIndex = -1;
                    });

                    //Ensure that it remains at the original value
                    Assert.AreEqual(3, storage.EntryPageIndex);
                }

                //Reload and ensure that it is still the original value
                using (var storage = StreamingPageStorage.Load(stream, true, true, true))
                {
                    Assert.AreEqual(3, storage.EntryPageIndex);
                }
            }
        }

        [TestMethod]
        public void EntryPageWorks()
        {
            using (MemoryStream stream = new MemoryStream())
            {
                const long capacity = 64;
                const long pageSize = 1024;
                using (var storage = StreamingPageStorage.Create(stream, pageSize, capacity, null, new CancellationToken(false), true, 1024))
                {
                    Assert.IsNull(storage.EntryPageIndex);

                    storage.EntryPageIndex = 0;
                    Assert.AreEqual(0, storage.EntryPageIndex);

                    storage.EntryPageIndex = null;
                    Assert.IsNull(storage.EntryPageIndex);

                    storage.EntryPageIndex = 12345;
                    Assert.AreEqual(12345, storage.EntryPageIndex);

                    storage.EntryPageIndex = long.MaxValue;
                    Assert.AreEqual(long.MaxValue, storage.EntryPageIndex);

                    storage.EntryPageIndex = 1;
                    Assert.AreEqual(1, storage.EntryPageIndex);
                }

                //Re-load
                using (var storage = StreamingPageStorage.Load(stream, false, true, true))
                {
                    Assert.AreEqual(1, storage.EntryPageIndex);
                }

                //Re-load
                using (var storage = StreamingPageStorage.Load(stream, false, true, true))
                {
                    storage.EntryPageIndex = 4;
                    Assert.AreEqual(4, storage.EntryPageIndex);
                }
            }
        }

        [TestMethod]
        public void InflateThrowsWhenFixed()
        {
            using (MemoryStream ms = new MemoryStream())
            {
                using (MockStream stream = new MockStream(ms, true, true, true))
                {
                    const long pageSize = 1024;
                    const long capacity = 64;
                    StreamingPageStorage.Create(stream, pageSize, capacity, null, new CancellationToken(false), true, 1024).Dispose();
                    using (StreamingPageStorage storage = StreamingPageStorage.Load(stream, false, true, true))
                    {
                        stream.OnWrite += (strm, buf, off, len) =>
                        {
                            Assert.Fail("The Stream.Write method must not be called.");
                        };

                        stream.OnSetLength += (strm, len) =>
                        {
                            Assert.Fail("The Stream.SetLength method must not be called.");
                        };

                        Assert.ThrowsException<InvalidOperationException>(() =>
                        {
                            storage.TryInflate(1, null, new CancellationToken(false));
                        });

                        Assert.AreEqual(capacity, storage.PageCapacity);
                    }
                }
            }
        }

        [TestMethod]
        public void InflateThrowsWhenPageCountIsNegative()
        {
            using (MemoryStream ms = new MemoryStream())
            {
                using (MockStream stream = new MockStream(ms, true, true, true))
                {
                    const long pageSize = 1024;
                    const long capacity = 64;
                    using (StreamingPageStorage storage = StreamingPageStorage.Create(stream, pageSize, capacity, null, new CancellationToken(false), true, 1024))
                    {
                        stream.OnWrite += (strm, buf, off, len) =>
                        {
                            Assert.Fail("The Stream.Write method must not be called.");
                        };

                        stream.OnSetLength += (strm, len) =>
                        {
                            Assert.Fail("The Stream.SetLength method must not be called.");
                        };

                        Assert.ThrowsException<ArgumentOutOfRangeException>(() =>
                        {
                            storage.TryInflate(-1, null, new CancellationToken(false));
                        });

                        Assert.AreEqual(capacity, storage.PageCapacity);
                    }
                }
            }
        }

        [TestMethod]
        public void InflateReportsProgress()
        {
            using (MemoryStream stream = new MemoryStream())
            {
                const long pageSize = 1024;
                const long capacity = 64;
                using (StreamingPageStorage storage = StreamingPageStorage.Create(stream, pageSize, capacity, null, new CancellationToken(false), true, 1024))
                {
                    long? target = null;
                    long? previousProgress = null;
                    ProgressReporter reporter = new ProgressReporter((x) => {

                        if (target == null)
                            target = x.Target;
                        Assert.IsNotNull(target);

                        Assert.IsTrue(x.Current <= target.Value);
                        if (previousProgress != null)
                            Assert.IsTrue(x.Current >= previousProgress.Value);

                        previousProgress = x.Current;
                    });

                    const long desiredAmount = 16;
                    long gotAmount = storage.TryInflate(desiredAmount, reporter, new CancellationToken(false));
                    Assert.AreEqual(desiredAmount, gotAmount);
                    Assert.IsNotNull(target);
                    Assert.AreEqual(target, previousProgress);
                }
            }
        }

        [TestMethod]
        public void InflateCancellationWorks()
        {
            using (MemoryStream stream = new MemoryStream())
            {
                const long pageSize = 1024;
                const long initialCapacity = 64;
                using (StreamingPageStorage storage = StreamingPageStorage.Create(stream, pageSize, initialCapacity, null, new CancellationToken(false), true, 1024))
                {
                    CancellationTokenSource cancellation = new CancellationTokenSource();

                    long expectedAmount = -1;
                    ProgressReporter reporter = new ProgressReporter((x) => {
                        if (x.Current > 8)
                            cancellation.Cancel();

                        expectedAmount = StreamingPageStorage.GetPageCapacityForStreamSize(stream.Length, pageSize) - initialCapacity;
                    });

                    const long desiredAmount = 16;
                    long gotAmount = storage.TryInflate(desiredAmount, reporter, cancellation.Token);
                    Assert.AreNotEqual(desiredAmount, gotAmount);//Make sure it did indeed cancel early
                    Assert.AreEqual(expectedAmount, gotAmount);
                }
            }
        }

        private static void WriteTestPages(StreamingPageStorage storage, IEnumerable<long> indices)
        {
            foreach (var index in indices)
            {
                byte[] buffer = new byte[storage.PageSize];
                for (long i = 0; i < buffer.Length; i++)
                    buffer[i] = (byte)(i + index);
                storage.WriteTo(index, 0, buffer, 0, buffer.Length);
            }
        }

        private static void ReadAndVerifyTestPages(StreamingPageStorage storage, IEnumerable<long> indices)
        {
            foreach (var index in indices)
            {
                byte[] buffer = new byte[storage.PageSize];
                storage.ReadFrom(index, 0, buffer, 0, buffer.Length);

                for (long i = 0; i < buffer.Length; i++)
                    Assert.AreEqual((byte)(i + index), buffer[i]);
            }
        }

        [TestMethod]
        public void ReadAndVerifyTestPagesWorks()
        {
            using (MemoryStream stream = new MemoryStream())
            {
                using (StreamingPageStorage storage = StreamingPageStorage.Create(stream, 1024, 64, null, new CancellationToken(false), true, 1024))
                {
                    HashSet<long> indices = new HashSet<long>();
                    for (long i = 0; i < storage.PageCapacity; i++)
                    {
                        Assert.IsTrue(storage.TryAllocatePage(out long index));
                        Assert.IsTrue(indices.Add(index));
                    }

                    //Make sure write-then-read works successfully
                    WriteTestPages(storage, indices);
                    ReadAndVerifyTestPages(storage, indices);

                    //Corrupt just one page
                    storage.WriteTo(1, 50, new byte[] { 1, 3, 2, 4 }, 0, 4);

                    Assert.ThrowsException<AssertFailedException>(() => {
                        ReadAndVerifyTestPages(storage, indices);
                    });
                }
            }
        }

        [DataTestMethod]
        [DataRow(0, true, 0)]
        [DataRow(0, true, 1)]
        [DataRow(0, true, 2)]
        [DataRow(0, true, 3)]
        [DataRow(0, true, 128)]
        [DataRow(0, false, 0)]
        [DataRow(0, false, 1)]
        [DataRow(0, false, 2)]
        [DataRow(0, false, 3)]
        [DataRow(0, false, 128)]
        [DataRow(1, true, 0)]
        [DataRow(1, true, 1)]
        [DataRow(1, true, 2)]
        [DataRow(1, true, 3)]
        [DataRow(1, true, 128)]
        [DataRow(1, false, 0)]
        [DataRow(1, false, 1)]
        [DataRow(1, false, 2)]
        [DataRow(1, false, 3)]
        [DataRow(1, false, 128)]
        [DataRow(10, true, 0)]
        [DataRow(10, true, 1)]
        [DataRow(10, true, 2)]
        [DataRow(10, true, 3)]
        [DataRow(10, true, 128)]
        [DataRow(10, false, 0)]
        [DataRow(10, false, 1)]
        [DataRow(10, false, 2)]
        [DataRow(10, false, 3)]
        [DataRow(10, false, 128)]
        public void InflateWorks(long initCapacity, bool useISafeResizable, long amount)
        {
            using (MemoryStream ms = new MemoryStream())
            {
                const long pageSize = 1024;
                StreamingPageStorage.Create(ms, pageSize, initCapacity, null, new CancellationToken(false), true, 1024).Dispose();

                Stream stream;
                if (useISafeResizable)
                {
                    var safeStream = new MockSafeResizableStorage(ms, true, true, true, null, long.MaxValue);
                    safeStream.OnSetLength += (strm, len) => {
                        Assert.Fail("When the " + nameof(ISafeResizable) + " interface is used, the " + nameof(Stream.SetLength) + " method must not be called.");
                    };
                    stream = safeStream;
                }
                else
                {
                    stream = new MockStream(ms, true, true, true);
                }

                using (stream)
                {
                    HashSet<long> storedIndices = new HashSet<long>();

                    using (var storage = StreamingPageStorage.Load(stream, false, false, true))
                    {
                        //Allocate all pages in the initial capacity
                        for (long i = 0; i < initCapacity; i++)
                        {
                            Assert.IsTrue(storage.TryAllocatePage(out long index));
                            Assert.IsTrue(storedIndices.Add(index));
                        }

                        //Deallocate the first 1/3 of those indices
                        for (long i = 0; i < initCapacity / 3; i++)
                        {
                            Assert.IsTrue(storage.FreePage(i));
                            Assert.IsTrue(storedIndices.Remove(i));
                        }

                        //Write some test data to the allocated pages
                        WriteTestPages(storage, storedIndices);

                        long got = storage.TryInflate(amount, null, new CancellationToken(false));
                        Assert.AreEqual(amount, got);

                        Assert.AreEqual(initCapacity + amount, storage.PageCapacity);
                        Assert.AreEqual(storedIndices.Count, storage.AllocatedPageCount);
                        storage.Validate(null, new CancellationToken(false));

                        //Make sure all the new pages are set to 'unallocated'
                        for (long i = 0; i < amount; i++)
                            Assert.IsFalse(storage.IsPageAllocated(initCapacity + i));

                        //Make sure the data we wrote before inflation is still stored
                        ReadAndVerifyTestPages(storage, storedIndices);
                    }

                    //Reload the storage
                    using (var storage = StreamingPageStorage.Load(stream, false, true, true))
                    {
                        Assert.AreEqual(initCapacity + amount, storage.PageCapacity);
                        Assert.AreEqual(storedIndices.Count, storage.AllocatedPageCount);
                        storage.Validate(null, new CancellationToken(false));

                        //Make sure all the new pages are set to 'unallocated'
                        for (long i = 0; i < amount; i++)
                            Assert.IsFalse(storage.IsPageAllocated(initCapacity + i));

                        //Make sure the data we wrote before inflation is still stored
                        ReadAndVerifyTestPages(storage, storedIndices);

                        //Make sure we can allocate the new pages
                        for (long i = 0; i < amount; i++)
                        {
                            Assert.IsTrue(storage.TryAllocatePage(out long index));
                            Assert.IsTrue(storedIndices.Add(index));
                        }

                        storage.Validate(null, new CancellationToken(false));
                        WriteTestPages(storage, storedIndices);
                        ReadAndVerifyTestPages(storage, storedIndices);
                        storage.Validate(null, new CancellationToken(false));
                    }
                }
            }
        }

        [DataTestMethod]
        [DataRow(5, 5, 1)]
        [DataRow(5, 5, 2)]
        [DataRow(5, 5, 100)]
        [DataRow(5, 4, 2)]
        [DataRow(5, 4, 100)]
        public void InflateSafeCapsToLimit(long limit, long initCapacity, long inflateAmount)
        {
            Assert.IsTrue(limit <= initCapacity + inflateAmount);
            long maxAmount = limit - (initCapacity);

            using (MemoryStream ms = new MemoryStream())
            {
                const long pageSize = 1024;
                StreamingPageStorage.Create(ms, pageSize, initCapacity, null, new CancellationToken(false), true, 1024);

                using (MockSafeResizableStorage stream = new MockSafeResizableStorage(ms, true, true, true, null, StreamingPageStorage.GetRequiredStreamSize(pageSize, limit)))
                {
                    HashSet<long> storedIndices = new HashSet<long>();

                    using (StreamingPageStorage storage = StreamingPageStorage.Load(stream, false, false, true))
                    {
                        //Allocate all pages in the initial capacity
                        for (long i = 0; i < initCapacity; i++)
                        {
                            Assert.IsTrue(storage.TryAllocatePage(out long index));
                            Assert.IsTrue(storedIndices.Add(index));
                        }

                        //Deallocate the first 1/3 of those indices
                        for (long i = 0; i < initCapacity / 3; i++)
                        {
                            Assert.IsTrue(storage.FreePage(i));
                            Assert.IsTrue(storedIndices.Remove(i));
                        }

                        //Write some test data to the allocated pages
                        WriteTestPages(storage, storedIndices);

                        long got = storage.TryInflate(inflateAmount, null, new CancellationToken(false));
                        Assert.AreNotEqual(maxAmount, inflateAmount);//Expect it to have been limited to the max
                        Assert.AreEqual(maxAmount, got);

                        Assert.AreEqual(initCapacity + maxAmount, storage.PageCapacity);
                        Assert.AreEqual(storedIndices.Count, storage.AllocatedPageCount);
                        storage.Validate(null, new CancellationToken(false));

                        //Make sure all the new pages are set to 'unallocated'
                        for (long i = 0; i < maxAmount; i++)
                            Assert.IsFalse(storage.IsPageAllocated(initCapacity + i));

                        //Make sure the data we wrote before inflation is still stored
                        ReadAndVerifyTestPages(storage, storedIndices);
                    }
                    
                    //Reload the storage
                    using (var storage = StreamingPageStorage.Load(stream, false, true, true))
                    {
                        //Make sure we can allocate the new pages
                        for (long i = 0; i < maxAmount; i++)
                        {
                            Assert.IsTrue(storage.TryAllocatePage(out long index));
                            Assert.IsTrue(storedIndices.Add(index));
                        }

                        storage.Validate(null, new CancellationToken(false));
                        WriteTestPages(storage, storedIndices);
                        ReadAndVerifyTestPages(storage, storedIndices);
                        storage.Validate(null, new CancellationToken(false));
                    }
                }
            }
        }

        [TestMethod]
        public void DeflateThrowsWhenFixed()
        {
            using (MemoryStream ms = new MemoryStream())
            {
                using (MockStream stream = new MockStream(ms, true, true, true))
                {
                    const long pageSize = 1024;
                    const long capacity = 64;
                    StreamingPageStorage.Create(stream, pageSize, capacity, null, new CancellationToken(false), true, 1024).Dispose();
                    using (StreamingPageStorage storage = StreamingPageStorage.Load(stream, false, true, true))
                    {
                        stream.OnWrite += (strm, buf, off, len) =>
                        {
                            Assert.Fail("The Stream.Write method must not be called.");
                        };

                        stream.OnSetLength += (strm, len) =>
                        {
                            Assert.Fail("The Stream.SetLength method must not be called.");
                        };

                        Assert.ThrowsException<InvalidOperationException>(() =>
                        {
                            storage.TryDeflate(1, null, new CancellationToken(false));
                        });

                        Assert.AreEqual(capacity, storage.PageCapacity);
                    }
                }
            }
        }

        [DataTestMethod]
        [DataRow(-1)]
        [DataRow(-2)]
        public void DeflateThrowsWhenPageCountIsNegative(long amount)
        {
            using (MemoryStream ms = new MemoryStream())
            {
                using (MockStream stream = new MockStream(ms, true, true, true))
                {
                    const long pageSize = 1024;
                    const long capacity = 64;
                    StreamingPageStorage.Create(stream, pageSize, capacity, null, new CancellationToken(false), true, 1024).Dispose();
                    using (StreamingPageStorage storage = StreamingPageStorage.Load(stream, false, false, true))
                    {
                        stream.OnWrite += (strm, buf, off, len) =>
                        {
                            Assert.Fail("The Stream.Write method must not be called.");
                        };

                        stream.OnSetLength += (strm, len) =>
                        {
                            Assert.Fail("The Stream.SetLength method must not be called.");
                        };

                        Assert.ThrowsException<ArgumentOutOfRangeException>(() =>
                        {
                            storage.TryDeflate(amount, null, new CancellationToken(false));
                        });

                        Assert.AreEqual(capacity, storage.PageCapacity);
                    }
                }
            }
        }

        [TestMethod]
        public void DeflateReportsProgress()
        {
            using (MemoryStream stream = new MemoryStream())
            {
                const long pageSize = 1024;
                const long capacity = 64;
                using (StreamingPageStorage storage = StreamingPageStorage.Create(stream, pageSize, capacity, null, new CancellationToken(false), true, 1024))
                {
                    long? target = null;
                    long? previousProgress = null;
                    ProgressReporter reporter = new ProgressReporter((x) => {

                        if (target == null)
                            target = x.Target;
                        Assert.IsNotNull(target);

                        Assert.IsTrue(x.Current <= target.Value);
                        if (previousProgress != null)
                            Assert.IsTrue(x.Current >= previousProgress.Value);

                        previousProgress = x.Current;
                    });

                    const long desiredAmount = 16;
                    long gotAmount = storage.TryDeflate(desiredAmount, reporter, new CancellationToken(false));
                    Assert.AreEqual(desiredAmount, gotAmount);
                    Assert.IsNotNull(target);
                    Assert.AreEqual(target, previousProgress);
                }
            }
        }

        [TestMethod]
        public void DeflateCancellationWorks()
        {
            using (MemoryStream ms = new MemoryStream())
            {
                const long pageSize = 1024;
                const long initCapacity = 64;
                const long amount = 32;
                StreamingPageStorage.Create(ms, pageSize, initCapacity, null, new CancellationToken(false), true, 1024).Dispose();

                using (var stream = new MockSafeResizableStorage(ms, true, true, true, null, long.MaxValue))
                {
                    long got;
                    HashSet<long> storedIndices = new HashSet<long>();
                    using (var storage = StreamingPageStorage.Load(stream, false, false, true))
                    {
                        //Allocate all pages in the 'keep' portion
                        long keepSize = initCapacity - amount;
                        for (long i = 0; i < keepSize; i++)
                        {
                            Assert.IsTrue(storage.TryAllocatePage(out long index));
                            Assert.IsTrue(storedIndices.Add(index));
                        }

                        //Deallocate the first 1/3 of those indices
                        for (long i = 0; i < keepSize / 3; i++)
                        {
                            Assert.IsTrue(storage.FreePage(i));
                            Assert.IsTrue(storedIndices.Remove(i));
                        }

                        //Write some test data to the allocated pages
                        WriteTestPages(storage, storedIndices);

                        CancellationTokenSource cancellation = new CancellationTokenSource();
                        long expectedAmount = -1;
                        ProgressReporter reporter = new ProgressReporter((x) => {
                            if (x.Current == 7)
                            {
                                cancellation.Cancel();
                                expectedAmount = initCapacity - StreamingPageStorage.GetPageCapacityForStreamSize(stream.Length, pageSize);
                            }
                        });

                        got = storage.TryDeflate(amount, reporter, cancellation.Token);
                        Assert.AreNotEqual(-1, expectedAmount);
                        Assert.AreNotEqual(amount, got);//Expect cancellation to have stopped us before the desired amount
                        Assert.AreEqual(expectedAmount, got);

                        Assert.AreEqual(initCapacity - got, storage.PageCapacity);
                        Assert.AreEqual(storedIndices.Count, storage.AllocatedPageCount);
                        storage.Validate(null, new CancellationToken(false));

                        //Make sure all the new pages are set to 'unallocated'
                        for (long i = 0; i < amount; i++)
                            Assert.IsFalse(storage.IsPageAllocated(initCapacity - (i + 1)));

                        //Make sure the data we wrote before inflation is still stored
                        ReadAndVerifyTestPages(storage, storedIndices);
                    }

                    //Reload the storage
                    using (var storage = StreamingPageStorage.Load(stream, true, true, true))
                    {
                        Assert.AreEqual(initCapacity - got, storage.PageCapacity);
                        Assert.AreEqual(storedIndices.Count, storage.AllocatedPageCount);
                        storage.Validate(null, new CancellationToken(false));

                        //Make sure all the new pages are set to 'unallocated'
                        for (long i = 0; i < amount; i++)
                            Assert.IsFalse(storage.IsPageAllocated(initCapacity - (i + 1)));

                        //Make sure the data we wrote before inflation is still stored
                        ReadAndVerifyTestPages(storage, storedIndices);
                    }
                }
            }
        }

        [DataTestMethod]
        [DataRow(0, true, 0, 0)]
        [DataRow(0, true, 1, 0)]
        [DataRow(0, true, 256, 0)]
        [DataRow(1, true, 1, 1)]
        [DataRow(1, true, 0, 0)]
        [DataRow(1, true, 2, 1)]
        [DataRow(1, true, 3, 1)]
        [DataRow(1, true, 256, 1)]
        [DataRow(256, true, 1024, 256)]
        [DataRow(256, true, 0, 0)]
        [DataRow(256, true, 1, 1)]
        [DataRow(256, true, 2, 2)]
        [DataRow(256, true, 3, 3)]
        [DataRow(256, true, 128, 128)]
        [DataRow(0, false, 0, 0)]
        [DataRow(0, false, 1, 0)]
        [DataRow(0, false, 256, 0)]
        [DataRow(1, false, 0, 0)]
        [DataRow(1, false, 1, 1)]
        [DataRow(1, false, 2, 1)]
        [DataRow(1, false, 3, 1)]
        [DataRow(1, false, 256, 1)]
        [DataRow(256, false, 1024, 256)]
        [DataRow(256, false, 0, 0)]
        [DataRow(256, false, 1, 1)]
        [DataRow(256, false, 2, 2)]
        [DataRow(256, false, 3, 3)]
        [DataRow(256, false, 128, 128)]
        public void DeflateWorks(long initCapacity, bool useISafeResizable, long requestAmount, long expectedActualAmountRemoved)
        {
            using (MemoryStream ms = new MemoryStream())
            {
                const long pageSize = 1024;
                StreamingPageStorage.Create(ms, pageSize, initCapacity, null, new CancellationToken(false), true, 1024);

                Stream stream;
                if (useISafeResizable)
                {
                    var safeStream = new MockSafeResizableStorage(ms, true, true, true, null, long.MaxValue);
                    safeStream.OnSetLength += (strm, len) => {
                        Assert.Fail("When the " + nameof(ISafeResizable) + " interface is used, the " + nameof(Stream.SetLength) + " method must not be called.");
                    };
                    stream = safeStream;
                }
                else
                {
                    stream = new MockStream(ms, true, true, true);
                }

                using (stream)
                {
                    HashSet<long> storedIndices = new HashSet<long>();

                    using (var storage = StreamingPageStorage.Load(stream, false, false, true))
                    {
                        //Allocate all pages in the 'keep' portion
                        long keepSize = initCapacity - requestAmount;
                        for (long i = 0; i < keepSize; i++)
                        {
                            Assert.IsTrue(storage.TryAllocatePage(out long index));
                            Assert.IsTrue(storedIndices.Add(index));
                        }

                        //Deallocate the first 1/3 of those indices
                        for (long i = 0; i < keepSize / 3; i++)
                        {
                            Assert.IsTrue(storage.FreePage(i));
                            Assert.IsTrue(storedIndices.Remove(i));
                        }

                        //Write some test data to the allocated pages
                        WriteTestPages(storage, storedIndices);

                        long got = storage.TryDeflate(requestAmount, null, new CancellationToken(false));
                        Assert.AreEqual(expectedActualAmountRemoved, got);

                        Assert.AreEqual(initCapacity - expectedActualAmountRemoved, storage.PageCapacity);
                        Assert.AreEqual(storedIndices.Count, storage.AllocatedPageCount);
                        storage.Validate(null, new CancellationToken(false));

                        //Make sure all the new pages are set to 'unallocated'
                        for (long i = 0; i < requestAmount; i++)
                            Assert.IsFalse(storage.IsPageAllocated(initCapacity - (i + 1)));

                        //Make sure the data we wrote before inflation is still stored
                        ReadAndVerifyTestPages(storage, storedIndices);
                    }

                    //Reload the storage
                    using (var storage = StreamingPageStorage.Load(stream, true, true, true))
                    {
                        Assert.AreEqual(initCapacity - expectedActualAmountRemoved, storage.PageCapacity);
                        Assert.AreEqual(storedIndices.Count, storage.AllocatedPageCount);
                        storage.Validate(null, new CancellationToken(false));

                        //Make sure all the new pages are set to 'unallocated'
                        for (long i = 0; i < requestAmount; i++)
                            Assert.IsFalse(storage.IsPageAllocated(initCapacity - (i + 1)));

                        //Make sure the data we wrote before inflation is still stored
                        ReadAndVerifyTestPages(storage, storedIndices);
                    }
                }
            }
        }

        [DataTestMethod]
        [DataRow(true, 16, 15, 100)]
        [DataRow(true, 16, 15, 1)]
        [DataRow(true, 16, 10, 6)]
        [DataRow(true, 16, 10, 1024)]
        public void DeflateStopsAtLimit(bool useISafeResizable, long initCapacity, long lastAllocatedIndex, long deflateRequestAmount)
        {
            Assert.IsTrue(initCapacity > lastAllocatedIndex);
            long maxDeflateAmount = initCapacity - (lastAllocatedIndex + 1);
            long expectedDeflateAmount = maxDeflateAmount;
            if (deflateRequestAmount < expectedDeflateAmount)
                expectedDeflateAmount = deflateRequestAmount;

            using (MemoryStream ms = new MemoryStream())
            {
                const long pageSize = 1024;
                StreamingPageStorage.Create(ms, pageSize, initCapacity, null, new CancellationToken(false), true, 1024).Dispose();

                Stream stream;
                if (useISafeResizable)
                {
                    var safeStream = new MockSafeResizableStorage(ms, true, true, true, null, long.MaxValue);
                    safeStream.OnSetLength += (strm, len) => {
                        Assert.Fail("When the " + nameof(ISafeResizable) + " interface is used, the " + nameof(Stream.SetLength) + " method must not be called.");
                    };
                    stream = safeStream;
                }
                else
                {
                    stream = new MockStream(ms, true, true, true);
                }

                using (stream)
                {
                    HashSet<long> storedIndices = new HashSet<long>();
                    using (var storage = StreamingPageStorage.Load(stream, false, false, true))
                    {
                        //Allocate all pages in the 'keep' portion
                        long keepSize = lastAllocatedIndex + 1;
                        for (long i = 0; i < keepSize; i++)
                        {
                            Assert.IsTrue(storage.TryAllocatePage(out long index));
                            Assert.IsTrue(storedIndices.Add(index));
                        }

                        //Deallocate the first 1/3 of those indices
                        for (long i = 0; i < keepSize / 3; i++)
                        {
                            Assert.IsTrue(storage.FreePage(i));
                            Assert.IsTrue(storedIndices.Remove(i));
                        }

                        //Make sure 'lastAllocatedIndex' is indeed allocated
                        Assert.IsTrue(storage.IsPageAllocated(lastAllocatedIndex));

                        //Write some test data to the allocated pages
                        WriteTestPages(storage, storedIndices);

                        long got = storage.TryDeflate(deflateRequestAmount, null, new CancellationToken(false));
                        Assert.AreNotEqual(deflateRequestAmount, got);
                        Assert.AreEqual(expectedDeflateAmount, got);
                        Assert.IsTrue(lastAllocatedIndex == storage.PageCapacity - 1);

                        Assert.AreEqual(initCapacity - expectedDeflateAmount, storage.PageCapacity);
                        Assert.AreEqual(storedIndices.Count, storage.AllocatedPageCount);
                        storage.Validate(null, new CancellationToken(false));

                        //Make sure all the new pages are set to 'unallocated'
                        for (long i = 0; i < expectedDeflateAmount; i++)
                            Assert.IsFalse(storage.IsPageAllocated(initCapacity - (i + 1)));

                        //Make sure the data we wrote before inflation is still stored
                        ReadAndVerifyTestPages(storage, storedIndices);
                    }

                    //Reload the storage
                    using (var storage = StreamingPageStorage.Load(stream, true, true, true))
                    {
                        Assert.AreEqual(initCapacity - expectedDeflateAmount, storage.PageCapacity);
                        Assert.AreEqual(storedIndices.Count, storage.AllocatedPageCount);
                        storage.Validate(null, new CancellationToken(false));

                        //Make sure all the new pages are set to 'unallocated'
                        for (long i = 0; i < expectedDeflateAmount; i++)
                            Assert.IsFalse(storage.IsPageAllocated(initCapacity - (i + 1)));

                        //Make sure the data we wrote before inflation is still stored
                        ReadAndVerifyTestPages(storage, storedIndices);
                    }
                }
            }
        }

        [TestMethod]
        public void DeflateGracefullyHandlesResizeFailure()
        {
            using (MemoryStream ms = new MemoryStream())
            {
                const long pageSize = 1024;
                const long initCapacity = 64;
                const long amount = 32;
                StreamingPageStorage.Create(ms, pageSize, initCapacity, null, new CancellationToken(false), true, 1024);

                using (var stream = new MockSafeResizableStorage(ms, true, true, true, null, long.MaxValue))
                {
                    HashSet<long> storedIndices = new HashSet<long>();
                    long got;
                    using (var storage = StreamingPageStorage.Load(stream, false, false, true))
                    {
                        //Allocate all pages in the 'keep' portion
                        long keepSize = initCapacity - amount;
                        for (long i = 0; i < keepSize; i++)
                        {
                            Assert.IsTrue(storage.TryAllocatePage(out long index));
                            Assert.IsTrue(storedIndices.Add(index));
                        }

                        //Deallocate the first 1/3 of those indices
                        for (long i = 0; i < keepSize / 3; i++)
                        {
                            Assert.IsTrue(storage.FreePage(i));
                            Assert.IsTrue(storedIndices.Remove(i));
                        }

                        //Write some test data to the allocated pages
                        WriteTestPages(storage, storedIndices);

                        long expectedAmount = -1;
                        ProgressReporter reporter = new ProgressReporter((x) => {
                            if (x.Current == 7)
                            {
                                stream.ForceTrySetSizeFail = true;
                                expectedAmount = initCapacity - StreamingPageStorage.GetPageCapacityForStreamSize(stream.Length, pageSize);
                            }
                        });

                        got = storage.TryDeflate(amount, reporter, new CancellationToken(false));
                        Assert.AreNotEqual(-1, expectedAmount);
                        Assert.AreNotEqual(amount, got);//Expect failure to have stopped us before the desired amount
                        Assert.AreEqual(expectedAmount, got);

                        Assert.AreEqual(initCapacity - got, storage.PageCapacity);
                        Assert.AreEqual(storedIndices.Count, storage.AllocatedPageCount);
                        storage.Validate(null, new CancellationToken(false));

                        //Make sure all the new pages are set to 'unallocated'
                        for (long i = 0; i < amount; i++)
                            Assert.IsFalse(storage.IsPageAllocated(initCapacity - (i + 1)));

                        //Make sure the data we wrote before inflation is still stored
                        ReadAndVerifyTestPages(storage, storedIndices);
                    }

                    //Reload the storage
                    using (var storage = StreamingPageStorage.Load(stream, true, true, true))
                    {
                        Assert.AreEqual(initCapacity - got, storage.PageCapacity);
                        Assert.AreEqual(storedIndices.Count, storage.AllocatedPageCount);
                        storage.Validate(null, new CancellationToken(false));

                        //Make sure all the new pages are set to 'unallocated'
                        for (long i = 0; i < amount; i++)
                            Assert.IsFalse(storage.IsPageAllocated(initCapacity - (i + 1)));

                        //Make sure the data we wrote before inflation is still stored
                        ReadAndVerifyTestPages(storage, storedIndices);
                    }
                }
            }
        }

        [TestMethod]
        public void ValidateThrowsExceptionWhenCorrupted()
        {
            using (MemoryStream stream = new MemoryStream())
            {
                using (StreamingPageStorage storage = StreamingPageStorage.Create(stream, 1024, 64, null, new CancellationToken(false), true, 1024))
                {
                    Assert.IsTrue(storage.Validate(null, new CancellationToken(false)));

                    //Corrupt the latter half of the memory
                    byte[] buffer = new byte[stream.Length / 2];
                    stream.Position = stream.Length - buffer.Length;
                    stream.Write(buffer, 0, buffer.Length);
                    stream.Flush();

                    Assert.ThrowsException<CorruptDataException>(() => {
                        storage.Validate(null, new CancellationToken(false));
                    });
                }
            }
        }

        [TestMethod]
        public void ValidateWorks()
        {
            using (MemoryStream stream = new MemoryStream())
            {
                using (StreamingPageStorage storage = StreamingPageStorage.Create(stream, 1024, 64, null, new CancellationToken(false), true, 1024))
                {
                    Assert.IsTrue(storage.Validate(null, new CancellationToken(false)));
                }
            }
        }

        [TestMethod]
        public void ValidateReportsProgress()
        {
            using (MemoryStream stream = new MemoryStream())
            {
                long? target = null;
                long? lastProgress = null;

                using (StreamingPageStorage storage = StreamingPageStorage.Create(stream, 1024, 64, null, new CancellationToken(false), true, 1024))
                {
                    ProgressReporter reporter = new ProgressReporter((x) =>
                    {

                        if (target == null)
                            target = x.Target;
                        Assert.IsNotNull(target);

                        Assert.IsTrue(x.Current <= target.Value);
                        if (lastProgress != null)
                            Assert.IsTrue(x.Current >= lastProgress.Value);
                        lastProgress = x.Current;
                    });

                    Assert.IsTrue(storage.Validate(reporter, new CancellationToken(false)));
                    Assert.IsNotNull(target);
                    Assert.AreEqual(target, lastProgress);
                }
            }
        }

        [TestMethod]
        public void ValidateCancellationWorks()
        {
            using (MemoryStream stream = new MemoryStream())
            {
                using (StreamingPageStorage storage = StreamingPageStorage.Create(stream, 1024, 64, null, new CancellationToken(false), true, 1024))
                {
                    CancellationTokenSource cancellation = new CancellationTokenSource();
                    ProgressReporter reporter = new ProgressReporter((x) => {
                        if (x.Current > 4)
                            cancellation.Cancel();
                    });

                    //False indicates cancellation
                    Assert.IsFalse(storage.Validate(reporter, cancellation.Token));
                }
            }
        }

        [TestMethod]
        public void DisposeWillLeaveBaseStreamOpenIfRequested()
        {
            using (MemoryStream ms = new MemoryStream())
            {
                MockSafeResizableStorage stream = new MockSafeResizableStorage(ms, true, true, true, null, long.MaxValue);
                stream.OnDispose += (strm, isDisposing) => {
                    Assert.Fail("This method must not be called, the stream should have remained open.");
                };

                //Test from creating a StreamingPageStorage
                var storage = StreamingPageStorage.Create(stream, 1024, 1, null, new CancellationToken(false), true);
                storage.Dispose();
                Assert.IsTrue(storage.IsDisposed);

                //Test from creating a fixed StreamingPageStorage
                storage = StreamingPageStorage.CreateFixed(stream, 1024, null, new CancellationToken(false), true);
                storage.Dispose();
                Assert.IsTrue(storage.IsDisposed);

                //Test from loading a StreamingPageStorage
                storage = StreamingPageStorage.Load(stream, false, false, true);
                storage.Dispose();
                Assert.IsTrue(storage.IsDisposed);
            }
        }

        [TestMethod]
        public void DisposeWillDisposeBaseStreamIfNotRequestedToLeaveOpen()
        {
            using (MemoryStream ms = new MemoryStream())
            {
                MockSafeResizableStorage stream = new MockSafeResizableStorage(ms, true, true, true, null, long.MaxValue);
                int disposeCallCount = 0;
                stream.OnDispose += (strm, isDisposing) => {
                    disposeCallCount++;
                };

                //Test from creating a StreamingPageStorage
                var storage = StreamingPageStorage.Create(stream, 1024, 1, null, new CancellationToken(false), false);
                storage.Dispose();
                Assert.IsTrue(storage.IsDisposed);
                Assert.AreEqual(1, disposeCallCount);

                //Test from creating a fixed StreamingPageStorage
                storage = StreamingPageStorage.CreateFixed(stream, 1024, null, new CancellationToken(false), false);
                storage.Dispose();
                Assert.IsTrue(storage.IsDisposed);
                Assert.AreEqual(2, disposeCallCount);

                //Test from loading a StreamingPageStorage
                storage = StreamingPageStorage.Load(stream, false, false, false);
                storage.Dispose();
                Assert.IsTrue(storage.IsDisposed);
                Assert.AreEqual(3, disposeCallCount);
            }
        }

        [TestMethod]
        public void DisposeAfterDisposedHasNoEffect()
        {
            using (MemoryStream ms = new MemoryStream())
            {
                MockSafeResizableStorage stream = new MockSafeResizableStorage(ms, true, true, true, null, long.MaxValue);
                int disposeCallCount = 0;
                stream.OnDispose += (strm, isDisposing) => {
                    disposeCallCount++;
                };
                
                var storage = StreamingPageStorage.Create(stream, 1024, 1, null, new CancellationToken(false), false);

                //Dispose once...
                storage.Dispose();
                Assert.IsTrue(storage.IsDisposed);
                Assert.AreEqual(1, disposeCallCount);

                //Dispose again
                storage.Dispose();
                Assert.IsTrue(storage.IsDisposed);
                Assert.AreEqual(1, disposeCallCount);
            }
        }
    }
}
