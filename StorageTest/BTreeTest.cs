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
    public class BTreeTest
    {
        [TestMethod]
        public void ConstructorThrowsWhenArgIsNull()
        {
            Assert.ThrowsException<ArgumentNullException>(() => {
                MockBTree bTree = new MockBTree(null, new MockLongSerializer(), new MockStringSerializer());
            });

            using (MemoryStream ms = new MemoryStream())
            {
                using (MockStream stream = new MockStream(ms, true, true, true))
                {
                    using (StreamingPageStorage storage = StreamingPageStorage.Create(stream, 1024, 3, null, new CancellationToken(false), true))
                    {
                        Assert.ThrowsException<ArgumentNullException>(() =>
                        {
                            MockBTree bTree = new MockBTree(storage, null, new MockStringSerializer());
                        });

                        Assert.ThrowsException<ArgumentNullException>(() =>
                        {
                            MockBTree bTree = new MockBTree(storage, new MockLongSerializer(), null);
                        });
                    }
                }
            }
        }

        [DataTestMethod]
        [DataRow(0)]
        [DataRow(-1)]
        [DataRow(-2)]
        public void ConstructorThrowsWhenArgOutOfRange(long maxMovePairCount)
        {
            using (MemoryStream ms = new MemoryStream())
            {
                using (MockStream stream = new MockStream(ms, true, true, true))
                {
                    using (StreamingPageStorage storage = StreamingPageStorage.Create(stream, 1024, 3, null, new CancellationToken(false), true))
                    {
                        Assert.ThrowsException<ArgumentOutOfRangeException>(() =>
                        {
                            MockBTree tree = new MockBTree(storage, new MockLongSerializer(), new MockStringSerializer(), maxMovePairCount);
                        });
                    }
                }
            }
        }

        [TestMethod]
        public void ConstructorThrowsWhenPageIsTooSmall()
        {
            using (MemoryStream ms = new MemoryStream())
            {
                using (MockStream stream = new MockStream(ms, true, true, true))
                {
                    long pageSize = BTreeNode<long, string>.GetRequiredPageSize(MockLongSerializer.CDataSize, MockStringSerializer.CDataSize, BTreeNode<long, string>.VeryMinKeyValuePairCapacity) - 1;
                    using (StreamingPageStorage storage = StreamingPageStorage.Create(stream, pageSize, 3, null, new CancellationToken(false), true))
                    {
                        Assert.ThrowsException<ArgumentException>(() =>
                        {
                            MockBTree tree = new MockBTree(storage, new MockLongSerializer(), new MockStringSerializer());
                        });
                    }
                }
            }
        }

        [TestMethod]
        public void ConstructorAcceptsMinPageSize()
        {
            using (MemoryStream ms = new MemoryStream())
            {
                using (MockStream stream = new MockStream(ms, true, true, true))
                {
                    long pageSize = BTreeNode<long, string>.GetRequiredPageSize(MockLongSerializer.CDataSize, MockStringSerializer.CDataSize, BTreeNode<long, string>.VeryMinKeyValuePairCapacity);
                    using (StreamingPageStorage storage = StreamingPageStorage.Create(stream, pageSize, 3, null, new CancellationToken(false), true))
                    {
                        MockBTree tree = new MockBTree(storage, new MockLongSerializer(), new MockStringSerializer());
                        Assert.AreEqual(storage, tree.PageStorage);
                    }
                }
            }
        }
        
        [TestMethod]
        public void EverythingWorksUsingMinPageSize()
        {
            using (MemoryStream ms = new MemoryStream())
            {
                using (MockStream stream = new MockStream(ms, true, true, true))
                {
                    long pageSize = BTreeNode<long, string>.GetRequiredPageSize(MockLongSerializer.CDataSize, MockStringSerializer.CDataSize, BTreeNode<long, string>.VeryMinKeyValuePairCapacity);
                    using (StreamingPageStorage storage = StreamingPageStorage.Create(stream, pageSize, 3, null, new CancellationToken(false), true))
                    {
                        MockBTree tree = new MockBTree(storage, new MockLongSerializer(), new MockStringSerializer());

                        const long count = 10240;

                        //Add some elements
                        for (long i = 0; i < count; i++)
                        {
                            Assert.AreEqual(i, tree.Count);

                            Assert.IsFalse(tree.TryGetValue(i, out var mustNotExist, new CancellationToken(false)));
                            Assert.IsNull(mustNotExist);

                            Assert.IsTrue(tree.Insert(i, i.ToString(), false, out bool alreadyExists));
                            Assert.IsFalse(alreadyExists);

                            Assert.IsTrue(tree.TryGetValue(i, out var mustExist, new CancellationToken(false)));
                            Assert.AreEqual(i.ToString(), mustExist);
                            Assert.AreEqual(i + 1, tree.Count);
                        }

                        //Remove the elements
                        for (long i = 0; i < count; i++)
                        {
                            Assert.AreEqual(count - i, tree.Count);
                            Assert.IsTrue(tree.TryGetValue(i, out var mustExist, new CancellationToken(false)));
                            Assert.AreEqual(i.ToString(), mustExist);

                            Assert.IsTrue(tree.Remove(i, out var removedValue));
                            Assert.AreEqual(i.ToString(), removedValue);

                            Assert.AreEqual(count - (i + 1), tree.Count);
                            Assert.IsFalse(tree.TryGetValue(i, out var mustNotExist, new CancellationToken(false)));
                            Assert.IsNull(mustNotExist);
                        }
                    }
                }
            }
        }

        [DataTestMethod]
        [DataRow(BTreeNode<long, string>.VeryMinKeyValuePairCapacity, 0)]
        [DataRow(BTreeNode<long, string>.VeryMinKeyValuePairCapacity, 1)]
        [DataRow(BTreeNode<long, string>.VeryMinKeyValuePairCapacity, 2)]
        [DataRow(BTreeNode<long, string>.VeryMinKeyValuePairCapacity, 3)]
        [DataRow(BTreeNode<long, string>.VeryMinKeyValuePairCapacity + 1/*Even #, should round down to odd #*/, 0)]
        [DataRow(BTreeNode<long, string>.VeryMinKeyValuePairCapacity + 1/*Even #, should round down to odd #*/, 1)]
        [DataRow(BTreeNode<long, string>.VeryMinKeyValuePairCapacity + 1/*Even #, should round down to odd #*/, 2)]
        [DataRow(BTreeNode<long, string>.VeryMinKeyValuePairCapacity + 1/*Even #, should round down to odd #*/, 3)]
        [DataRow(BTreeNode<long, string>.VeryMinKeyValuePairCapacity + 2, 0)]
        [DataRow(BTreeNode<long, string>.VeryMinKeyValuePairCapacity + 2, 1)]
        [DataRow(BTreeNode<long, string>.VeryMinKeyValuePairCapacity + 2, 2)]
        [DataRow(BTreeNode<long, string>.VeryMinKeyValuePairCapacity + 2, 3)]
        [DataRow(BTreeNode<long, string>.VeryMinKeyValuePairCapacity + 3/*Even*/, 0)]
        [DataRow(BTreeNode<long, string>.VeryMinKeyValuePairCapacity + 3/*Even*/, 1)]
        [DataRow(BTreeNode<long, string>.VeryMinKeyValuePairCapacity + 3/*Even*/, 2)]
        [DataRow(BTreeNode<long, string>.VeryMinKeyValuePairCapacity + 3/*Even*/, 3)]
        public void EverythingWorksWhenPageSizeHasPadding(long keyValueCapacity, long paddingAmount)
        {
            using (MemoryStream ms = new MemoryStream())
            {
                using (MockStream stream = new MockStream(ms, true, true, true))
                {
                    //GetRequiredPageSize wants an odd number
                    long oddCapacity = keyValueCapacity;
                    if (oddCapacity % 2 == 0)
                        oddCapacity--;

                    long pageSize = BTreeNode<long, string>.GetRequiredPageSize(MockLongSerializer.CDataSize, MockStringSerializer.CDataSize, oddCapacity) + paddingAmount;

                    //If an even number was requested for this test, add another key-value pair's size to make the capacity even
                    if (keyValueCapacity % 2 == 0)
                        pageSize += BTreeNode<long, string>.GetElementSize(MockLongSerializer.CDataSize, MockStringSerializer.CDataSize);

                    using (StreamingPageStorage storage = StreamingPageStorage.Create(stream, pageSize, 3, null, new CancellationToken(false), true))
                    {
                        MockBTree tree = new MockBTree(storage, new MockLongSerializer(), new MockStringSerializer());

                        const long count = 1024;

                        //Add some elements
                        for (long i = 0; i < count; i++)
                        {
                            Assert.AreEqual(i, tree.Count);

                            Assert.IsFalse(tree.TryGetValue(i, out var mustNotExist, new CancellationToken(false)));
                            Assert.IsNull(mustNotExist);

                            Assert.IsTrue(tree.Insert(i, i.ToString(), false, out bool alreadyExists));
                            Assert.IsFalse(alreadyExists);

                            Assert.IsTrue(tree.TryGetValue(i, out var mustExist, new CancellationToken(false)));
                            Assert.AreEqual(i.ToString(), mustExist);
                            Assert.AreEqual(i + 1, tree.Count);
                        }

                        //Remove the elements
                        for (long i = 0; i < count; i++)
                        {
                            Assert.AreEqual(count - i, tree.Count);
                            Assert.IsTrue(tree.TryGetValue(i, out var mustExist, new CancellationToken(false)));
                            Assert.AreEqual(i.ToString(), mustExist);

                            Assert.IsTrue(tree.Remove(i, out var removedValue));
                            Assert.AreEqual(i.ToString(), removedValue);

                            Assert.AreEqual(count - (i + 1), tree.Count);
                            Assert.IsFalse(tree.TryGetValue(i, out var mustNotExist, new CancellationToken(false)));
                            Assert.IsNull(mustNotExist);
                        }
                    }
                }
            }
        }

        [DataTestMethod]
        [DataRow(0, BTreeNode<long, string>.VeryMinKeyValuePairCapacity)]
        [DataRow(0, BTreeNode<long, string>.VeryMinKeyValuePairCapacity + 2)]
        [DataRow(0, BTreeNode<long, string>.VeryMinKeyValuePairCapacity + 4)]
        [DataRow(1, BTreeNode<long, string>.VeryMinKeyValuePairCapacity)]
        [DataRow(1, BTreeNode<long, string>.VeryMinKeyValuePairCapacity + 2)]
        [DataRow(1, BTreeNode<long, string>.VeryMinKeyValuePairCapacity + 4)]
        [DataRow(2, BTreeNode<long, string>.VeryMinKeyValuePairCapacity)]
        [DataRow(2, BTreeNode<long, string>.VeryMinKeyValuePairCapacity + 2)]
        [DataRow(2, BTreeNode<long, string>.VeryMinKeyValuePairCapacity + 4)]
        [DataRow(3, BTreeNode<long, string>.VeryMinKeyValuePairCapacity)]
        [DataRow(3, BTreeNode<long, string>.VeryMinKeyValuePairCapacity + 2)]
        [DataRow(3, BTreeNode<long, string>.VeryMinKeyValuePairCapacity + 4)]
        [DataRow(4, BTreeNode<long, string>.VeryMinKeyValuePairCapacity)]
        [DataRow(4, BTreeNode<long, string>.VeryMinKeyValuePairCapacity + 2)]
        [DataRow(4, BTreeNode<long, string>.VeryMinKeyValuePairCapacity + 4)]
        [DataRow(5, BTreeNode<long, string>.VeryMinKeyValuePairCapacity)]
        [DataRow(5, BTreeNode<long, string>.VeryMinKeyValuePairCapacity + 2)]
        [DataRow(5, BTreeNode<long, string>.VeryMinKeyValuePairCapacity + 4)]
        [DataRow(6, BTreeNode<long, string>.VeryMinKeyValuePairCapacity)]
        [DataRow(6, BTreeNode<long, string>.VeryMinKeyValuePairCapacity + 2)]
        [DataRow(6, BTreeNode<long, string>.VeryMinKeyValuePairCapacity + 4)]
        [DataRow(7, BTreeNode<long, string>.VeryMinKeyValuePairCapacity)]
        [DataRow(7, BTreeNode<long, string>.VeryMinKeyValuePairCapacity + 2)]
        [DataRow(7, BTreeNode<long, string>.VeryMinKeyValuePairCapacity + 4)]
        [DataRow(1000, BTreeNode<long, string>.VeryMinKeyValuePairCapacity)]
        [DataRow(1000, BTreeNode<long, string>.VeryMinKeyValuePairCapacity + 2)]
        [DataRow(1000, BTreeNode<long, string>.VeryMinKeyValuePairCapacity + 4)]
        public void TryGetValueOnNodeWorks(long count, long keyValueCapacity)
        {
            using (MemoryStream ms = new MemoryStream())
            {
                using (MockStream stream = new MockStream(ms, true, true, true))
                {
                    long pageSize = BTreeNode<long, string>.GetRequiredPageSize(MockLongSerializer.CDataSize, MockStringSerializer.CDataSize, keyValueCapacity);
                    using (StreamingPageStorage storage = StreamingPageStorage.Create(stream, pageSize, 1, null, new CancellationToken(false), true))
                    {
                        MockBTree tree = new MockBTree(storage, new MockLongSerializer(), new MockStringSerializer());

                        //Add some elements
                        for (long i = 0; i < count; i++)
                            Assert.IsTrue(tree.Insert(i, i.ToString(), false, out _));

                        for (long i = 0; i < count; i++)
                        {
                            Assert.IsTrue(tree.TryGetValueOnNode(i, out var value, out var onNode, out long indexOnNode, new CancellationToken(false)));
                            Assert.AreEqual(i.ToString(), value);
                            Assert.AreEqual(i, onNode.GetKeyAt(indexOnNode));
                            Assert.AreEqual(i.ToString(), onNode.GetValueAt(indexOnNode));
                        }
                    }
                }
            }
        }

        [TestMethod]
        public void TryGetValueOnNodeCanBeCancelled()
        {
            using (MemoryStream ms = new MemoryStream())
            {
                using (MockStream stream = new MockStream(ms, true, true, true))
                {
                    long pageSize = BTreeNode<long, string>.GetRequiredPageSize(MockLongSerializer.CDataSize, MockStringSerializer.CDataSize, BTreeNode<long, string>.VeryMinKeyValuePairCapacity);
                    using (StreamingPageStorage storage = StreamingPageStorage.Create(stream, pageSize, 1, null, new CancellationToken(false), true))
                    {
                        MockBTree tree = new MockBTree(storage, new MockLongSerializer(), new MockStringSerializer());

                        const long count = 1024;

                        //Add some elements
                        for (long i = 0; i < count; i++)
                            Assert.IsTrue(tree.Insert(i, i.ToString(), false, out _));

                        CancellationTokenSource cancellation = new CancellationTokenSource();

                        //Interrupt the read IO such that it will cause cancellation while the tree is traversing the nodes
                        long readCount = 0;
                        stream.OnRead += (strm, buffer, bufOff, bufLen) => {
                            if (readCount++ > 25)
                                cancellation.Cancel();
                        };

                        long keyToRemove = count - 1;
                        Assert.IsTrue(tree.ContainsKey(keyToRemove, new CancellationToken(false)));

                        Assert.IsFalse(tree.TryGetValueOnNode(keyToRemove, out var value, out var onNode, out long indexOnNode, cancellation.Token));
                        Assert.IsNull(onNode);
                        Assert.AreEqual(-1, indexOnNode);
                        Assert.IsNull(value);
                    }
                }
            }
        }

        [DataTestMethod]
        [DataRow(0, BTreeNode<long, string>.VeryMinKeyValuePairCapacity)]
        [DataRow(0, BTreeNode<long, string>.VeryMinKeyValuePairCapacity + 2)]
        [DataRow(0, BTreeNode<long, string>.VeryMinKeyValuePairCapacity + 4)]
        [DataRow(1, BTreeNode<long, string>.VeryMinKeyValuePairCapacity)]
        [DataRow(1, BTreeNode<long, string>.VeryMinKeyValuePairCapacity + 2)]
        [DataRow(1, BTreeNode<long, string>.VeryMinKeyValuePairCapacity + 4)]
        [DataRow(2, BTreeNode<long, string>.VeryMinKeyValuePairCapacity)]
        [DataRow(2, BTreeNode<long, string>.VeryMinKeyValuePairCapacity + 2)]
        [DataRow(2, BTreeNode<long, string>.VeryMinKeyValuePairCapacity + 4)]
        [DataRow(3, BTreeNode<long, string>.VeryMinKeyValuePairCapacity)]
        [DataRow(3, BTreeNode<long, string>.VeryMinKeyValuePairCapacity + 2)]
        [DataRow(3, BTreeNode<long, string>.VeryMinKeyValuePairCapacity + 4)]
        [DataRow(4, BTreeNode<long, string>.VeryMinKeyValuePairCapacity)]
        [DataRow(4, BTreeNode<long, string>.VeryMinKeyValuePairCapacity + 2)]
        [DataRow(4, BTreeNode<long, string>.VeryMinKeyValuePairCapacity + 4)]
        [DataRow(5, BTreeNode<long, string>.VeryMinKeyValuePairCapacity)]
        [DataRow(5, BTreeNode<long, string>.VeryMinKeyValuePairCapacity + 2)]
        [DataRow(5, BTreeNode<long, string>.VeryMinKeyValuePairCapacity + 4)]
        [DataRow(6, BTreeNode<long, string>.VeryMinKeyValuePairCapacity)]
        [DataRow(6, BTreeNode<long, string>.VeryMinKeyValuePairCapacity + 2)]
        [DataRow(6, BTreeNode<long, string>.VeryMinKeyValuePairCapacity + 4)]
        [DataRow(7, BTreeNode<long, string>.VeryMinKeyValuePairCapacity)]
        [DataRow(7, BTreeNode<long, string>.VeryMinKeyValuePairCapacity + 2)]
        [DataRow(7, BTreeNode<long, string>.VeryMinKeyValuePairCapacity + 4)]
        [DataRow(1000, BTreeNode<long, string>.VeryMinKeyValuePairCapacity)]
        [DataRow(1000, BTreeNode<long, string>.VeryMinKeyValuePairCapacity + 2)]
        [DataRow(1000, BTreeNode<long, string>.VeryMinKeyValuePairCapacity + 4)]
        public void TryGetValueOnNodeFailsWhenKeyIsNotPresent(long count, long keyValueCapacity)
        {
            using (MemoryStream ms = new MemoryStream())
            {
                using (MockStream stream = new MockStream(ms, true, true, true))
                {
                    long pageSize = BTreeNode<long, string>.GetRequiredPageSize(MockLongSerializer.CDataSize, MockStringSerializer.CDataSize, keyValueCapacity);
                    using (StreamingPageStorage storage = StreamingPageStorage.Create(stream, pageSize, 1, null, new CancellationToken(false), true))
                    {
                        MockBTree tree = new MockBTree(storage, new MockLongSerializer(), new MockStringSerializer());

                        //Add some elements
                        for (long i = 0; i < count; i++)
                            Assert.IsTrue(tree.Insert(i, i.ToString(), false, out _));

                        long key = -1;
                        Assert.IsFalse(tree.TryGetValueOnNode(key, out var value, out var onNode, out long indexOnNode, new CancellationToken(false)));
                        Assert.IsNull(value);
                        Assert.IsNull(onNode);
                        Assert.AreEqual(-1, indexOnNode);
                    }
                }
            }
        }
        
        [DataTestMethod]
        [DataRow(0, BTreeNode<long, string>.VeryMinKeyValuePairCapacity)]
        [DataRow(0, BTreeNode<long, string>.VeryMinKeyValuePairCapacity + 2)]
        [DataRow(0, BTreeNode<long, string>.VeryMinKeyValuePairCapacity + 4)]
        [DataRow(1, BTreeNode<long, string>.VeryMinKeyValuePairCapacity)]
        [DataRow(1, BTreeNode<long, string>.VeryMinKeyValuePairCapacity + 2)]
        [DataRow(1, BTreeNode<long, string>.VeryMinKeyValuePairCapacity + 4)]
        [DataRow(2, BTreeNode<long, string>.VeryMinKeyValuePairCapacity)]
        [DataRow(2, BTreeNode<long, string>.VeryMinKeyValuePairCapacity + 2)]
        [DataRow(2, BTreeNode<long, string>.VeryMinKeyValuePairCapacity + 4)]
        [DataRow(3, BTreeNode<long, string>.VeryMinKeyValuePairCapacity)]
        [DataRow(3, BTreeNode<long, string>.VeryMinKeyValuePairCapacity + 2)]
        [DataRow(3, BTreeNode<long, string>.VeryMinKeyValuePairCapacity + 4)]
        [DataRow(4, BTreeNode<long, string>.VeryMinKeyValuePairCapacity)]
        [DataRow(4, BTreeNode<long, string>.VeryMinKeyValuePairCapacity + 2)]
        [DataRow(4, BTreeNode<long, string>.VeryMinKeyValuePairCapacity + 4)]
        [DataRow(5, BTreeNode<long, string>.VeryMinKeyValuePairCapacity)]
        [DataRow(5, BTreeNode<long, string>.VeryMinKeyValuePairCapacity + 2)]
        [DataRow(5, BTreeNode<long, string>.VeryMinKeyValuePairCapacity + 4)]
        [DataRow(6, BTreeNode<long, string>.VeryMinKeyValuePairCapacity)]
        [DataRow(6, BTreeNode<long, string>.VeryMinKeyValuePairCapacity + 2)]
        [DataRow(6, BTreeNode<long, string>.VeryMinKeyValuePairCapacity + 4)]
        [DataRow(7, BTreeNode<long, string>.VeryMinKeyValuePairCapacity)]
        [DataRow(7, BTreeNode<long, string>.VeryMinKeyValuePairCapacity + 2)]
        [DataRow(7, BTreeNode<long, string>.VeryMinKeyValuePairCapacity + 4)]
        [DataRow(1000, BTreeNode<long, string>.VeryMinKeyValuePairCapacity)]
        [DataRow(1000, BTreeNode<long, string>.VeryMinKeyValuePairCapacity + 2)]
        [DataRow(1000, BTreeNode<long, string>.VeryMinKeyValuePairCapacity + 4)]
        public void TryGetValueWorks(long count, long keyValueCapacity)
        {
            using (MemoryStream ms = new MemoryStream())
            {
                using (MockStream stream = new MockStream(ms, true, true, true))
                {
                    long pageSize = BTreeNode<long, string>.GetRequiredPageSize(MockLongSerializer.CDataSize, MockStringSerializer.CDataSize, keyValueCapacity);
                    using (StreamingPageStorage storage = StreamingPageStorage.Create(stream, pageSize, 1, null, new CancellationToken(false), true))
                    {
                        MockBTree tree = new MockBTree(storage, new MockLongSerializer(), new MockStringSerializer());

                        //Add some elements
                        for (long i = 0; i < count; i++)
                            Assert.IsTrue(tree.Insert(i, i.ToString(), false, out _));

                        for (long i = 0; i < count; i++)
                        {
                            Assert.IsTrue(tree.TryGetValue(i, out var value, new CancellationToken(false)));
                            Assert.AreEqual(i.ToString(), value);
                        }
                    }
                }
            }
        }

        [TestMethod]
        public void TryGetValueCanBeCancelled()
        {
            using (MemoryStream ms = new MemoryStream())
            {
                using (MockStream stream = new MockStream(ms, true, true, true))
                {
                    long pageSize = BTreeNode<long, string>.GetRequiredPageSize(MockLongSerializer.CDataSize, MockStringSerializer.CDataSize, BTreeNode<long, string>.VeryMinKeyValuePairCapacity);
                    using (StreamingPageStorage storage = StreamingPageStorage.Create(stream, pageSize, 1, null, new CancellationToken(false), true))
                    {
                        MockBTree tree = new MockBTree(storage, new MockLongSerializer(), new MockStringSerializer());

                        const long count = 1024;

                        //Add some elements
                        for (long i = 0; i < count; i++)
                            Assert.IsTrue(tree.Insert(i, i.ToString(), false, out _));

                        CancellationTokenSource cancellation = new CancellationTokenSource();

                        //Interrupt the read IO such that it will cause cancellation while the tree is traversing the nodes
                        long readCount = 0;
                        stream.OnRead += (strm, buffer, bufOff, bufLen) => {
                            if (readCount++ > 25)
                                cancellation.Cancel();
                        };

                        long keyToRemove = count - 1;
                        Assert.IsTrue(tree.ContainsKey(keyToRemove, new CancellationToken(false)));

                        Assert.IsFalse(tree.TryGetValue(keyToRemove, out var value, cancellation.Token));
                        Assert.IsNull(value);
                    }
                }
            }
        }

        [DataTestMethod]
        [DataRow(0, BTreeNode<long, string>.VeryMinKeyValuePairCapacity)]
        [DataRow(0, BTreeNode<long, string>.VeryMinKeyValuePairCapacity + 2)]
        [DataRow(0, BTreeNode<long, string>.VeryMinKeyValuePairCapacity + 4)]
        [DataRow(1, BTreeNode<long, string>.VeryMinKeyValuePairCapacity)]
        [DataRow(1, BTreeNode<long, string>.VeryMinKeyValuePairCapacity + 2)]
        [DataRow(1, BTreeNode<long, string>.VeryMinKeyValuePairCapacity + 4)]
        [DataRow(2, BTreeNode<long, string>.VeryMinKeyValuePairCapacity)]
        [DataRow(2, BTreeNode<long, string>.VeryMinKeyValuePairCapacity + 2)]
        [DataRow(2, BTreeNode<long, string>.VeryMinKeyValuePairCapacity + 4)]
        [DataRow(3, BTreeNode<long, string>.VeryMinKeyValuePairCapacity)]
        [DataRow(3, BTreeNode<long, string>.VeryMinKeyValuePairCapacity + 2)]
        [DataRow(3, BTreeNode<long, string>.VeryMinKeyValuePairCapacity + 4)]
        [DataRow(4, BTreeNode<long, string>.VeryMinKeyValuePairCapacity)]
        [DataRow(4, BTreeNode<long, string>.VeryMinKeyValuePairCapacity + 2)]
        [DataRow(4, BTreeNode<long, string>.VeryMinKeyValuePairCapacity + 4)]
        [DataRow(5, BTreeNode<long, string>.VeryMinKeyValuePairCapacity)]
        [DataRow(5, BTreeNode<long, string>.VeryMinKeyValuePairCapacity + 2)]
        [DataRow(5, BTreeNode<long, string>.VeryMinKeyValuePairCapacity + 4)]
        [DataRow(6, BTreeNode<long, string>.VeryMinKeyValuePairCapacity)]
        [DataRow(6, BTreeNode<long, string>.VeryMinKeyValuePairCapacity + 2)]
        [DataRow(6, BTreeNode<long, string>.VeryMinKeyValuePairCapacity + 4)]
        [DataRow(7, BTreeNode<long, string>.VeryMinKeyValuePairCapacity)]
        [DataRow(7, BTreeNode<long, string>.VeryMinKeyValuePairCapacity + 2)]
        [DataRow(7, BTreeNode<long, string>.VeryMinKeyValuePairCapacity + 4)]
        [DataRow(1000, BTreeNode<long, string>.VeryMinKeyValuePairCapacity)]
        [DataRow(1000, BTreeNode<long, string>.VeryMinKeyValuePairCapacity + 2)]
        [DataRow(1000, BTreeNode<long, string>.VeryMinKeyValuePairCapacity + 4)]
        public void TryGetValueFailsWhenKeyIsNotPresent(long count, long keyValueCapacity)
        {
            using (MemoryStream ms = new MemoryStream())
            {
                using (MockStream stream = new MockStream(ms, true, true, true))
                {
                    long pageSize = BTreeNode<long, string>.GetRequiredPageSize(MockLongSerializer.CDataSize, MockStringSerializer.CDataSize, keyValueCapacity);
                    using (StreamingPageStorage storage = StreamingPageStorage.Create(stream, pageSize, 1, null, new CancellationToken(false), true))
                    {
                        MockBTree tree = new MockBTree(storage, new MockLongSerializer(), new MockStringSerializer());

                        //Add some elements
                        for (long i = 0; i < count; i++)
                            Assert.IsTrue(tree.Insert(i, i.ToString(), false, out _));

                        long key = -1;
                        Assert.IsFalse(tree.TryGetValue(key, out var value, new CancellationToken(false)));
                        Assert.IsNull(value);
                    }
                }
            }
        }

        [DataTestMethod]
        [DataRow(true)]
        [DataRow(false)]
        public void InsertThrowsWhenReadOnly(bool updateIfExists)
        {
            using (MemoryStream ms = new MemoryStream())
            {
                using (var storage = StreamingPageStorage.Create(ms, 10240, 1, null, new CancellationToken(false), true))
                {
                    //Initialize the mock B-Tree while 'storage' is writable
                    MockBTree bTree = new MockBTree(storage, new MockLongSerializer(), new MockStringSerializer());
                }

                //Re-load the storage as read-only
                using (var storage = StreamingPageStorage.Load(ms, true, true, true))
                {
                    //Re-load the MockBTree as read-only (determined by the storage being read-only)
                    var bTree = new MockBTree(storage, new MockLongSerializer(), new MockStringSerializer());

                    Assert.ThrowsException<InvalidOperationException>(() => {
                        bTree.Insert(1, "Value", updateIfExists, out bool alreadyExists);
                    });
                }
            }
        }

        [DataTestMethod]
        [DataRow(BTreeNode<long, string>.VeryMinKeyValuePairCapacity, 64, false)]
        [DataRow(BTreeNode<long, string>.VeryMinKeyValuePairCapacity, 64, true)]
        [DataRow(BTreeNode<long, string>.VeryMinKeyValuePairCapacity + 2, 64, false)]
        [DataRow(BTreeNode<long, string>.VeryMinKeyValuePairCapacity + 2, 64, true)]
        [DataRow(BTreeNode<long, string>.VeryMinKeyValuePairCapacity + 4, 64, false)]
        [DataRow(BTreeNode<long, string>.VeryMinKeyValuePairCapacity + 4, 64, true)]
        [DataRow(BTreeNode<long, string>.VeryMinKeyValuePairCapacity + 8, 64, false)]
        [DataRow(BTreeNode<long, string>.VeryMinKeyValuePairCapacity + 8, 64, true)]
        [DataRow(BTreeNode<long, string>.VeryMinKeyValuePairCapacity + 16, 256, false)]
        [DataRow(BTreeNode<long, string>.VeryMinKeyValuePairCapacity + 16, 256, true)]
        public void InsertNewKeyValuePairWorks(long keyValuePairCapacity, long maxCount, bool updateIfExists)
        {
            for(long count = 1; count <= maxCount; count++)
            {
                using (MemoryStream ms = new MemoryStream())
                {
                    using (MockStream stream = new MockStream(ms, true, true, true))
                    {
                        long pageSize = BTreeNode<long, string>.GetRequiredPageSize(MockLongSerializer.CDataSize, MockStringSerializer.CDataSize, keyValuePairCapacity);
                        using (StreamingPageStorage storage = StreamingPageStorage.Create(stream, pageSize, 3, null, new CancellationToken(false), true))
                        {
                            MockBTree tree = new MockBTree(storage, new MockLongSerializer(), new MockStringSerializer());

                            //Insert 'count' key-value pairs
                            for (long i = 0; i < count; i++)
                            {
                                Assert.AreEqual(i, tree.Count);
                                Assert.IsTrue(tree.Insert(i, i.ToString(), updateIfExists, out bool alreadyExists));
                                Assert.IsFalse(alreadyExists);
                                Assert.AreEqual(i + 1, tree.Count);
                            }

                            //Make sure they were inserted
                            Assert.AreEqual(count, tree.Count);
                            for (long i = 0; i < count; i++)
                            {
                                Assert.IsTrue(tree.TryGetValue(i, out var value, new CancellationToken(false)));
                                Assert.AreEqual(i.ToString(), value);
                            }
                        }
                    }
                }
            }
        }

        [DataTestMethod]
        [DataRow(BTreeNode<long, string>.VeryMinKeyValuePairCapacity, 64, false)]
        [DataRow(BTreeNode<long, string>.VeryMinKeyValuePairCapacity, 64, true)]
        [DataRow(BTreeNode<long, string>.VeryMinKeyValuePairCapacity + 2, 64, false)]
        [DataRow(BTreeNode<long, string>.VeryMinKeyValuePairCapacity + 2, 64, true)]
        [DataRow(BTreeNode<long, string>.VeryMinKeyValuePairCapacity + 4, 64, false)]
        [DataRow(BTreeNode<long, string>.VeryMinKeyValuePairCapacity + 4, 64, true)]
        [DataRow(BTreeNode<long, string>.VeryMinKeyValuePairCapacity + 8, 64, false)]
        [DataRow(BTreeNode<long, string>.VeryMinKeyValuePairCapacity + 8, 64, true)]
        [DataRow(BTreeNode<long, string>.VeryMinKeyValuePairCapacity + 16, 256, false)]
        [DataRow(BTreeNode<long, string>.VeryMinKeyValuePairCapacity + 16, 256, true)]
        public void InsertNewKeyValuePairFailsWhenKeyExistsAndUpdateIsNotAllowed(long keyValuePairCapacity, long maxCount, bool updateIfExists)
        {
            for (long count = 1; count <= maxCount; count++)
            {
                using (MemoryStream ms = new MemoryStream())
                {
                    using (MockStream stream = new MockStream(ms, true, true, true))
                    {
                        long pageSize = BTreeNode<long, string>.GetRequiredPageSize(MockLongSerializer.CDataSize, MockStringSerializer.CDataSize, keyValuePairCapacity);
                        using (StreamingPageStorage storage = StreamingPageStorage.Create(stream, pageSize, 3, null, new CancellationToken(false), true))
                        {
                            MockBTree tree = new MockBTree(storage, new MockLongSerializer(), new MockStringSerializer());

                            //Insert them for the first time
                            for (long i = 0; i < count; i++)
                            {
                                Assert.IsTrue(tree.Insert(i, i.ToString(), updateIfExists, out bool alreadyExists));
                                Assert.IsFalse(alreadyExists);
                            }

                            Assert.AreEqual(count, tree.Count);

                            //Try updating value (but not allowed)
                            for (long i = 0; i < count; i++)
                            {
                                Assert.IsFalse(tree.Insert(i, i.ToString(), false, out bool alreadyExists));
                                Assert.IsTrue(alreadyExists);
                            }

                            //Make sure the nothing changed
                            Assert.AreEqual(count, tree.Count);
                            for (long i = 0; i < count; i++)
                            {
                                Assert.IsTrue(tree.TryGetValue(i, out var value, new CancellationToken(false)));
                                Assert.AreEqual(i.ToString(), value);
                            }
                        }
                    }
                }
            }
        }

        [DataTestMethod]
        [DataRow(BTreeNode<long, string>.VeryMinKeyValuePairCapacity, 64, false)]
        [DataRow(BTreeNode<long, string>.VeryMinKeyValuePairCapacity, 64, true)]
        [DataRow(BTreeNode<long, string>.VeryMinKeyValuePairCapacity + 2, 64, false)]
        [DataRow(BTreeNode<long, string>.VeryMinKeyValuePairCapacity + 2, 64, true)]
        [DataRow(BTreeNode<long, string>.VeryMinKeyValuePairCapacity + 4, 64, false)]
        [DataRow(BTreeNode<long, string>.VeryMinKeyValuePairCapacity + 4, 64, true)]
        [DataRow(BTreeNode<long, string>.VeryMinKeyValuePairCapacity + 8, 64, false)]
        [DataRow(BTreeNode<long, string>.VeryMinKeyValuePairCapacity + 8, 64, true)]
        [DataRow(BTreeNode<long, string>.VeryMinKeyValuePairCapacity + 16, 256, false)]
        [DataRow(BTreeNode<long, string>.VeryMinKeyValuePairCapacity + 16, 256, true)]
        public void InsertNewKeyValuePairCanUpdateExistingValues(long keyValuePairCapacity, long maxCount, bool updateIfExists)
        {
            for (long count = 1; count <= maxCount; count++)
            {
                using (MemoryStream ms = new MemoryStream())
                {
                    using (MockStream stream = new MockStream(ms, true, true, true))
                    {
                        long pageSize = BTreeNode<long, string>.GetRequiredPageSize(MockLongSerializer.CDataSize, MockStringSerializer.CDataSize, keyValuePairCapacity);
                        using (StreamingPageStorage storage = StreamingPageStorage.Create(stream, pageSize, 3, null, new CancellationToken(false), true))
                        {
                            MockBTree tree = new MockBTree(storage, new MockLongSerializer(), new MockStringSerializer());

                            //Insert them for the first time
                            for (long i = 0; i < count; i++)
                            {
                                Assert.IsTrue(tree.Insert(i, i.ToString(), updateIfExists, out bool alreadyExists));
                                Assert.IsFalse(alreadyExists);
                            }

                            Assert.AreEqual(count, tree.Count);

                            //Update the values
                            for (long i = 0; i < count; i++)
                            {
                                Assert.IsTrue(tree.Insert(i, i.ToString() + "_Updated", true, out bool alreadyExists));
                                Assert.IsTrue(alreadyExists);
                            }

                            //Make sure they were updated correctly
                            Assert.AreEqual(count, tree.Count);
                            for (long i = 0; i < count; i++)
                            {
                                Assert.IsTrue(tree.TryGetValue(i, out var value, new CancellationToken(false)));
                                Assert.AreEqual(i.ToString() + "_Updated", value);
                            }
                        }
                    }
                }
            }
        }

        private static BTreeNode<long, string> CreateFullNode(MockBTree bTree, long goalDepth, long currentDepth, ref long keyIncrementer, Dictionary<long, string> addedPairs)
        {
            Assert.IsTrue(BTreeNode<long, string>.TryCreateNew(bTree, goalDepth == currentDepth, out var node));
            node.KeyValuePairCount = node.MaxKeyValuePairCapacity;
            for(long i = 0; i < node.KeyValuePairCount; i++)
            {
                if(i == 0 && !node.IsLeaf)
                {
                    //Create the left sub-tree
                    var leftSubTreeRoot = CreateFullNode(bTree, goalDepth, currentDepth + 1, ref keyIncrementer, addedPairs);
                    node.SetSubTreeAt(i, leftSubTreeRoot);
                }

                long currentKey = keyIncrementer++;
                node.SetKeyAt(i, currentKey);
                node.SetValueAt(i, currentKey.ToString());
                addedPairs.Add(currentKey, currentKey.ToString());

                if(!node.IsLeaf)
                {
                    //Create the right sub-tree
                    var rightSubTreeRoot = CreateFullNode(bTree, goalDepth, currentDepth + 1, ref keyIncrementer, addedPairs);
                    node.SetSubTreeAt(i + 1, rightSubTreeRoot);
                }
            }
            return node;
        }

        private static MockBTree CreateFullTree(MemoryStream ms, long keyValueCapacity, long nodeDepth, bool isFixed, out MockSafeResizableStorage resizableStorage, out StreamingPageStorage pageStorage, out Dictionary<long, string> addedPairs, long maxMoveCount = 2)
        {
            resizableStorage = new MockSafeResizableStorage(ms, true, true, true, null, long.MaxValue);
            long pageSize = BTreeNode<long, string>.GetRequiredPageSize(MockLongSerializer.CDataSize, MockStringSerializer.CDataSize, keyValueCapacity);
            
            //For simplicity, we start with non-fixed storage (after the tree is created, if 'isFixed' is requested, we will re-load as fixed)
            pageStorage = StreamingPageStorage.Create(resizableStorage, pageSize, 1, null, new CancellationToken(false), true);
            addedPairs = new Dictionary<long, string>();
            MockBTree ret = new MockBTree(pageStorage, new MockLongSerializer(), new MockStringSerializer(), maxMoveCount);
            long keyIncrementer = 0;
            ret.Root = CreateFullNode(ret, nodeDepth, 0, ref keyIncrementer, addedPairs);

            if (isFixed)
            {
                //Dispose the old one (since we are done using it, about to reload a new instance)
                pageStorage.Dispose();

                pageStorage = StreamingPageStorage.Load(ms, false, isFixed, true);
                //Since we re-loaded the pageStorage, also re-load the tree
                ret = new MockBTree(pageStorage, new MockLongSerializer(), new MockStringSerializer(), maxMoveCount);
            }

            ret.Count = addedPairs.Count;
            return ret;
        }

        [DataTestMethod]
        [DataRow(5, 1)]
        [DataRow(5, 2)]
        [DataRow(5, 3)]
        [DataRow(5, 4)]
        [DataRow(7, 1)]
        [DataRow(7, 2)]
        [DataRow(17, 1)]
        [DataRow(17, 2)]
        public void InsertCausesInflationWhenNecessary(long keyValueCapacity, long nodeDepth)
        {
            using (MemoryStream ms = new MemoryStream())
            {
                var tree = CreateFullTree(ms, keyValueCapacity, nodeDepth, false, out var resizableStorage, out var pageStorage, out var initPairs);
                using (pageStorage)
                {
                    Assert.AreEqual(pageStorage.PageCapacity, pageStorage.AllocatedPageCount);
                    long initCapacity = pageStorage.PageCapacity;

                    Assert.IsTrue(tree.Insert(-1, (-1).ToString(), false, out bool alreadyExists));
                    Assert.IsFalse(alreadyExists);

                    Assert.IsTrue(pageStorage.PageCapacity > initCapacity);

                    //Make sure everything is correct
                    Assert.AreEqual(initPairs.Count + 1, tree.Count);
                    foreach (var pair in initPairs)
                    {
                        Assert.IsTrue(tree.TryGetValue(pair.Key, out var gotValue, new CancellationToken(false)));
                        Assert.AreEqual(pair.Value, gotValue);
                    }

                    //Make sure the new value was added
                    Assert.IsTrue(tree.TryGetValue(-1, out var gotValue_, new CancellationToken(false)));
                    Assert.AreEqual((-1).ToString(), gotValue_);
                }
            }
        }

        [DataTestMethod]
        [DataRow(5, 1, 1)]
        [DataRow(5, 2, 1)]
        [DataRow(5, 2, 2)]
        [DataRow(5, 2, 3)]
        [DataRow(5, 3, 1)]
        [DataRow(5, 3, 2)]
        [DataRow(5, 3, 3)]
        [DataRow(5, 3, 4)]
        [DataRow(5, 4, 5)]
        [DataRow(5, 5, 6)]
        [DataRow(7, 1, 1)]
        [DataRow(7, 1, 2)]
        [DataRow(7, 2, 1)]
        [DataRow(7, 2, 2)]
        [DataRow(7, 3, 1)]
        [DataRow(7, 3, 2)]
        public void InsertFailsWhenInternalInflationFails(long keyValueCapacity, long nodeDepth, long successfulInflationCount)
        {
            /* To understand this test, consider a tree with several nodes high, all nodes full.
             * We want to insert a key-value pair to one of the lowest nodes (farthest from root).
             * As the insertion algorithm traverses the nodes on its way to the destination leaf
             * node, it will split any full nodes (all, for this test). We want to test what
             * happens when the split fails at a particular position (after 
             * 'successfulInflationCount' succesful inflations). For this test to be valid, we 
             * need to successfully split 'successfulInflationCount' nodes and still have one more
             * node to inflate - this one must fail to inflate. */

            using (MemoryStream ms = new MemoryStream())
            {
                var tree = CreateFullTree(ms, keyValueCapacity, nodeDepth, false, out var resizableStorage, out var pageStorage, out var initPairs);
                using (pageStorage)
                {
                    Assert.AreEqual(pageStorage.PageCapacity, pageStorage.AllocatedPageCount);
                    long initCapacity = pageStorage.PageCapacity;

                    //Ensure that we can only inflate 'successfulInflateCount' times
                    long attemptedInflateCount = 0;
                    resizableStorage.OnTrySetSize += (rs, sz) => {
                        attemptedInflateCount++;

                        if (attemptedInflateCount > successfulInflationCount)
                            resizableStorage.ForceTrySetSizeFail = true;
                    };

                    //Try to insert a key to one of the lowest nodes, this will fail due to inability
                    //to inflate the required number of times (for node splits)
                    long keyToInsert = -1;//New key insertion always leads to a leaf node, so we could use any non-existing key value here
                    Assert.IsFalse(tree.Insert(keyToInsert, keyToInsert.ToString(), false, out bool alreadyExists));//If this call does not fail, 'successfulInflationCount' is likely too high
                    Assert.IsFalse(alreadyExists);
                    Assert.IsTrue(attemptedInflateCount > successfulInflationCount);//Ensure insertion really failed due to inflation failure

                    //Make sure nothing really changed.
                    //Some nodes may have been split, but we don't observe that from public interface.
                    Assert.AreEqual(initPairs.Count, tree.Count);
                    foreach (var pair in initPairs)
                    {
                        Assert.IsTrue(tree.TryGetValue(pair.Key, out var gotValue, new CancellationToken(false)));
                        Assert.AreEqual(pair.Value, gotValue);
                    }

                    //Make sure the new value was NOT added
                    Assert.IsFalse(tree.TryGetValue(-1, out var gotValue_, new CancellationToken(false)));
                    Assert.IsNull(gotValue_);
                }
            }
        }

        [DataTestMethod]
        [DataRow(5, 1, 1)]
        [DataRow(5, 2, 1)]
        [DataRow(5, 2, 2)]
        [DataRow(5, 2, 3)]
        [DataRow(5, 3, 1)]
        [DataRow(5, 3, 2)]
        [DataRow(5, 3, 3)]
        [DataRow(5, 3, 4)]
        [DataRow(5, 4, 5)]
        [DataRow(5, 5, 6)]
        [DataRow(7, 1, 1)]
        [DataRow(7, 1, 2)]
        [DataRow(7, 2, 1)]
        [DataRow(7, 2, 2)]
        [DataRow(7, 3, 1)]
        [DataRow(7, 3, 2)]
        [DataRow(7, 3, 3)]
        [DataRow(7, 4, 1)]
        public void InsertCanUpdateValueEvenIfInternalInflationFails(long keyValueCapacity, long nodeDepth, long successfulInflationCount)
        {
            /* To understand this test, consider a tree with several nodes high, all nodes full.
             * We want to update the value of a key-value pair on one of the lowest nodes (farthest
             * from root). As the insertion algorithm traverses the nodes on its way to the destination
             * leaf node, it will split any full nodes (all, for this test). We want to test what
             * happens when the split fails at a particular position (after 'successfulInflationCount' 
             * succesful inflations). For this test to be valid, we need to successfully split
             * 'successfulInflationCount' nodes and still have one more node to inflate - this one must
             * fail to inflate. */

            using (MemoryStream ms = new MemoryStream())
            {
                var tree = CreateFullTree(ms, keyValueCapacity, nodeDepth, false, out var resizableStorage, out var pageStorage, out var initPairs);
                using (pageStorage)
                {
                    Assert.AreEqual(pageStorage.PageCapacity, pageStorage.AllocatedPageCount);
                    long initCapacity = pageStorage.PageCapacity;

                    //Ensure that we can only inflate 'successfulInflateCount' times
                    long attemptedInflateCount = 0;
                    resizableStorage.OnTrySetSize += (rs, sz) => {
                        attemptedInflateCount++;

                        if (attemptedInflateCount > successfulInflationCount)
                            resizableStorage.ForceTrySetSizeFail = true;
                    };

                    //Try to update the value of an existing key on one of the lowest nodes. The internal
                    //splitting will eventually fail (due to inflation failure), but this will not corrupt
                    //anything and it will not prevent us from updating the existing key-value pair.
                    long keyToUpdate = 0;
                    Assert.IsTrue(tree.Insert(keyToUpdate, "NewValue", true, out bool alreadyExists));
                    Assert.IsTrue(alreadyExists);
                    Assert.IsTrue(attemptedInflateCount > successfulInflationCount);//Ensure insertion really failed due to inflation failure

                    //Make sure that the value was updated, and that all other key-value pairs remain unchanged
                    Assert.AreEqual(initPairs.Count, tree.Count);
                    foreach (var pair in initPairs)
                    {
                        Assert.IsTrue(tree.TryGetValue(pair.Key, out var gotValue, new CancellationToken(false)));
                        string expectedValue = pair.Value;
                        if (pair.Key == keyToUpdate)
                            expectedValue = "NewValue";

                        Assert.AreEqual(expectedValue, gotValue);
                    }
                }
            }
        }

        [DataTestMethod]
        [DataRow(5, 1, 1)]
        [DataRow(5, 2, 1)]
        [DataRow(5, 2, 2)]
        [DataRow(5, 2, 3)]
        [DataRow(5, 3, 1)]
        [DataRow(5, 3, 2)]
        [DataRow(5, 3, 3)]
        [DataRow(5, 3, 4)]
        [DataRow(5, 4, 5)]
        [DataRow(5, 5, 6)]
        [DataRow(7, 1, 1)]
        [DataRow(7, 1, 2)]
        [DataRow(7, 2, 1)]
        [DataRow(7, 2, 2)]
        [DataRow(7, 3, 1)]
        [DataRow(7, 3, 2)]
        [DataRow(7, 3, 3)]
        [DataRow(7, 4, 1)]
        public void InsertCannotInsertOrUpdateNonExistingKeyPairValueIfInternalInflationFails(long keyValueCapacity, long nodeDepth, long successfulInflationCount)
        {
            /* To understand this test, consider a tree with several nodes high, all nodes full.
             * We want to update the value of a key-value pair on one of the lowest nodes (farthest
             * from root). As the insertion algorithm traverses the nodes on its way to the destination
             * leaf node, it will split any full nodes (all, for this test). We want to test what
             * happens when the split fails at a particular position (after 'successfulInflationCount' 
             * succesful inflations). For this test to be valid, we need to successfully split
             * 'successfulInflationCount' nodes and still have one more node to inflate - this one must
             * fail to inflate. */

            using (MemoryStream ms = new MemoryStream())
            {
                var tree = CreateFullTree(ms, keyValueCapacity, nodeDepth, false, out var resizableStorage, out var pageStorage, out var initPairs);
                using (pageStorage)
                {
                    Assert.AreEqual(pageStorage.PageCapacity, pageStorage.AllocatedPageCount);
                    long initCapacity = pageStorage.PageCapacity;

                    //Ensure that we can only inflate 'successfulInflateCount' times
                    long attemptedInflateCount = 0;
                    resizableStorage.OnTrySetSize += (rs, sz) => {
                        attemptedInflateCount++;

                        if (attemptedInflateCount > successfulInflationCount)
                            resizableStorage.ForceTrySetSizeFail = true;
                    };

                    //Try to update the value of a non-existing key on one of the lowest nodes. The internal
                    //splitting will eventually fail (due to inflation failure), but this will not corrupt
                    //anything and it will not prevent us from updating the existing key-value pair.
                    long keyToUpdate = -1;
                    Assert.IsFalse(tree.Insert(keyToUpdate, "NewValue", true, out bool alreadyExists));
                    Assert.IsFalse(alreadyExists);
                    Assert.IsTrue(attemptedInflateCount > successfulInflationCount);//Ensure insertion really failed due to inflation failure

                    //Make sure nothing changed
                    Assert.AreEqual(initPairs.Count, tree.Count);
                    foreach (var pair in initPairs)
                    {
                        Assert.IsTrue(tree.TryGetValue(pair.Key, out var gotValue, new CancellationToken(false)));
                        string expectedValue = pair.Value;
                        Assert.AreEqual(expectedValue, gotValue);
                    }
                }
            }
        }

        [DataTestMethod]
        [DataRow(5, 1, false)]
        [DataRow(5, 2, false)]
        [DataRow(5, 3, false)]
        [DataRow(5, 4, false)]
        [DataRow(5, 5, false)]
        [DataRow(7, 1, false)]
        [DataRow(7, 2, false)]
        [DataRow(7, 4, false)]
        [DataRow(13, 1, false)]
        [DataRow(13, 2, false)]
        [DataRow(13, 3, false)]
        [DataRow(5, 1, true)]
        [DataRow(5, 2, true)]
        [DataRow(5, 3, true)]
        [DataRow(5, 4, true)]
        [DataRow(5, 5, true)]
        [DataRow(7, 1, true)]
        [DataRow(7, 2, true)]
        [DataRow(7, 4, true)]
        [DataRow(13, 1, true)]
        [DataRow(13, 2, true)]
        [DataRow(13, 3, true)]
        public void InsertFailsWhenCapacityIsFullAndFixed(long keyValueCapacity, long nodeDepth, bool updateIfExists)
        {
            using (MemoryStream ms = new MemoryStream())
            {
                var tree = CreateFullTree(ms, keyValueCapacity, nodeDepth, true, out var resizableStorage, out var pageStorage, out var initPairs);
                using (pageStorage)
                {
                    Assert.AreEqual(pageStorage.PageCapacity, pageStorage.AllocatedPageCount);
                    Assert.IsTrue(pageStorage.IsCapacityFixed);

                    resizableStorage.OnTrySetSize += (st, sz) =>
                    {
                        Assert.Fail("TrySetSize must not be called when the capacity is fixed.");
                    };

                    long initCapacity = pageStorage.PageCapacity;

                    //Try to insert a new key-value pair. This will fail since capacity is full and fixed-size
                    Assert.IsFalse(tree.Insert(-1, (-1).ToString(), updateIfExists, out bool alreadyExists));
                    Assert.IsFalse(alreadyExists);
                    Assert.AreEqual(initCapacity, pageStorage.PageCapacity);

                    //Make sure nothing changed
                    Assert.AreEqual(initPairs.Count, tree.Count);
                    foreach (var pair in initPairs)
                    {
                        Assert.IsTrue(tree.TryGetValue(pair.Key, out var gotValue, new CancellationToken(false)));
                        string expectedValue = pair.Value;
                        Assert.AreEqual(expectedValue, gotValue);
                    }
                }
            }
        }

        [DataTestMethod]
        [DataRow(5, 1)]
        [DataRow(5, 2)]
        [DataRow(5, 3)]
        [DataRow(5, 4)]
        [DataRow(5, 5)]
        [DataRow(7, 1)]
        [DataRow(7, 2)]
        [DataRow(7, 3)]
        [DataRow(19, 1)]
        [DataRow(19, 2)]
        public void InsertCanUpdateWhenCapacityIsFullAndFixed(long keyValueCapacity, long nodeDepth)
        {
            using (MemoryStream ms = new MemoryStream())
            {
                var tree = CreateFullTree(ms, keyValueCapacity, nodeDepth, true, out var resizableStorage, out var pageStorage, out var initPairs);
                using (pageStorage)
                {
                    Assert.AreEqual(pageStorage.PageCapacity, pageStorage.AllocatedPageCount);
                    Assert.IsTrue(pageStorage.IsCapacityFixed);

                    resizableStorage.OnTrySetSize += (st, sz) =>
                    {
                        Assert.Fail("TrySetSize must not be called when the capacity is fixed.");
                    };

                    long initCapacity = pageStorage.PageCapacity;

                    //Update the value to an existing key-value pair
                    long keyToUpdate = 0;
                    Assert.IsTrue(tree.Insert(keyToUpdate, "NewValue", true, out bool alreadyExists));
                    Assert.IsTrue(alreadyExists);
                    Assert.AreEqual(initCapacity, pageStorage.PageCapacity);

                    //Make sure that the value was updated, and that all other key-value pairs remain unchanged
                    Assert.AreEqual(initPairs.Count, tree.Count);
                    foreach (var pair in initPairs)
                    {
                        Assert.IsTrue(tree.TryGetValue(pair.Key, out var gotValue, new CancellationToken(false)));
                        string expectedValue = pair.Value;
                        if (pair.Key == keyToUpdate)
                            expectedValue = "NewValue";

                        Assert.AreEqual(expectedValue, gotValue);
                    }
                }
            }
        }

        [TestMethod]
        public void RemoveThrowsWhenReadOnly()
        {
            using (MemoryStream ms = new MemoryStream())
            {
                using (var storage = StreamingPageStorage.Create(ms, 10240, 1, null, new CancellationToken(false), true))
                {
                    //Initialize the mock B-Tree while 'storage' is writable
                    MockBTree bTree = new MockBTree(storage, new MockLongSerializer(), new MockStringSerializer());
                }

                //Re-load the storage as read-only
                using (var storage = StreamingPageStorage.Load(ms, true, true, true))
                {
                    //Re-load the MockBTree as read-only (determined by the storage being read-only)
                    var bTree = new MockBTree(storage, new MockLongSerializer(), new MockStringSerializer());

                    Assert.ThrowsException<InvalidOperationException>(() => {
                        bTree.Remove(0, out var removed);
                    });
                }
            }
        }

        [TestMethod]
        public void RemoveChangesNothingWhenKeyDoesNotExist()
        {
            using (MemoryStream ms = new MemoryStream())
            {
                using (MockStream stream = new MockStream(ms, true, true, true))
                {
                    long pageSize = BTreeNode<long, string>.GetRequiredPageSize(MockLongSerializer.CDataSize, MockStringSerializer.CDataSize, BTreeNode<long, string>.VeryMinKeyValuePairCapacity);
                    using (StreamingPageStorage storage = StreamingPageStorage.Create(stream, pageSize, 1, null, new CancellationToken(false), true))
                    {
                        MockBTree tree = new MockBTree(storage, new MockLongSerializer(), new MockStringSerializer());

                        const long keyValuePairCount = 1024;

                        //Add some elements
                        for (long i = 0; i < keyValuePairCount; i++)
                            Assert.IsTrue(tree.Insert(i, i.ToString(), false, out _));

                        //Try to remove a key-value pair which does not exist
                        Assert.IsFalse(tree.Remove(-1, out var value));
                        Assert.IsNull(value);

                        //Make sure all stored key-value pairs are still valid
                        Assert.AreEqual(keyValuePairCount, tree.Count);
                        for (long i = 0; i < keyValuePairCount; i++)
                        {
                            Assert.IsTrue(tree.TryGetValue(i, out var value_, new CancellationToken(false)));
                            Assert.AreEqual(i.ToString(), value_);
                        }
                    }
                }
            }
        }

        public enum InsertKeyOrder
        {
            Ascending,
            Descending,
            Alternating,
            Random
        }

        public enum RemoveKeyOrder
        {
            SameAsInsertion,
            OppositeOfInsertion,
            Random
        }

        [DataTestMethod]
        [DataRow(5, 1024, InsertKeyOrder.Alternating, RemoveKeyOrder.SameAsInsertion)]
        [DataRow(5, 1024, InsertKeyOrder.Alternating, RemoveKeyOrder.OppositeOfInsertion)]
        [DataRow(5, 1024, InsertKeyOrder.Alternating, RemoveKeyOrder.Random)]
        [DataRow(5, 1024, InsertKeyOrder.Ascending, RemoveKeyOrder.SameAsInsertion)]
        [DataRow(5, 1024, InsertKeyOrder.Ascending, RemoveKeyOrder.OppositeOfInsertion)]
        [DataRow(5, 1024, InsertKeyOrder.Ascending, RemoveKeyOrder.Random)]
        [DataRow(5, 1024, InsertKeyOrder.Descending, RemoveKeyOrder.SameAsInsertion)]
        [DataRow(5, 1024, InsertKeyOrder.Descending, RemoveKeyOrder.OppositeOfInsertion)]
        [DataRow(5, 1024, InsertKeyOrder.Descending, RemoveKeyOrder.Random)]
        [DataRow(5, 1024, InsertKeyOrder.Random, RemoveKeyOrder.SameAsInsertion)]
        [DataRow(5, 1024, InsertKeyOrder.Random, RemoveKeyOrder.OppositeOfInsertion)]
        [DataRow(5, 1024, InsertKeyOrder.Random, RemoveKeyOrder.Random)]

        [DataRow(17, 1024, InsertKeyOrder.Alternating, RemoveKeyOrder.SameAsInsertion)]
        [DataRow(17, 1024, InsertKeyOrder.Alternating, RemoveKeyOrder.OppositeOfInsertion)]
        [DataRow(17, 1024, InsertKeyOrder.Alternating, RemoveKeyOrder.Random)]
        [DataRow(17, 1024, InsertKeyOrder.Ascending, RemoveKeyOrder.SameAsInsertion)]
        [DataRow(17, 1024, InsertKeyOrder.Ascending, RemoveKeyOrder.OppositeOfInsertion)]
        [DataRow(17, 1024, InsertKeyOrder.Ascending, RemoveKeyOrder.Random)]
        [DataRow(17, 1024, InsertKeyOrder.Descending, RemoveKeyOrder.SameAsInsertion)]
        [DataRow(17, 1024, InsertKeyOrder.Descending, RemoveKeyOrder.OppositeOfInsertion)]
        [DataRow(17, 1024, InsertKeyOrder.Descending, RemoveKeyOrder.Random)]
        [DataRow(17, 1024, InsertKeyOrder.Random, RemoveKeyOrder.SameAsInsertion)]
        [DataRow(17, 1024, InsertKeyOrder.Random, RemoveKeyOrder.OppositeOfInsertion)]
        [DataRow(17, 1024, InsertKeyOrder.Random, RemoveKeyOrder.Random)]

        [DataRow(127, 4096, InsertKeyOrder.Random, RemoveKeyOrder.Random)]
        public void RemoveWorks(long keyValuePairCapacity, long keyValuePairCount, InsertKeyOrder insertOrder, RemoveKeyOrder removeOrder)
        {
            using (MemoryStream ms = new MemoryStream())
            {
                using (MockStream stream = new MockStream(ms, true, true, true))
                {
                    long pageSize = BTreeNode<long, string>.GetRequiredPageSize(MockLongSerializer.CDataSize, MockStringSerializer.CDataSize, keyValuePairCapacity);
                    using (StreamingPageStorage storage = StreamingPageStorage.Create(stream, pageSize, 3, null, new CancellationToken(false), true))
                    {
                        MockBTree tree = new MockBTree(storage, new MockLongSerializer(), new MockStringSerializer());

                        Random r = new Random(5);//Constant seed for consistent tests
                        HashSet<long> usedKeys = new HashSet<long>();
                        List<long> usedKeysInOrder = new List<long>();

                        long allocatedPageCountBeforeInsertion = storage.AllocatedPageCount;

                        //Add some elements
                        for (long i = 0; i < keyValuePairCount; i++)
                        {
                            Assert.AreEqual(i, tree.Count);

                            long key = -1;
                            switch (insertOrder)
                            {
                                case InsertKeyOrder.Alternating:
                                    key = i * (i % 2 == 0 ? -1 : 1);
                                    break;
                                case InsertKeyOrder.Ascending:
                                    key = i;
                                    break;
                                case InsertKeyOrder.Descending:
                                    key = -i;
                                    break;

                                case InsertKeyOrder.Random:
                                    key = r.Next();
                                    while (usedKeys.Contains(key))
                                        key = r.Next();
                                    break;
                            }

                            usedKeys.Add(key);
                            usedKeysInOrder.Add(key);

                            Assert.IsFalse(tree.TryGetValue(key, out var mustNotExist, new CancellationToken(false)));
                            Assert.IsNull(mustNotExist);

                            Assert.IsTrue(tree.Insert(key, key.ToString(), false, out bool alreadyExists));
                            Assert.IsFalse(alreadyExists);

                            Assert.IsTrue(tree.TryGetValue(key, out var mustExist, new CancellationToken(false)));
                            Assert.AreEqual(key.ToString(), mustExist);
                            Assert.AreEqual(i + 1, tree.Count);
                        }

                        long allocatedPageCountAfterInsertion = storage.AllocatedPageCount;
                        Assert.IsTrue(allocatedPageCountAfterInsertion > allocatedPageCountBeforeInsertion);

                        //Remove the elements
                        for (long i = 0; i < keyValuePairCount; i++)
                        {
                            Assert.AreEqual(keyValuePairCount - i, tree.Count);

                            long key = -1;
                            switch (removeOrder)
                            {
                                case RemoveKeyOrder.SameAsInsertion:
                                    key = usedKeysInOrder[(int)i];
                                    break;

                                case RemoveKeyOrder.OppositeOfInsertion:
                                    key = usedKeysInOrder[usedKeysInOrder.Count - (int)(i + 1)];
                                    break;

                                case RemoveKeyOrder.Random:
                                    long randIndex = r.Next();
                                    key = usedKeysInOrder[(int)randIndex % usedKeysInOrder.Count];
                                    usedKeysInOrder.RemoveAt((int)randIndex % usedKeysInOrder.Count);
                                    break;
                            }

                            Assert.IsTrue(tree.TryGetValue(key, out var mustExist, new CancellationToken(false)));
                            Assert.AreEqual(key.ToString(), mustExist);

                            Assert.IsTrue(tree.Remove(key, out var removedValue));
                            Assert.AreEqual(key.ToString(), removedValue);

                            Assert.AreEqual(keyValuePairCount - (i + 1), tree.Count);
                            Assert.IsFalse(tree.TryGetValue(key, out var mustNotExist, new CancellationToken(false)));
                            Assert.IsNull(mustNotExist);
                        }

                        //Make sure pages were freed
                        Assert.AreEqual(allocatedPageCountBeforeInsertion, storage.AllocatedPageCount);
                    }

                    //Reload the tree as read-only so we can validate its entire structure
                    using (var storage = StreamingPageStorage.Load(stream, true, true, true))
                    {
                        var tree = new MockBTree(storage, new MockLongSerializer(), new MockStringSerializer());
                        tree.Validate(new CancellationToken(false));
                    }
                }
            }
        }

        [DataTestMethod]
        [DataRow(5, 1024, InsertKeyOrder.Alternating, RemoveKeyOrder.SameAsInsertion)]
        [DataRow(5, 1024, InsertKeyOrder.Alternating, RemoveKeyOrder.OppositeOfInsertion)]
        [DataRow(5, 1024, InsertKeyOrder.Alternating, RemoveKeyOrder.Random)]
        [DataRow(5, 1024, InsertKeyOrder.Ascending, RemoveKeyOrder.SameAsInsertion)]
        [DataRow(5, 1024, InsertKeyOrder.Ascending, RemoveKeyOrder.OppositeOfInsertion)]
        [DataRow(5, 1024, InsertKeyOrder.Ascending, RemoveKeyOrder.Random)]
        [DataRow(5, 1024, InsertKeyOrder.Descending, RemoveKeyOrder.SameAsInsertion)]
        [DataRow(5, 1024, InsertKeyOrder.Descending, RemoveKeyOrder.OppositeOfInsertion)]
        [DataRow(5, 1024, InsertKeyOrder.Descending, RemoveKeyOrder.Random)]
        [DataRow(5, 1024, InsertKeyOrder.Random, RemoveKeyOrder.SameAsInsertion)]
        [DataRow(5, 1024, InsertKeyOrder.Random, RemoveKeyOrder.OppositeOfInsertion)]
        [DataRow(5, 1024, InsertKeyOrder.Random, RemoveKeyOrder.Random)]
        [DataRow(127, 1024, InsertKeyOrder.Random, RemoveKeyOrder.Random)]
        public void RemoveSomeThenValidateWorks(long keyValuePairCapacity, long keyValuePairCount, InsertKeyOrder insertOrder, RemoveKeyOrder removeOrder)
        {
            using (MemoryStream ms = new MemoryStream())
            {
                using (MockStream stream = new MockStream(ms, true, true, true))
                {
                    long pageSize = BTreeNode<long, string>.GetRequiredPageSize(MockLongSerializer.CDataSize, MockStringSerializer.CDataSize, keyValuePairCapacity);
                    using (StreamingPageStorage storage = StreamingPageStorage.Create(stream, pageSize, 3, null, new CancellationToken(false), true))
                    {
                        MockBTree tree = new MockBTree(storage, new MockLongSerializer(), new MockStringSerializer());

                        Random r = new Random(5);//Constant seed for consistent tests
                        HashSet<long> usedKeys = new HashSet<long>();
                        List<long> usedKeysInOrder = new List<long>();

                        long allocatedPageCountBeforeInsertion = storage.AllocatedPageCount;

                        //Add some elements
                        for (long i = 0; i < keyValuePairCount; i++)
                        {
                            Assert.AreEqual(i, tree.Count);

                            long key = -1;
                            switch (insertOrder)
                            {
                                case InsertKeyOrder.Alternating:
                                    key = i * (i % 2 == 0 ? -1 : 1);
                                    break;
                                case InsertKeyOrder.Ascending:
                                    key = i;
                                    break;
                                case InsertKeyOrder.Descending:
                                    key = -i;
                                    break;

                                case InsertKeyOrder.Random:
                                    key = r.Next();
                                    while (usedKeys.Contains(key))
                                        key = r.Next();
                                    break;
                            }

                            usedKeys.Add(key);
                            usedKeysInOrder.Add(key);

                            Assert.IsFalse(tree.TryGetValue(key, out var mustNotExist, new CancellationToken(false)));
                            Assert.IsNull(mustNotExist);

                            Assert.IsTrue(tree.Insert(key, key.ToString(), false, out bool alreadyExists));
                            Assert.IsFalse(alreadyExists);

                            Assert.IsTrue(tree.TryGetValue(key, out var mustExist, new CancellationToken(false)));
                            Assert.AreEqual(key.ToString(), mustExist);
                            Assert.AreEqual(i + 1, tree.Count);
                        }

                        long allocatedPageCountAfterInsertion = storage.AllocatedPageCount;
                        Assert.IsTrue(allocatedPageCountAfterInsertion > allocatedPageCountBeforeInsertion);

                        //Remove half of the elements
                        for (long i = 0; i < keyValuePairCount / 2; i++)
                        {
                            long key = -1;
                            switch (removeOrder)
                            {
                                case RemoveKeyOrder.SameAsInsertion:
                                    key = usedKeysInOrder[(int)i];
                                    break;

                                case RemoveKeyOrder.OppositeOfInsertion:
                                    key = usedKeysInOrder[usedKeysInOrder.Count - (int)(i + 1)];
                                    break;

                                case RemoveKeyOrder.Random:
                                    long randIndex = r.Next();
                                    key = usedKeysInOrder[(int)randIndex % usedKeysInOrder.Count];
                                    usedKeysInOrder.RemoveAt((int)randIndex % usedKeysInOrder.Count);
                                    break;
                            }

                            Assert.IsTrue(tree.Remove(key, out var removedValue));
                            Assert.AreEqual(key.ToString(), removedValue);
                        }
                    }

                    //Reload the tree as read-only so we can validate its entire structure
                    using (var storage = StreamingPageStorage.Load(stream, true, true, true))
                    {
                        var tree = new MockBTree(storage, new MockLongSerializer(), new MockStringSerializer());
                        tree.Validate(new CancellationToken(false));
                    }
                }
            }
        }

        [TestMethod]
        public void RemoveWillNotCauseDeflation()
        {
            using (MemoryStream ms = new MemoryStream())
            {
                using (MockStream stream = new MockStream(ms, true, true, true))
                {
                    long pageSize = BTreeNode<long, string>.GetRequiredPageSize(MockLongSerializer.CDataSize, MockStringSerializer.CDataSize, BTreeNode<long, string>.VeryMinKeyValuePairCapacity);
                    using (StreamingPageStorage storage = StreamingPageStorage.Create(stream, pageSize, 1, null, new CancellationToken(false), true))
                    {
                        MockBTree tree = new MockBTree(storage, new MockLongSerializer(), new MockStringSerializer());

                        const long keyValuePairCount = 1024;
                        long allocatedPageCountBeforeInsertion = storage.AllocatedPageCount;
                        long pageCapacityBeforeInsertion = storage.PageCapacity;

                        //Add some elements
                        for (long i = 0; i < keyValuePairCount; i++)
                            Assert.IsTrue(tree.Insert(i, i.ToString(), false, out _));

                        long allocatedPageCountAfterInsertion = storage.AllocatedPageCount;
                        long pageCapacityAfterInsertion = storage.PageCapacity;
                        Assert.IsTrue(allocatedPageCountAfterInsertion > allocatedPageCountBeforeInsertion);
                        Assert.IsTrue(pageCapacityAfterInsertion > pageCapacityBeforeInsertion);

                        //Remove the elements
                        for (long i = 0; i < keyValuePairCount; i++)
                            Assert.IsTrue(tree.Remove(i, out _));

                        //Make sure the page capacity has NOT decreased
                        Assert.AreEqual(pageCapacityAfterInsertion, storage.PageCapacity);
                    }
                }
            }
        }

        [DataTestMethod]
        [DataRow(BTreeNode<long, string>.VeryMinKeyValuePairCapacity, 0)]
        [DataRow(BTreeNode<long, string>.VeryMinKeyValuePairCapacity, 1)]
        [DataRow(BTreeNode<long, string>.VeryMinKeyValuePairCapacity, 2)]
        [DataRow(BTreeNode<long, string>.VeryMinKeyValuePairCapacity, 3)]
        [DataRow(BTreeNode<long, string>.VeryMinKeyValuePairCapacity, 5)]
        [DataRow(BTreeNode<long, string>.VeryMinKeyValuePairCapacity, 6)]
        [DataRow(BTreeNode<long, string>.VeryMinKeyValuePairCapacity, 7)]
        [DataRow(BTreeNode<long, string>.VeryMinKeyValuePairCapacity, 15)]
        [DataRow(BTreeNode<long, string>.VeryMinKeyValuePairCapacity, 16)]
        [DataRow(BTreeNode<long, string>.VeryMinKeyValuePairCapacity, 17)]
        [DataRow(BTreeNode<long, string>.VeryMinKeyValuePairCapacity + 2, 0)]
        [DataRow(BTreeNode<long, string>.VeryMinKeyValuePairCapacity + 2, 1)]
        [DataRow(BTreeNode<long, string>.VeryMinKeyValuePairCapacity + 2, 2)]
        [DataRow(BTreeNode<long, string>.VeryMinKeyValuePairCapacity + 2, 3)]
        [DataRow(BTreeNode<long, string>.VeryMinKeyValuePairCapacity + 2, 15)]
        [DataRow(BTreeNode<long, string>.VeryMinKeyValuePairCapacity + 2, 16)]
        [DataRow(BTreeNode<long, string>.VeryMinKeyValuePairCapacity + 2, 17)]
        [DataRow(BTreeNode<long, string>.VeryMinKeyValuePairCapacity + 4, 0)]
        [DataRow(BTreeNode<long, string>.VeryMinKeyValuePairCapacity + 4, 1)]
        [DataRow(BTreeNode<long, string>.VeryMinKeyValuePairCapacity + 4, 2)]
        [DataRow(BTreeNode<long, string>.VeryMinKeyValuePairCapacity + 4, 3)]
        [DataRow(BTreeNode<long, string>.VeryMinKeyValuePairCapacity + 4, 15)]
        [DataRow(BTreeNode<long, string>.VeryMinKeyValuePairCapacity + 4, 16)]
        [DataRow(BTreeNode<long, string>.VeryMinKeyValuePairCapacity + 4, 17)]
        [DataRow(BTreeNode<long, string>.VeryMinKeyValuePairCapacity + 1024, 0)]
        [DataRow(BTreeNode<long, string>.VeryMinKeyValuePairCapacity + 1024, 1)]
        [DataRow(BTreeNode<long, string>.VeryMinKeyValuePairCapacity + 1024, 2)]
        [DataRow(BTreeNode<long, string>.VeryMinKeyValuePairCapacity + 1024, 3)]
        [DataRow(BTreeNode<long, string>.VeryMinKeyValuePairCapacity + 1024, 15)]
        [DataRow(BTreeNode<long, string>.VeryMinKeyValuePairCapacity + 1024, 16)]
        [DataRow(BTreeNode<long, string>.VeryMinKeyValuePairCapacity + 1024, 17)]
        [DataRow(BTreeNode<long, string>.VeryMinKeyValuePairCapacity + 1024, 1024)]
        public void AscendingTraversal(long keyValuePairCapacity, long count)
        {
            using (MemoryStream ms = new MemoryStream())
            {
                using (MockStream stream = new MockStream(ms, true, true, true))
                {
                    long pageSize = BTreeNode<long, string>.GetRequiredPageSize(MockLongSerializer.CDataSize, MockStringSerializer.CDataSize, keyValuePairCapacity);
                    using (StreamingPageStorage storage = StreamingPageStorage.Create(stream, pageSize, 3, null, new CancellationToken(false), true))
                    {
                        MockBTree tree = new MockBTree(storage, new MockLongSerializer(), new MockStringSerializer());

                        Random r = new Random(5);//Constant seed for consistent tests
                        HashSet<long> usedKeys = new HashSet<long>();
                        List<long> usedKeysInOrder = new List<long>();

                        //Add some elements
                        for (long i = 0; i < count; i++)
                        {
                            Assert.AreEqual(i, tree.Count);

                            long key = r.Next();
                            while (usedKeys.Contains(key))
                                key = r.Next();

                            usedKeys.Add(key);
                            usedKeysInOrder.Add(key);
                            Assert.IsTrue(tree.Insert(key, key.ToString(), false, out bool alreadyExists));
                            Assert.IsFalse(alreadyExists);
                        }

                        usedKeysInOrder = usedKeysInOrder.OrderBy(x => x).ToList();

                        //Traverse
                        long tI = 0;
                        foreach (var pair in tree.Traverse(true))
                        {
                            long expectedKey = usedKeysInOrder[(int)tI];
                            string expectedValue = expectedKey.ToString();
                            Assert.AreEqual(expectedKey, pair.Key);
                            Assert.AreEqual(expectedValue, pair.Value);
                            tI++;
                        }
                    }
                }
            }
        }
        
        [DataTestMethod]
        [DataRow(BTreeNode<long, string>.VeryMinKeyValuePairCapacity, 0)]
        [DataRow(BTreeNode<long, string>.VeryMinKeyValuePairCapacity, 1)]
        [DataRow(BTreeNode<long, string>.VeryMinKeyValuePairCapacity, 2)]
        [DataRow(BTreeNode<long, string>.VeryMinKeyValuePairCapacity, 3)]
        [DataRow(BTreeNode<long, string>.VeryMinKeyValuePairCapacity, 5)]
        [DataRow(BTreeNode<long, string>.VeryMinKeyValuePairCapacity, 6)]
        [DataRow(BTreeNode<long, string>.VeryMinKeyValuePairCapacity, 7)]
        [DataRow(BTreeNode<long, string>.VeryMinKeyValuePairCapacity, 15)]
        [DataRow(BTreeNode<long, string>.VeryMinKeyValuePairCapacity, 16)]
        [DataRow(BTreeNode<long, string>.VeryMinKeyValuePairCapacity, 17)]
        [DataRow(BTreeNode<long, string>.VeryMinKeyValuePairCapacity + 2, 0)]
        [DataRow(BTreeNode<long, string>.VeryMinKeyValuePairCapacity + 2, 1)]
        [DataRow(BTreeNode<long, string>.VeryMinKeyValuePairCapacity + 2, 2)]
        [DataRow(BTreeNode<long, string>.VeryMinKeyValuePairCapacity + 2, 3)]
        [DataRow(BTreeNode<long, string>.VeryMinKeyValuePairCapacity + 2, 15)]
        [DataRow(BTreeNode<long, string>.VeryMinKeyValuePairCapacity + 2, 16)]
        [DataRow(BTreeNode<long, string>.VeryMinKeyValuePairCapacity + 2, 17)]
        [DataRow(BTreeNode<long, string>.VeryMinKeyValuePairCapacity + 4, 0)]
        [DataRow(BTreeNode<long, string>.VeryMinKeyValuePairCapacity + 4, 1)]
        [DataRow(BTreeNode<long, string>.VeryMinKeyValuePairCapacity + 4, 2)]
        [DataRow(BTreeNode<long, string>.VeryMinKeyValuePairCapacity + 4, 3)]
        [DataRow(BTreeNode<long, string>.VeryMinKeyValuePairCapacity + 4, 15)]
        [DataRow(BTreeNode<long, string>.VeryMinKeyValuePairCapacity + 4, 16)]
        [DataRow(BTreeNode<long, string>.VeryMinKeyValuePairCapacity + 4, 17)]
        [DataRow(BTreeNode<long, string>.VeryMinKeyValuePairCapacity + 1024, 0)]
        [DataRow(BTreeNode<long, string>.VeryMinKeyValuePairCapacity + 1024, 1)]
        [DataRow(BTreeNode<long, string>.VeryMinKeyValuePairCapacity + 1024, 2)]
        [DataRow(BTreeNode<long, string>.VeryMinKeyValuePairCapacity + 1024, 3)]
        [DataRow(BTreeNode<long, string>.VeryMinKeyValuePairCapacity + 1024, 15)]
        [DataRow(BTreeNode<long, string>.VeryMinKeyValuePairCapacity + 1024, 16)]
        [DataRow(BTreeNode<long, string>.VeryMinKeyValuePairCapacity + 1024, 17)]
        [DataRow(BTreeNode<long, string>.VeryMinKeyValuePairCapacity + 1024, 1024)]
        public void DescendingTraversal(long keyValuePairCapacity, long count)
        {
            using (MemoryStream ms = new MemoryStream())
            {
                using (MockStream stream = new MockStream(ms, true, true, true))
                {
                    long pageSize = BTreeNode<long, string>.GetRequiredPageSize(MockLongSerializer.CDataSize, MockStringSerializer.CDataSize, keyValuePairCapacity);
                    using (StreamingPageStorage storage = StreamingPageStorage.Create(stream, pageSize, 3, null, new CancellationToken(false), true))
                    {
                        MockBTree tree = new MockBTree(storage, new MockLongSerializer(), new MockStringSerializer());

                        Random r = new Random(5);//Constant seed for consistent tests
                        HashSet<long> usedKeys = new HashSet<long>();
                        List<long> usedKeysInOrder = new List<long>();

                        //Add some elements
                        for (long i = 0; i < count; i++)
                        {
                            Assert.AreEqual(i, tree.Count);

                            long key = r.Next();
                            while (usedKeys.Contains(key))
                                key = r.Next();

                            usedKeys.Add(key);
                            usedKeysInOrder.Add(key);
                            Assert.IsTrue(tree.Insert(key, key.ToString(), false, out bool alreadyExists));
                            Assert.IsFalse(alreadyExists);
                        }

                        usedKeysInOrder = usedKeysInOrder.OrderByDescending(x => x).ToList();

                        //Traverse
                        long tI = 0;
                        foreach (var pair in tree.Traverse(false))
                        {
                            long expectedKey = usedKeysInOrder[(int)tI];
                            string expectedValue = expectedKey.ToString();
                            Assert.AreEqual(expectedKey, pair.Key);
                            Assert.AreEqual(expectedValue, pair.Value);
                            tI++;
                        }
                    }
                }
            }
        }

        [DataTestMethod]
        [DataRow(true)]
        [DataRow(false)]
        public void CannotInsertWhileTraversing(bool ascending)
        {
            using (MemoryStream ms = new MemoryStream())
            {
                using (MockStream stream = new MockStream(ms, true, true, true))
                {
                    long pageSize = BTreeNode<long, string>.GetRequiredPageSize(MockLongSerializer.CDataSize, MockStringSerializer.CDataSize, BTreeNode<long, string>.VeryMinKeyValuePairCapacity);
                    using (StreamingPageStorage storage = StreamingPageStorage.Create(stream, pageSize, 1, null, new CancellationToken(false), true))
                    {
                        MockBTree tree = new MockBTree(storage, new MockLongSerializer(), new MockStringSerializer());

                        const long count = 128;

                        //Add some elements
                        for (long i = 0; i < count; i++)
                            Assert.IsTrue(tree.Insert(i, i.ToString(), false, out _));

                        foreach (var pair in tree.Traverse(ascending))
                        {
                            Assert.ThrowsException<InvalidOperationException>(() => {
                                tree.Insert(-1, "New Value", true, out _);
                            });
                        }
                    }
                }
            }
        }

        [DataTestMethod]
        [DataRow(true)]
        [DataRow(false)]
        public void CannotRemoveWhileTraversing(bool ascending)
        {
            using (MemoryStream ms = new MemoryStream())
            {
                using (MockStream stream = new MockStream(ms, true, true, true))
                {
                    long pageSize = BTreeNode<long, string>.GetRequiredPageSize(MockLongSerializer.CDataSize, MockStringSerializer.CDataSize, BTreeNode<long, string>.VeryMinKeyValuePairCapacity);
                    using (StreamingPageStorage storage = StreamingPageStorage.Create(stream, pageSize, 1, null, new CancellationToken(false), true))
                    {
                        MockBTree tree = new MockBTree(storage, new MockLongSerializer(), new MockStringSerializer());

                        const long count = 128;

                        //Add some elements
                        for (long i = 0; i < count; i++)
                            Assert.IsTrue(tree.Insert(i, i.ToString(), false, out _));

                        foreach (var pair in tree.Traverse(ascending))
                        {
                            Assert.ThrowsException<InvalidOperationException>(() => {
                                tree.Remove(pair.Key, out _);
                            });
                        }
                    }
                }
            }
        }

        [TestMethod]
        public void ValidateThrowsWhenNotReadOnly()
        {
            using (MemoryStream ms = new MemoryStream())
            {
                using (MockStream stream = new MockStream(ms, true, true, true))
                {
                    long pageSize = BTreeNode<long, string>.GetRequiredPageSize(MockLongSerializer.CDataSize, MockStringSerializer.CDataSize, BTreeNode<long, string>.VeryMinKeyValuePairCapacity);
                    using (StreamingPageStorage storage = StreamingPageStorage.Create(stream, pageSize, 1, null, new CancellationToken(false), true))
                    {
                        MockBTree tree = new MockBTree(storage, new MockLongSerializer(), new MockStringSerializer());
                        Assert.ThrowsException<InvalidOperationException>(() => {
                            tree.Validate(new CancellationToken(false));
                        });
                    }
                }
            }
        }
    }
}
