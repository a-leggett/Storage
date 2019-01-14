using Microsoft.VisualStudio.TestTools.UnitTesting;
using Storage;
using Storage.Algorithms;
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
    public class BTreeNodeTest
    {
        private static void InitializeGarbagePayload(Stream stream, byte value)
        {
            byte[] buffer = new byte[stream.Length];
            for (int i = 0; i < buffer.Length; i++)
                buffer[i] = value;
            stream.Position = 0;
            stream.Write(buffer, 0, buffer.Length);
            stream.Flush();
        }

        [DataTestMethod]
        [DataRow(-1, 128, 7)]
        [DataRow(0, 128, 7)]
        [DataRow(1, -1, 7)]
        [DataRow(1, 128, BTreeNode<long, string>.VeryMinKeyValuePairCapacity - 1)]
        public void GetRequiredPageSizeThrowsWhenArgIsOutOfRange(long keySize, long valueSize, long keyValuePairCapacity)
        {
            Assert.ThrowsException<ArgumentOutOfRangeException>(()=> {
                BTreeNode<long, string>.GetRequiredPageSize(keySize, valueSize, keyValuePairCapacity);
            });
        }

        [DataTestMethod]
        [DataRow(6)]
        [DataRow(8)]
        [DataRow(10)]
        [DataRow(12)]
        [DataRow(14)]
        [DataRow(16)]
        [DataRow(18)]
        public void GetRequiredPageSizeThrowsWhenCapacityIsNotOdd(long capacity)
        {
            Assert.ThrowsException<ArgumentException>(()=> {
                BTreeNode<long, string>.GetRequiredPageSize(128, 128, capacity);
            });
        }
        
        [DataTestMethod]
        [DataRow(-1, 128)]
        [DataRow(0, 128)]
        [DataRow(128, -1)]
        public void GetKeyValuePairCapacityForPageSizeThrowsWhenArgOutOfRange(long keySize, long valueSize)
        {
            Assert.ThrowsException<ArgumentOutOfRangeException>(()=> {
                BTreeNode<long, string>.GetKeyValuePairCapacityForPageSize(102400, keySize, valueSize);
            });
        }

        [DataTestMethod]
        [DataRow(1, 0, 1)]
        [DataRow(1, 1, 1)]
        [DataRow(1, 2, 1)]
        [DataRow(1, 3, 1)]
        [DataRow(1, 128, 1)]
        [DataRow(2, 128, 1)]
        [DataRow(3, 128, 1)]
        [DataRow(4, 128, 1)]
        [DataRow(5, 128, 1)]
        [DataRow(128, 128, 1)]
        [DataRow(1, 0, 2)]
        [DataRow(1, 1, 2)]
        [DataRow(1, 2, 2)]
        [DataRow(1, 3, 2)]
        [DataRow(1, 128, 2)]
        [DataRow(2, 128, 2)]
        [DataRow(3, 128, 2)]
        [DataRow(4, 128, 2)]
        [DataRow(5, 128, 2)]
        [DataRow(128, 128, 2)]
        public void GetKeyValuePairCapacityForPageSizeReturnsZeroWhenInsufficient(long keySize, long valueSize, long bytesBelowMin)
        {
            long veryMin = BTreeNode<long, string>.GetRequiredPageSize(keySize, valueSize, BTreeNode<long, string>.VeryMinKeyValuePairCapacity);
            Assert.AreEqual(0, BTreeNode<long, string>.GetKeyValuePairCapacityForPageSize(veryMin - bytesBelowMin, keySize, valueSize));
        }

        [DataTestMethod]
        [DataRow(4, 9, BTreeNode<long, string>.VeryMinKeyValuePairCapacity, 1)]
        [DataRow(4, 9, BTreeNode<long, string>.VeryMinKeyValuePairCapacity, 2)]
        [DataRow(4, 9, BTreeNode<long, string>.VeryMinKeyValuePairCapacity, 3)]
        [DataRow(4, 9, BTreeNode<long, string>.VeryMinKeyValuePairCapacity, 4)]
        [DataRow(4, 9, BTreeNode<long, string>.VeryMinKeyValuePairCapacity + 2, 1)]
        [DataRow(4, 9, BTreeNode<long, string>.VeryMinKeyValuePairCapacity + 2, 2)]
        [DataRow(4, 9, BTreeNode<long, string>.VeryMinKeyValuePairCapacity + 2, 3)]
        [DataRow(4, 9, BTreeNode<long, string>.VeryMinKeyValuePairCapacity + 2, 4)]
        [DataRow(4, 9, BTreeNode<long, string>.VeryMinKeyValuePairCapacity + 4, 1)]
        [DataRow(4, 9, BTreeNode<long, string>.VeryMinKeyValuePairCapacity + 4, 2)]
        [DataRow(4, 9, BTreeNode<long, string>.VeryMinKeyValuePairCapacity + 4, 3)]
        [DataRow(4, 9, BTreeNode<long, string>.VeryMinKeyValuePairCapacity + 4, 4)]
        public void GetKeyValuePairCapacityForPageSizeRoundsDownExcess(long keySize, long valueSize, long capacity, long excessBytes)
        {
            long size = BTreeNode<long, string>.GetRequiredPageSize(keySize, valueSize, capacity) + excessBytes;
            Assert.AreEqual(capacity, BTreeNode<long, string>.GetKeyValuePairCapacityForPageSize(size, keySize, valueSize));
        }

        [TestMethod]
        public void TryCreateNewThrowsWhenTreeIsNull()
        {
            Assert.ThrowsException<ArgumentNullException>(()=> {
                BTreeNode<long, string>.TryCreateNew(null, false, out _);
            });
        }

        [TestMethod]
        public void TryCreateNewThrowsWhenTreeIsReadOnly()
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
                    MockBTree bTree = new MockBTree(storage, new MockLongSerializer(), new MockStringSerializer());

                    Assert.ThrowsException<InvalidOperationException>(() => {
                        BTreeNode<long, string>.TryCreateNew(bTree, false, out _);
                    });
                }
            }
        }

        [DataTestMethod]
        [DataRow(false, (byte)0x00)]
        [DataRow(true, (byte)0x00)]
        [DataRow(false, (byte)0x11)]
        [DataRow(true, (byte)0x11)]
        [DataRow(false, (byte)0xCC)]
        [DataRow(true, (byte)0xCC)]
        [DataRow(false, (byte)0xFF)]
        [DataRow(true, (byte)0xFF)]
        public void TryCreateNewWorks(bool isLeaf, byte garbagePayloadValue)
        {
            using (MemoryStream ms = new MemoryStream())
            {
                using (var storage = StreamingPageStorage.Create(ms, 10240, 1, null, new CancellationToken(false), true))
                {
                    MockBTree bTree = new MockBTree(storage, new MockLongSerializer(), new MockStringSerializer());
                    
                    Assert.IsTrue(BTreeNode<long, string>.TryCreateNew(bTree, isLeaf, out var node));
                    Assert.AreEqual(isLeaf, node.IsLeaf);
                    Assert.AreEqual(0, node.KeyValuePairCount);

                    //Load the node from storage (rather than using the instance from creation)
                    node = new BTreeNode<long, string>(bTree, node.PageIndex);
                    Assert.AreEqual(isLeaf, node.IsLeaf);
                    Assert.AreEqual(0, node.KeyValuePairCount);
                }
            }
        }

        [TestMethod]
        public void TryCreateNewFailsWhenCapacityIsFullAndFixed()
        {
            using (MemoryStream ms = new MemoryStream())
            {
                const long pageSize = 10240;
                ms.SetLength(StreamingPageStorage.GetRequiredStreamSize(pageSize, 2));
                using (var storage = StreamingPageStorage.CreateFixed(ms, pageSize, null, new CancellationToken(false), true))
                {
                    MockBTree bTree = new MockBTree(storage, new MockLongSerializer(), new MockStringSerializer());

                    //Take away the only page
                    Assert.IsTrue(storage.TryAllocatePage(out _));

                    //Ensure that we cannot create a new node
                    Assert.IsFalse(BTreeNode<long, string>.TryCreateNew(bTree, true, out var node));
                    Assert.IsNull(node);
                }
            }
        }

        [TestMethod]
        public void TryCreateNewCausesInflationWhenNecessary()
        {
            using (MemoryStream ms = new MemoryStream())
            {
                using (var storage = StreamingPageStorage.Create(ms, 10240, 1, null, new CancellationToken(false), true))
                {
                    MockBTree bTree = new MockBTree(storage, new MockLongSerializer(), new MockStringSerializer());

                    //Ensure that the IPageStorage is at full capacity
                    Assert.AreEqual(storage.PageCapacity, storage.AllocatedPageCount);
                    long initCapacity = storage.PageCapacity;

                    //Allocate a node
                    Assert.IsTrue(BTreeNode<long, string>.TryCreateNew(bTree, true, out var node));
                    Assert.AreEqual(initCapacity + 1, storage.PageCapacity);
                }
            }
        }

        [TestMethod]
        public void TryCreateNewFailsWhenInflationFails()
        {
            using (MemoryStream ms = new MemoryStream())
            {
                var stream = new MockSafeResizableStorage(ms, true, true, true, null, long.MaxValue);
                using (var storage = StreamingPageStorage.Create(stream, 102400, 1, null, new CancellationToken(false), true))
                {
                    MockBTree bTree = new MockBTree(storage, new MockLongSerializer(), new MockStringSerializer());

                    //Ensure that the IPageStorage is at full capacity
                    Assert.AreEqual(storage.PageCapacity, storage.AllocatedPageCount);
                    long initCapacity = storage.PageCapacity;

                    //Make sure the next inflation fails
                    stream.ForceTrySetSizeFail = true;

                    //Try to allocate a node
                    Assert.IsFalse(BTreeNode<long, string>.TryCreateNew(bTree, true, out var node));
                    Assert.IsNull(node);
                    Assert.AreEqual(initCapacity, storage.PageCapacity);
                }
            }
        }

        [DataTestMethod]
        [DataRow(0, -1)]
        [DataRow(0, 0)]
        [DataRow(0, 1)]
        [DataRow(1, -1)]
        [DataRow(1, 1)]
        [DataRow(2, -1)]
        [DataRow(2, 2)]
        [DataRow(2, 3)]
        [DataRow(3, -1)]
        [DataRow(3, 3)]
        [DataRow(3, 4)]
        public void GetKeyAtThrowsWhenIndexIsOutOfRange(long count, long index)
        {
            using (MemoryStream ms = new MemoryStream())
            {
                using (var storage = StreamingPageStorage.Create(ms, 10240, 1, null, new CancellationToken(false), true))
                {
                    MockBTree bTree = new MockBTree(storage, new MockLongSerializer(), new MockStringSerializer());

                    //Allocate a node
                    Assert.IsTrue(BTreeNode<long, string>.TryCreateNew(bTree, true, out var node));

                    //Initialize the count
                    node.KeyValuePairCount = count;

                    Assert.ThrowsException<ArgumentOutOfRangeException>(() =>
                    {
                        node.GetKeyAt(index);
                    });
                }
            }
        }

        [TestMethod]
        public void SetKeyAtThrowsWhenReadOnly()
        {
            using (MemoryStream ms = new MemoryStream())
            {
                long nodeIndex;
                using (var storage = StreamingPageStorage.Create(ms, 10240, 1, null, new CancellationToken(false), true))
                {
                    //Initialize the mock B-Tree while 'storage' is writable
                    MockBTree bTree = new MockBTree(storage, new MockLongSerializer(), new MockStringSerializer());

                    //Allocate a node
                    Assert.IsTrue(BTreeNode<long, string>.TryCreateNew(bTree, true, out var node));
                    nodeIndex = node.PageIndex;

                    //Initialize the count
                    node.KeyValuePairCount = 5;
                }

                //Re-load the storage as read-only
                using (var storage = StreamingPageStorage.Load(ms, true, true, true))
                {
                    //Re-load the MockBTree as read-only (determined by the storage being read-only)
                    MockBTree bTree = new MockBTree(storage, new MockLongSerializer(), new MockStringSerializer());

                    //Re-load the node
                    var node = new BTreeNode<long, string>(bTree, nodeIndex);

                    Assert.ThrowsException<InvalidOperationException>(() => {
                        node.SetKeyAt(0, 0);
                    });
                }
            }
        }

        [DataTestMethod]
        [DataRow(0, -1)]
        [DataRow(0, 0)]
        [DataRow(0, 1)]
        [DataRow(1, -1)]
        [DataRow(1, 1)]
        [DataRow(2, -1)]
        [DataRow(2, 2)]
        [DataRow(2, 3)]
        [DataRow(3, -1)]
        [DataRow(3, 3)]
        [DataRow(3, 4)]
        public void SetKeyAtThrowsWhenIndexIsOutOfRange(long count, long index)
        {
            using (MemoryStream ms = new MemoryStream())
            {
                using (var storage = StreamingPageStorage.Create(ms, 10240, 1, null, new CancellationToken(false), true))
                {
                    MockBTree bTree = new MockBTree(storage, new MockLongSerializer(), new MockStringSerializer());

                    //Allocate a node
                    Assert.IsTrue(BTreeNode<long, string>.TryCreateNew(bTree, true, out var node));

                    //Initialize the count
                    node.KeyValuePairCount = count;

                    Assert.ThrowsException<ArgumentOutOfRangeException>(() =>
                    {
                        node.SetKeyAt(index, 0);
                    });
                }
            }
        }

        [DataTestMethod]
        [DataRow(5, 0, false)]
        [DataRow(5, 4, false)]
        [DataRow(5, 5, false)]
        [DataRow(1023, 0, false)]
        [DataRow(1023, 1, false)]
        [DataRow(1023, 2, false)]
        [DataRow(1023, 3, false)]
        [DataRow(1023, 1022, false)]
        [DataRow(1023, 1023, false)]
        [DataRow(5, 0, true)]
        [DataRow(5, 4, true)]
        [DataRow(5, 5, true)]
        [DataRow(1023, 0, true)]
        [DataRow(1023, 1, true)]
        [DataRow(1023, 2, true)]
        [DataRow(1023, 3, true)]
        [DataRow(1023, 1022, true)]
        [DataRow(1023, 1023, true)]
        public void KeyStorageWorks(long capacity, long count, bool isLeaf)
        {
            using (MemoryStream ms = new MemoryStream())
            {
                using (var storage = StreamingPageStorage.Create(ms, BTreeNode<long, string>.GetRequiredPageSize(MockLongSerializer.CDataSize, MockStringSerializer.CDataSize, capacity), 1, null, new CancellationToken(false), true))
                {
                    MockBTree bTree = new MockBTree(storage, new MockLongSerializer(), new MockStringSerializer());

                    //Allocate a node
                    Assert.IsTrue(BTreeNode<long, string>.TryCreateNew(bTree, isLeaf, out var node));

                    //Initialize the count
                    node.KeyValuePairCount = count;
                    Assert.AreEqual(count, node.KeyValuePairCount);

                    long[] expected = new long[count];
                    for (long i = 0; i < count; i++)
                        expected[i] = i * count;

                    //Assign some keys
                    for (long i = 0; i < count; i++)
                        node.SetKeyAt(i, expected[i]);

                    //Read the keys
                    for (long i = 0; i < count; i++)
                        Assert.AreEqual(expected[i], node.GetKeyAt(i));
                    CollectionAssert.AreEquivalent(expected, node.Keys.ToArray());

                    //Re-load the node
                    node = new BTreeNode<long, string>(bTree, node.PageIndex);

                    //Read the keys again
                    for (long i = 0; i < count; i++)
                        Assert.AreEqual(expected[i], node.GetKeyAt(i));
                    CollectionAssert.AreEquivalent(expected, node.Keys.ToArray());
                }
            }
        }

        [DataTestMethod]
        [DataRow(0, -1)]
        [DataRow(0, 0)]
        [DataRow(0, 1)]
        [DataRow(1, -1)]
        [DataRow(1, 1)]
        [DataRow(2, -1)]
        [DataRow(2, 2)]
        [DataRow(2, 3)]
        [DataRow(3, -1)]
        [DataRow(3, 3)]
        [DataRow(3, 4)]
        public void GetValueAtThrowsWhenIndexIsOutOfRange(long count, long index)
        {
            using (MemoryStream ms = new MemoryStream())
            {
                using (var storage = StreamingPageStorage.Create(ms, 10240, 1, null, new CancellationToken(false), true))
                {
                    MockBTree bTree = new MockBTree(storage, new MockLongSerializer(), new MockStringSerializer());

                    //Allocate a node
                    Assert.IsTrue(BTreeNode<long, string>.TryCreateNew(bTree, true, out var node));

                    //Initialize the count
                    node.KeyValuePairCount = count;

                    Assert.ThrowsException<ArgumentOutOfRangeException>(() =>
                    {
                        node.GetValueAt(index);
                    });
                }
            }
        }

        [TestMethod]
        public void SetValueAtThrowsWhenReadOnly()
        {
            using (MemoryStream ms = new MemoryStream())
            {
                long nodeIndex;
                using (var storage = StreamingPageStorage.Create(ms, 10240, 1, null, new CancellationToken(false), true))
                {
                    //Initialize the mock B-Tree while 'storage' is writable
                    MockBTree bTree = new MockBTree(storage, new MockLongSerializer(), new MockStringSerializer());

                    //Allocate a node
                    Assert.IsTrue(BTreeNode<long, string>.TryCreateNew(bTree, true, out var node));
                    nodeIndex = node.PageIndex;

                    //Initialize the count
                    node.KeyValuePairCount = 5;
                }

                //Re-load the storage as read-only
                using (var storage = StreamingPageStorage.Load(ms, true, true, true))
                {
                    //Re-load the MockBTree as read-only (determined by the storage being read-only)
                    var bTree = new MockBTree(storage, new MockLongSerializer(), new MockStringSerializer());

                    //Re-load the node
                    var node = new BTreeNode<long, string>(bTree, nodeIndex);

                    Assert.ThrowsException<InvalidOperationException>(() => {
                        node.SetValueAt(0, "Value");
                    });
                }
            }
        }

        [DataTestMethod]
        [DataRow(0, -1)]
        [DataRow(0, 0)]
        [DataRow(0, 1)]
        [DataRow(1, -1)]
        [DataRow(1, 1)]
        [DataRow(2, -1)]
        [DataRow(2, 2)]
        [DataRow(2, 3)]
        [DataRow(3, -1)]
        [DataRow(3, 3)]
        [DataRow(3, 4)]
        public void SetValueAtThrowsWhenIndexIsOutOfRange(long count, long index)
        {
            using (MemoryStream ms = new MemoryStream())
            {
                using (var storage = StreamingPageStorage.Create(ms, 10240, 1, null, new CancellationToken(false), true))
                {
                    MockBTree bTree = new MockBTree(storage, new MockLongSerializer(), new MockStringSerializer());

                    //Allocate a node
                    Assert.IsTrue(BTreeNode<long, string>.TryCreateNew(bTree, true, out var node));

                    //Initialize the count
                    node.KeyValuePairCount = count;

                    Assert.ThrowsException<ArgumentOutOfRangeException>(() =>
                    {
                        node.SetValueAt(index, "Value");
                    });
                }
            }
        }

        [DataTestMethod]
        [DataRow(5, 0, false)]
        [DataRow(5, 1, false)]
        [DataRow(5, 2, false)]
        [DataRow(5, 3, false)]
        [DataRow(5, 4, false)]
        [DataRow(5, 5, false)]
        [DataRow(7, 0, false)]
        [DataRow(7, 0, false)]
        [DataRow(7, 1, false)]
        [DataRow(7, 2, false)]
        [DataRow(7, 3, false)]
        [DataRow(7, 4, false)]
        [DataRow(7, 5, false)]
        [DataRow(7, 6, false)]
        [DataRow(7, 7, false)]
        [DataRow(1023, 0, false)]
        [DataRow(1023, 1, false)]
        [DataRow(1023, 2, false)]
        [DataRow(1023, 3, false)]
        [DataRow(1023, 4, false)]
        [DataRow(1023, 5, false)]
        [DataRow(1023, 1022, false)]
        [DataRow(1023, 1023, false)]

        [DataRow(5, 0, true)]
        [DataRow(5, 1, true)]
        [DataRow(5, 2, true)]
        [DataRow(5, 3, true)]
        [DataRow(5, 4, true)]
        [DataRow(5, 5, true)]
        [DataRow(7, 0, true)]
        [DataRow(7, 1, true)]
        [DataRow(7, 2, true)]
        [DataRow(7, 3, true)]
        [DataRow(7, 4, true)]
        [DataRow(7, 5, true)]
        [DataRow(7, 6, true)]
        [DataRow(7, 7, true)]
        [DataRow(1023, 0, true)]
        [DataRow(1023, 1, true)]
        [DataRow(1023, 2, true)]
        [DataRow(1023, 3, true)]
        [DataRow(1023, 4, true)]
        [DataRow(1023, 5, true)]
        [DataRow(1023, 1022, true)]
        [DataRow(1023, 1023, true)]
        public void ValueStorageWorks(long capacity, long count, bool isLeaf)
        {
            using (MemoryStream ms = new MemoryStream())
            {
                using (var storage = StreamingPageStorage.Create(ms, BTreeNode<long, string>.GetRequiredPageSize(MockLongSerializer.CDataSize, MockStringSerializer.CDataSize, capacity), 1, null, new CancellationToken(false), true))
                {
                    MockBTree bTree = new MockBTree(storage, new MockLongSerializer(), new MockStringSerializer());

                    //Allocate a node
                    Assert.IsTrue(BTreeNode<long, string>.TryCreateNew(bTree, isLeaf, out var node));

                    //Initialize the count
                    node.KeyValuePairCount = count;
                    Assert.AreEqual(count, node.KeyValuePairCount);

                    string[] expected = new string[count];
                    for (long i = 0; i < count; i++)
                        expected[i] = i.ToString();

                    //Assign some values
                    for (long i = 0; i < count; i++)
                        node.SetValueAt(i, expected[i]);

                    //Read the values
                    for (long i = 0; i < count; i++)
                        Assert.AreEqual(expected[i], node.GetValueAt(i));
                    CollectionAssert.AreEquivalent(expected, node.Values.ToArray());

                    //Re-load the node
                    node = new BTreeNode<long, string>(bTree, node.PageIndex);

                    //Read the values again
                    for (long i = 0; i < count; i++)
                        Assert.AreEqual(expected[i], node.GetValueAt(i));
                    CollectionAssert.AreEquivalent(expected, node.Values.ToArray());
                }
            }
        }

        [DataTestMethod]
        [DataRow(5, 0, false)]
        [DataRow(5, 4, false)]
        [DataRow(5, 5, false)]
        [DataRow(1023, 0, false)]
        [DataRow(1023, 1, false)]
        [DataRow(1023, 2, false)]
        [DataRow(1023, 3, false)]
        [DataRow(1023, 1022, false)]
        [DataRow(1023, 1023, false)]
        [DataRow(5, 0, true)]
        [DataRow(5, 4, true)]
        [DataRow(5, 5, true)]
        [DataRow(1023, 0, true)]
        [DataRow(1023, 1, true)]
        [DataRow(1023, 2, true)]
        [DataRow(1023, 3, true)]
        [DataRow(1023, 1022, true)]
        [DataRow(1023, 1023, true)]
        public void KeyValuePairEnumeratorWorks(long capacity, long count, bool isLeaf)
        {
            using (MemoryStream ms = new MemoryStream())
            {
                using (var storage = StreamingPageStorage.Create(ms, BTreeNode<long, string>.GetRequiredPageSize(MockLongSerializer.CDataSize, MockStringSerializer.CDataSize, capacity), 1, null, new CancellationToken(false), true))
                {
                    MockBTree bTree = new MockBTree(storage, new MockLongSerializer(), new MockStringSerializer());

                    //Allocate a node
                    Assert.IsTrue(BTreeNode<long, string>.TryCreateNew(bTree, isLeaf, out var node));

                    //Initialize the count
                    node.KeyValuePairCount = count;
                    Assert.AreEqual(count, node.KeyValuePairCount);

                    KeyValuePair<long, string>[] expected = new KeyValuePair<long, string>[count];
                    for (long i = 0; i < count; i++)
                        expected[i] = new KeyValuePair<long, string>(i * count, i.ToString());

                    //Assign some key-value pairs
                    for (long i = 0; i < count; i++)
                    {
                        node.SetKeyAt(i, expected[i].Key);
                        node.SetValueAt(i, expected[i].Value);
                    }

                    //Read the pairs
                    for (long i = 0; i < count; i++)
                    {
                        Assert.AreEqual(expected[i].Key, node.GetKeyAt(i));
                        Assert.AreEqual(expected[i].Value, node.GetValueAt(i));
                    }

                    CollectionAssert.AreEquivalent(expected, node.ToArray());

                    //Re-load the node
                    node = new BTreeNode<long, string>(bTree, node.PageIndex);

                    //Read the pairs again
                    for (long i = 0; i < count; i++)
                    {
                        Assert.AreEqual(expected[i].Key, node.GetKeyAt(i));
                        Assert.AreEqual(expected[i].Value, node.GetValueAt(i));
                    }

                    CollectionAssert.AreEquivalent(expected, node.ToArray());
                }
            }
        }
        
        [DataTestMethod]
        [DataRow(0, -1)]
        [DataRow(0, 0)]
        [DataRow(0, 1)]
        [DataRow(1, -1)]
        [DataRow(1, 1)]
        [DataRow(2, -1)]
        [DataRow(2, 2)]
        [DataRow(2, 3)]
        [DataRow(3, -1)]
        [DataRow(3, 3)]
        [DataRow(3, 4)]
        public void GetSubTreeAtThrowsWhenIndexIsOutOfRange(long count, long index)
        {
            using (MemoryStream ms = new MemoryStream())
            {
                using (var storage = StreamingPageStorage.Create(ms, 10240, 1, null, new CancellationToken(false), true))
                {
                    MockBTree bTree = new MockBTree(storage, new MockLongSerializer(), new MockStringSerializer());

                    //Allocate a node
                    Assert.IsTrue(BTreeNode<long, string>.TryCreateNew(bTree, true, out var node));

                    //Initialize the count
                    node.KeyValuePairCount = count;

                    Assert.ThrowsException<ArgumentOutOfRangeException>(() =>
                    {
                        node.GetSubTreeAt(index);
                    });
                }
            }
        }

        [TestMethod]
        public void SetSubTreeAtThrowsWhenReadOnly()
        {
            using (MemoryStream ms = new MemoryStream())
            {
                long nodeIndex, subNodeIndex;
                using (var storage = StreamingPageStorage.Create(ms, 10240, 1, null, new CancellationToken(false), true))
                {
                    //Initialize the mock B-Tree while 'storage' is writable
                    MockBTree bTree = new MockBTree(storage, new MockLongSerializer(), new MockStringSerializer());

                    //Allocate a node
                    Assert.IsTrue(BTreeNode<long, string>.TryCreateNew(bTree, false, out var node));
                    nodeIndex = node.PageIndex;

                    //Initialize the count
                    node.KeyValuePairCount = 5;

                    //Allocate the 'sub-tree' node
                    Assert.IsTrue(BTreeNode<long, string>.TryCreateNew(bTree, true, out node));
                    subNodeIndex = node.PageIndex;
                }

                //Re-load the storage as read-only
                using (var storage = StreamingPageStorage.Load(ms, true, true, true))
                {
                    //Re-load the MockBTree as read-only (determined by the storage being read-only)
                    var bTree = new MockBTree(storage, new MockLongSerializer(), new MockStringSerializer());

                    //Re-load the node
                    var node = new BTreeNode<long, string>(bTree, nodeIndex);
                    var subTreeNode = new BTreeNode<long, string>(bTree, subNodeIndex);

                    Assert.ThrowsException<InvalidOperationException>(() => {
                        node.SetSubTreeAt(0, subTreeNode);
                    });
                }
            }
        }

        [DataTestMethod]
        [DataRow(0, -1)]
        [DataRow(0, 0)]
        [DataRow(0, 1)]
        [DataRow(1, -1)]
        [DataRow(1, 1)]
        [DataRow(2, -1)]
        [DataRow(2, 2)]
        [DataRow(2, 3)]
        [DataRow(3, -1)]
        [DataRow(3, 3)]
        [DataRow(3, 4)]
        public void SetSubTreeAtThrowsWhenIndexIsOutOfRange(long count, long index)
        {
            using (MemoryStream ms = new MemoryStream())
            {
                using (var storage = StreamingPageStorage.Create(ms, 10240, 1, null, new CancellationToken(false), true))
                {
                    MockBTree bTree = new MockBTree(storage, new MockLongSerializer(), new MockStringSerializer());

                    //Allocate a node
                    Assert.IsTrue(BTreeNode<long, string>.TryCreateNew(bTree, true, out var node));

                    //Initialize the count
                    node.KeyValuePairCount = count;

                    //Allocate a sub-tree node
                    Assert.IsTrue(BTreeNode<long, string>.TryCreateNew(bTree, true, out var subTreeNode));

                    Assert.ThrowsException<ArgumentOutOfRangeException>(() =>
                    {
                        node.SetSubTreeAt(index, subTreeNode);
                    });
                }
            }
        }

        [DataTestMethod]
        [DataRow(5, 0)]
        [DataRow(5, 1)]
        [DataRow(5, 2)]
        [DataRow(5, 3)]
        [DataRow(5, 4)]
        [DataRow(5, 5)]
        [DataRow(7, 0)]
        [DataRow(7, 1)]
        [DataRow(7, 2)]
        [DataRow(7, 3)]
        [DataRow(7, 4)]
        [DataRow(7, 5)]
        [DataRow(7, 6)]
        [DataRow(7, 7)]
        [DataRow(1023, 0)]
        [DataRow(1023, 1)]
        [DataRow(1023, 2)]
        [DataRow(1023, 3)]
        [DataRow(1023, 1022)]
        [DataRow(1023, 1023)]
        public void SubTreeStorageWorks(long keyValuePairCapacity, long count)
        {
            using (MemoryStream ms = new MemoryStream())
            {
                using (var storage = StreamingPageStorage.Create(ms, BTreeNode<long, string>.GetRequiredPageSize(MockLongSerializer.CDataSize, MockStringSerializer.CDataSize, keyValuePairCapacity), 1, null, new CancellationToken(false), true))
                {
                    MockBTree bTree = new MockBTree(storage, new MockLongSerializer(), new MockStringSerializer());

                    //Allocate a node
                    Assert.IsTrue(BTreeNode<long, string>.TryCreateNew(bTree, false, out var node));

                    //Initialize the count
                    node.KeyValuePairCount = count;
                    Assert.AreEqual(count, node.KeyValuePairCount);
                    Assert.AreEqual(count + 1, node.SubTreeCount);

                    //Assign some sub-trees
                    long[] expectedPSIndices = new long[count];
                    for (long i = 0; i < count; i++)
                    {
                        //Allocate a node
                        Assert.IsTrue(BTreeNode<long, string>.TryCreateNew(bTree, false, out var subTreeRoot));
                        expectedPSIndices[i] = subTreeRoot.PageIndex;
                        node.SetSubTreeAt(i, subTreeRoot);
                    }

                    //Read the values
                    for (long i = 0; i < count; i++)
                        Assert.AreEqual(expectedPSIndices[i], node.GetSubTreeAt(i).PageIndex);

                    //Re-load the node
                    node = new BTreeNode<long, string>(bTree, node.PageIndex);

                    //Read the values again
                    for (long i = 0; i < count; i++)
                        Assert.AreEqual(expectedPSIndices[i], node.GetSubTreeAt(i).PageIndex);
                }
            }
        }

        [DataTestMethod]
        [DataRow(5, 1, true)]
        [DataRow(5, 2, true)]
        [DataRow(5, 3, true)]
        [DataRow(5, 4, true)]
        [DataRow(5, 5, true)]
        [DataRow(9, 1, true)]
        [DataRow(9, 2, true)]
        [DataRow(9, 3, true)]
        [DataRow(9, 4, true)]
        [DataRow(9, 5, true)]
        [DataRow(9, 7, true)]
        [DataRow(9, 9, true)]
        [DataRow(1023, 9, true)]
        [DataRow(1023, 1022, true)]
        [DataRow(1023, 1023, true)]
        [DataRow(5, 1, false)]
        [DataRow(5, 2, false)]
        [DataRow(5, 3, false)]
        [DataRow(5, 4, false)]
        [DataRow(5, 5, false)]
        [DataRow(9, 1, false)]
        [DataRow(9, 2, false)]
        [DataRow(9, 3, false)]
        [DataRow(9, 4, false)]
        [DataRow(9, 5, false)]
        [DataRow(9, 7, false)]
        [DataRow(9, 9, false)]
        [DataRow(127, 9, false)]
        [DataRow(127, 126, false)]
        [DataRow(127, 127, false)]
        public void TryFindKeyIndexOrSubTreeIndexForKeyWorks(long capacity, long rootKeyValueCount, bool isLeaf)
        {
            using (MemoryStream ms = new MemoryStream())
            {
                using (var storage = StreamingPageStorage.Create(ms, BTreeNode<long, string>.GetRequiredPageSize(MockLongSerializer.CDataSize, MockStringSerializer.CDataSize, capacity), 1, null, new CancellationToken(false), true))
                {
                    MockBTree bTree = new MockBTree(storage, new MockLongSerializer(), new MockStringSerializer());

                    //Allocate the root node
                    Assert.IsTrue(BTreeNode<long, string>.TryCreateNew(bTree, isLeaf, out var node));

                    long keyIncrementer = 0;

                    //Initialize the keys
                    node.KeyValuePairCount = rootKeyValueCount;

                    long highestLeafKey = 0;
                    //Allocate the sub-trees
                    for (long i = 0; i < node.SubTreeCount; i++)
                    {
                        Assert.IsTrue(BTreeNode<long, string>.TryCreateNew(bTree, true, out var subNode));

                        //Write all values on the left sub-tree
                        subNode.KeyValuePairCount = subNode.MaxKeyValuePairCapacity;
                        for (long j = 0; j < subNode.KeyValuePairCount; j++)
                        {
                            long key = keyIncrementer++;
                            subNode.SetKeyAt(j, key);
                            if (key > highestLeafKey)
                                highestLeafKey = key;
                        }

                        //Add the left sub-tree
                        node.SetSubTreeAt(i, subNode);

                        //Write the key to the root node
                        if (i < node.KeyValuePairCount)
                            node.SetKeyAt(i, keyIncrementer++);
                    }

                    long count = keyIncrementer;

                    for (long i = 0; i < count; i++)
                    {
                        bool isOnRoot = node.TryFindKeyIndexOrSubTreeIndexForKey(i, out long indexOnThisNode, out bool mayBeFoundInSubTree, out long mayBeFoundInSubTreeIndex);
                        if (isOnRoot)
                        {
                            Assert.AreEqual(i, node.GetKeyAt(indexOnThisNode));
                            Assert.IsFalse(mayBeFoundInSubTree);
                            Assert.AreEqual(-1, mayBeFoundInSubTreeIndex);
                        }
                        else
                        {
                            Assert.AreEqual(-1, indexOnThisNode);
                            Assert.IsTrue(mayBeFoundInSubTree);
                            var subTreeRoot = node.GetSubTreeAt(mayBeFoundInSubTreeIndex);
                            Assert.IsTrue(subTreeRoot.TryFindValue(i, out var value));//Just to make sure it really exists in this node
                        }
                    }

                    //Test a key that is lower than all keys
                    long lowerThanMinKey = -1;
                    if (!isLeaf)
                    {
                        Assert.IsFalse(node.TryFindKeyIndexOrSubTreeIndexForKey(lowerThanMinKey, out long indexOnThisNode_, out bool mayBeFoundInSubTree_, out long mayBeFoundInSubTreeIndex_));
                        Assert.AreEqual(-1, indexOnThisNode_);
                        Assert.IsTrue(mayBeFoundInSubTree_);
                        var subTreeRoot_ = node.GetSubTreeAt(mayBeFoundInSubTreeIndex_);
                        Assert.IsFalse(subTreeRoot_.TryFindValue(lowerThanMinKey, out var value));//It does not exist (but it could have, root node didn't know!)
                    }
                    else
                    {
                        //Leaf nodes have no sub-trees, so it cannot possibly be stored in a sub-tree
                        Assert.IsFalse(node.TryFindKeyIndexOrSubTreeIndexForKey(lowerThanMinKey, out long indexOnThisNode_, out bool mayBeFoundInSubTree_, out long mayBeFoundInSubTreeIndex_));
                        Assert.AreEqual(-1, indexOnThisNode_);
                        Assert.IsFalse(mayBeFoundInSubTree_);
                        Assert.AreEqual(-1, mayBeFoundInSubTreeIndex_);
                    }

                    //Test a key that is larger than all keys
                    long higherThanMaxKey = highestLeafKey + 1;
                    if (!isLeaf)
                    {
                        Assert.IsFalse(node.TryFindKeyIndexOrSubTreeIndexForKey(higherThanMaxKey, out long indexOnThisNode_, out bool mayBeFoundInSubTree_, out long mayBeFoundInSubTreeIndex_));
                        Assert.AreEqual(-1, indexOnThisNode_);
                        Assert.IsTrue(mayBeFoundInSubTree_);
                        var subTreeRoot_ = node.GetSubTreeAt(mayBeFoundInSubTreeIndex_);
                        Assert.IsFalse(subTreeRoot_.TryFindValue(higherThanMaxKey, out var value));//It does not exist (but it could have, root node didn't know!)
                    }
                    else
                    {
                        //Leaf nodes have no sub-trees, so it cannot possibly be stored in a sub-tree
                        Assert.IsFalse(node.TryFindKeyIndexOrSubTreeIndexForKey(higherThanMaxKey, out long indexOnThisNode_, out bool mayBeFoundInSubTree_, out long mayBeFoundInSubTreeIndex_));
                        Assert.AreEqual(-1, indexOnThisNode_);
                        Assert.IsFalse(mayBeFoundInSubTree_);
                        Assert.AreEqual(-1, mayBeFoundInSubTreeIndex_);
                    }
                }
            }
        }

        [TestMethod]
        public void CopyNonLeafElementsThrowsWhenArgIsNull()
        {
            using (MemoryStream ms = new MemoryStream())
            {
                using (var storage = StreamingPageStorage.Create(ms, BTreeNode<long, string>.GetRequiredPageSize(MockLongSerializer.CDataSize, MockStringSerializer.CDataSize, 5), 1, null, new CancellationToken(false), true))
                {
                    MockBTree bTree = new MockBTree(storage, new MockLongSerializer(), new MockStringSerializer());

                    Assert.IsTrue(BTreeNode<long, string>.TryCreateNew(bTree, false, out var node1));
                    Assert.IsTrue(BTreeNode<long, string>.TryCreateNew(bTree, false, out var node2));
                    Assert.IsTrue(BTreeNode<long, string>.TryCreateNew(bTree, false, out var node3));
                    Assert.IsTrue(BTreeNode<long, string>.TryCreateNew(bTree, false, out var node4));

                    Assert.ThrowsException<ArgumentNullException>(() => {
                        BTreeNode<long, string>.CopyNonLeafElements(null, 0, node2, 0, 1, node3, node4);
                    });
                    Assert.ThrowsException<ArgumentNullException>(() => {
                        BTreeNode<long, string>.CopyNonLeafElements(node1, 0, null, 0, 1, node3, node4);
                    });
                }
            }
        }

        [TestMethod]
        public void CopyNonLeafElementsThrowsWhenTwoArgsNotFromSameTree()
        {
            using (MemoryStream ms1 = new MemoryStream())
            {
                using (MemoryStream ms2 = new MemoryStream())
                {
                    using (var storage1 = StreamingPageStorage.Create(ms1, BTreeNode<long, string>.GetRequiredPageSize(MockLongSerializer.CDataSize, MockStringSerializer.CDataSize, 5), 1, null, new CancellationToken(false), true))
                    {
                        using (var storage2 = StreamingPageStorage.Create(ms2, BTreeNode<long, string>.GetRequiredPageSize(MockLongSerializer.CDataSize, MockStringSerializer.CDataSize, 5), 1, null, new CancellationToken(false), true))
                        {
                            MockBTree bTree1 = new MockBTree(storage1, new MockLongSerializer(), new MockStringSerializer());
                            MockBTree bTree2 = new MockBTree(storage2, new MockLongSerializer(), new MockStringSerializer());

                            Assert.IsTrue(BTreeNode<long, string>.TryCreateNew(bTree1, false, out var node1));
                            Assert.IsTrue(BTreeNode<long, string>.TryCreateNew(bTree2, false, out var node2));
                            Assert.IsTrue(BTreeNode<long, string>.TryCreateNew(bTree1, false, out var node3));
                            Assert.IsTrue(BTreeNode<long, string>.TryCreateNew(bTree1, false, out var node4));

                            Assert.ThrowsException<InvalidOperationException>(() => {
                                BTreeNode<long, string>.CopyNonLeafElements(node1, 0, node2, 0, 1, node3, node4);
                            });
                        }
                    }
                }
            }
        }

        [TestMethod]
        public void CopyNonLeafElementsThrowsWhenBorderArgsNotFromSameTree()
        {
            using (MemoryStream ms1 = new MemoryStream())
            {
                using (MemoryStream ms2 = new MemoryStream())
                {
                    using (var storage1 = StreamingPageStorage.Create(ms1, BTreeNode<long, string>.GetRequiredPageSize(MockLongSerializer.CDataSize, MockStringSerializer.CDataSize, 5), 1, null, new CancellationToken(false), true))
                    {
                        using (var storage2 = StreamingPageStorage.Create(ms2, BTreeNode<long, string>.GetRequiredPageSize(MockLongSerializer.CDataSize, MockStringSerializer.CDataSize, 5), 1, null, new CancellationToken(false), true))
                        {
                            MockBTree bTree1 = new MockBTree(storage1, new MockLongSerializer(), new MockStringSerializer());
                            MockBTree bTree2 = new MockBTree(storage2, new MockLongSerializer(), new MockStringSerializer());

                            Assert.IsTrue(BTreeNode<long, string>.TryCreateNew(bTree1, false, out var node1));
                            Assert.IsTrue(BTreeNode<long, string>.TryCreateNew(bTree1, false, out var node2));
                            Assert.IsTrue(BTreeNode<long, string>.TryCreateNew(bTree2, false, out var node3_bad));
                            Assert.IsTrue(BTreeNode<long, string>.TryCreateNew(bTree1, false, out var node3_good));

                            Assert.ThrowsException<ArgumentException>(() => {
                                BTreeNode<long, string>.CopyNonLeafElements(node1, 0, node2, 0, 1, node3_bad, node3_good);
                            });

                            Assert.ThrowsException<ArgumentException>(() => {
                                BTreeNode<long, string>.CopyNonLeafElements(node1, 0, node2, 0, 1, node3_good, node3_bad);
                            });
                        }
                    }
                }
            }
        }

        [TestMethod]
        public void CopyNonLeafElementsThrowsWhenDestinationIsReadOnly()
        {
            using (MemoryStream ms = new MemoryStream())
            {
                long nodeIndex1, nodeIndex2, nodeIndex3, nodeIndex4;
                using (var storage = StreamingPageStorage.Create(ms, 10240, 1, null, new CancellationToken(false), true))
                {
                    //Initialize the mock B-Tree while 'storage' is writable
                    MockBTree bTree = new MockBTree(storage, new MockLongSerializer(), new MockStringSerializer());

                    Assert.IsTrue(BTreeNode<long, string>.TryCreateNew(bTree, false, out var node1));
                    Assert.IsTrue(BTreeNode<long, string>.TryCreateNew(bTree, false, out var node2));
                    Assert.IsTrue(BTreeNode<long, string>.TryCreateNew(bTree, false, out var node3));
                    Assert.IsTrue(BTreeNode<long, string>.TryCreateNew(bTree, false, out var node4));
                    nodeIndex1 = node1.PageIndex;
                    nodeIndex2 = node2.PageIndex;
                    nodeIndex3 = node3.PageIndex;
                    nodeIndex4 = node4.PageIndex;
                }

                //Re-load the storage as read-only
                using (var storage = StreamingPageStorage.Load(ms, true, true, true))
                {
                    //Re-load the MockBTree as read-only (determined by the storage being read-only)
                    var bTree = new MockBTree(storage, new MockLongSerializer(), new MockStringSerializer());

                    var node1 = new BTreeNode<long, string>(bTree, nodeIndex1);
                    var node2 = new BTreeNode<long, string>(bTree, nodeIndex2);
                    var node3 = new BTreeNode<long, string>(bTree, nodeIndex3);
                    var node4 = new BTreeNode<long, string>(bTree, nodeIndex4);

                    Assert.ThrowsException<InvalidOperationException>(() => {
                        BTreeNode<long, string>.CopyNonLeafElements(node1, 0, node2, 0, 1, node3, node4);
                    });
                }
            }
        }

        [DataTestMethod]
        [DataRow(5, 1, 1, -1, 0, 1)]
        [DataRow(5, 1, 1, 0, -1, 1)]
        [DataRow(5, 1, 1, 0, 0, -1)]
        [DataRow(5, 1, 1, 0, 0, 0)]
        [DataRow(5, 1, 1, 1, 0, 1)]//src+amount bad
        [DataRow(5, 1, 1, 0, 1, 1)]//src+amount bad
        [DataRow(5, 2, 2, 2, 0, 1)]//src+amount bad
        [DataRow(5, 2, 2, 0, 2, 1)]//src+amount bad
        [DataRow(5, 2, 2, 2, 0, 2)]//src+amount bad
        [DataRow(5, 2, 2, 0, 2, 2)]//src+amount bad
        public void CopyNonLeafElementsThrowsWhenArgIsOutOfRange(long capacity, long srcCount, long dstCount, long srcOffset, long dstOffset, long amount)
        {
            using (MemoryStream ms = new MemoryStream())
            {
                using (var storage = StreamingPageStorage.Create(ms, BTreeNode<long, string>.GetRequiredPageSize(MockLongSerializer.CDataSize, MockStringSerializer.CDataSize, capacity), 1, null, new CancellationToken(false), true))
                {
                    MockBTree bTree = new MockBTree(storage, new MockLongSerializer(), new MockStringSerializer());

                    Assert.IsTrue(BTreeNode<long, string>.TryCreateNew(bTree, false, out var node1));
                    Assert.IsTrue(BTreeNode<long, string>.TryCreateNew(bTree, false, out var node2));
                    Assert.IsTrue(BTreeNode<long, string>.TryCreateNew(bTree, false, out var node3));
                    Assert.IsTrue(BTreeNode<long, string>.TryCreateNew(bTree, false, out var node4));
                    node1.KeyValuePairCount = srcCount;
                    node2.KeyValuePairCount = dstCount;

                    Assert.ThrowsException<ArgumentOutOfRangeException>(() => {
                        BTreeNode<long, string>.CopyNonLeafElements(node1, srcOffset, node2, dstOffset, amount, node3, node4);
                    });
                }
            }
        }

        [DataTestMethod]
        [DataRow(true, false)]
        [DataRow(false, true)]
        [DataRow(true, true)]
        public void CopyNonLeafElementsThrowsWhenEitherArgIsALeaf(bool isSrcALeaf, bool isDstALeaf)
        {
            using (MemoryStream ms = new MemoryStream())
            {
                using (var storage = StreamingPageStorage.Create(ms, BTreeNode<long, string>.GetRequiredPageSize(MockLongSerializer.CDataSize, MockStringSerializer.CDataSize, 5), 1, null, new CancellationToken(false), true))
                {
                    MockBTree bTree = new MockBTree(storage, new MockLongSerializer(), new MockStringSerializer());

                    Assert.IsTrue(BTreeNode<long, string>.TryCreateNew(bTree, isSrcALeaf, out var node1));
                    Assert.IsTrue(BTreeNode<long, string>.TryCreateNew(bTree, isDstALeaf, out var node2));
                    Assert.IsTrue(BTreeNode<long, string>.TryCreateNew(bTree, true, out var node3));
                    Assert.IsTrue(BTreeNode<long, string>.TryCreateNew(bTree, true, out var node4));
                    node1.KeyValuePairCount = 5;
                    node2.KeyValuePairCount = 5;

                    Assert.ThrowsException<ArgumentException>(() => {
                        BTreeNode<long, string>.CopyNonLeafElements(node1, 0, node2, 0, 1, node3, node4);
                    });
                }
            }
        }

        [DataTestMethod]
        [DataRow(7, 7, 7, 7)]
        [DataRow(7, 7, 7, 6)]
        [DataRow(7, 7, 7, 5)]
        [DataRow(7, 7, 7, 2)]
        [DataRow(7, 7, 7, 1)]
        [DataRow(7, 6, 5, 5)]
        [DataRow(7, 6, 5, 4)]
        [DataRow(7, 6, 5, 3)]
        [DataRow(7, 6, 5, 2)]
        [DataRow(7, 6, 5, 1)]
        [DataRow(9, 5, 8, 5)]
        [DataRow(9, 5, 8, 4)]
        [DataRow(9, 5, 8, 3)]
        [DataRow(9, 5, 8, 2)]
        [DataRow(9, 5, 8, 1)]
        public void CopyNonLeafElementsBetweenDifferentNodesWorks(long capacity, long srcCount, long dstCount, long amount)
        {
            for(long maxMoveAmount = 1; maxMoveAmount < amount * 2 /* *2 to ensure it goes over*/; maxMoveAmount++)
            {
                for (long srcOffset = 0; srcOffset < srcCount - amount; srcOffset++)
                {
                    for (long dstOffset = 0; dstOffset < dstCount - amount; dstOffset++)
                    {
                        using (MemoryStream ms = new MemoryStream())
                        {
                            using (var storage = StreamingPageStorage.Create(ms, BTreeNode<long, string>.GetRequiredPageSize(MockLongSerializer.CDataSize, MockStringSerializer.CDataSize, capacity), 1, null, new CancellationToken(false), true))
                            {
                                MockBTree bTree = new MockBTree(storage, new MockLongSerializer(), new MockStringSerializer(), maxMoveAmount);
                                Assert.AreEqual(maxMoveAmount, bTree.MaxMoveKeyValuePairCount);

                                Assert.IsTrue(BTreeNode<long, string>.TryCreateNew(bTree, false, out var srcNode));
                                Assert.IsTrue(BTreeNode<long, string>.TryCreateNew(bTree, false, out var dstNode));
                                Assert.IsTrue(BTreeNode<long, string>.TryCreateNew(bTree, true, out var leftNode));
                                Assert.IsTrue(BTreeNode<long, string>.TryCreateNew(bTree, true, out var rightNode));
                                srcNode.KeyValuePairCount = srcCount;
                                dstNode.KeyValuePairCount = dstCount;

                                long keyIncrementer = 0;
                                long[] srcKeys = new long[srcCount];
                                for (long i = 0; i < srcCount; i++)
                                {
                                    srcKeys[i] = keyIncrementer++;
                                    srcNode.SetKeyAt(i, srcKeys[i]);
                                    srcNode.SetValueAt(i, srcKeys[i].ToString());
                                }

                                long[] srcSubTreePageIndices = new long[srcNode.SubTreeCount];
                                for (long i = 0; i < srcSubTreePageIndices.Length; i++)
                                {
                                    Assert.IsTrue(BTreeNode<long, string>.TryCreateNew(bTree, false, out var subTreeRootNode));
                                    srcSubTreePageIndices[i] = subTreeRootNode.PageIndex;
                                    srcNode.SetSubTreeAt(i, subTreeRootNode);
                                }

                                long[] dstKeys = new long[dstCount];
                                for (long i = 0; i < dstCount; i++)
                                {
                                    dstKeys[i] = keyIncrementer++;
                                    dstNode.SetKeyAt(i, dstKeys[i]);
                                    dstNode.SetValueAt(i, dstKeys[i].ToString());
                                }

                                long[] dstSubTreePageIndices = new long[dstNode.SubTreeCount];
                                for (long i = 0; i < dstSubTreePageIndices.Length; i++)
                                {
                                    Assert.IsTrue(BTreeNode<long, string>.TryCreateNew(bTree, false, out var subTreeRootNode));
                                    dstSubTreePageIndices[i] = subTreeRootNode.PageIndex;
                                    dstNode.SetSubTreeAt(i, subTreeRootNode);
                                }

                                long[] expectedDstKeys = new long[dstCount];
                                Array.Copy(dstKeys, 0, expectedDstKeys, 0, dstCount);
                                Array.Copy(srcKeys, srcOffset, expectedDstKeys, dstOffset, amount);

                                long[] expectedDstSubTreePageIndices = new long[dstNode.SubTreeCount];
                                Array.Copy(dstSubTreePageIndices, 0, expectedDstSubTreePageIndices, 0, dstNode.SubTreeCount);
                                Array.Copy(srcSubTreePageIndices, srcOffset + 1, expectedDstSubTreePageIndices, dstOffset + 1, amount - 1);
                                expectedDstSubTreePageIndices[dstOffset] = leftNode.PageIndex;
                                expectedDstSubTreePageIndices[dstOffset + amount] = rightNode.PageIndex;

                                BTreeNode<long, string>.CopyNonLeafElements(srcNode, srcOffset, dstNode, dstOffset, amount, leftNode, rightNode);

                                //Ensure the source node was not modified
                                for (long i = 0; i < srcCount; i++)
                                    Assert.AreEqual(srcKeys[i], srcNode.GetKeyAt(i));
                                for (long i = 0; i < srcNode.SubTreeCount; i++)
                                    Assert.AreEqual(srcSubTreePageIndices[i], srcNode.GetSubTreeAt(i).PageIndex);

                                for (long i = 0; i < dstCount; i++)
                                    Assert.AreEqual(expectedDstKeys[i], dstNode.GetKeyAt(i));

                                for (long i = 0; i < dstNode.SubTreeCount; i++)
                                    Assert.AreEqual(expectedDstSubTreePageIndices[i], dstNode.GetSubTreeAt(i).PageIndex);

                                //Make sure the values are correct too
                                for (long i = 0; i < srcNode.KeyValuePairCount; i++)
                                    Assert.AreEqual(srcNode.GetKeyAt(i).ToString(), srcNode.GetValueAt(i));
                                for (long i = 0; i < dstNode.KeyValuePairCount; i++)
                                    Assert.AreEqual(dstNode.GetKeyAt(i).ToString(), dstNode.GetValueAt(i));
                            }
                        }
                    }
                }
            }
        }

        [DataTestMethod]
        [DataRow(7, 7, 7)]
        [DataRow(7, 7, 6)]
        [DataRow(7, 7, 5)]
        [DataRow(7, 7, 2)]
        [DataRow(7, 7, 1)]
        [DataRow(7, 6, 5)]
        [DataRow(7, 6, 4)]
        [DataRow(7, 6, 3)]
        [DataRow(7, 6, 2)]
        [DataRow(7, 6, 1)]
        [DataRow(9, 5, 5)]
        [DataRow(9, 5, 4)]
        [DataRow(9, 5, 3)]
        [DataRow(9, 5, 2)]
        [DataRow(9, 5, 1)]
        public void CopyNonLeafElementsInSameNodeWorks(long capacity, long count, long amount)
        {
            for(long maxMoveCount = 1; maxMoveCount < capacity * 2/* *2 to ensure it goes over*/; maxMoveCount++)
            {
                for (long srcOffset = 0; srcOffset < count - amount; srcOffset++)
                {
                    for (long dstOffset = 0; dstOffset < count - amount; dstOffset++)
                    {
                        using (MemoryStream ms = new MemoryStream())
                        {
                            using (var storage = StreamingPageStorage.Create(ms, BTreeNode<long, string>.GetRequiredPageSize(MockLongSerializer.CDataSize, MockStringSerializer.CDataSize, capacity), 1, null, new CancellationToken(false), true))
                            {
                                MockBTree bTree = new MockBTree(storage, new MockLongSerializer(), new MockStringSerializer(), maxMoveCount);
                                Assert.AreEqual(maxMoveCount, bTree.MaxMoveKeyValuePairCount);

                                Assert.IsTrue(BTreeNode<long, string>.TryCreateNew(bTree, false, out var srcNode));
                                Assert.IsTrue(BTreeNode<long, string>.TryCreateNew(bTree, true, out var leftNode));
                                Assert.IsTrue(BTreeNode<long, string>.TryCreateNew(bTree, true, out var rightNode));
                                srcNode.KeyValuePairCount = count;

                                long keyIncrementer = 0;
                                long[] srcKeys = new long[count];
                                for (long i = 0; i < count; i++)
                                {
                                    srcKeys[i] = keyIncrementer++;
                                    srcNode.SetKeyAt(i, srcKeys[i]);
                                    srcNode.SetValueAt(i, srcKeys[i].ToString());
                                }

                                long[] srcSubTreePageIndices = new long[srcNode.SubTreeCount];
                                for (long i = 0; i < srcSubTreePageIndices.Length; i++)
                                {
                                    Assert.IsTrue(BTreeNode<long, string>.TryCreateNew(bTree, false, out var subTreeRootNode));
                                    srcSubTreePageIndices[i] = subTreeRootNode.PageIndex;
                                    srcNode.SetSubTreeAt(i, subTreeRootNode);
                                }

                                long[] expectedDstKeys = new long[count];
                                Array.Copy(srcKeys, 0, expectedDstKeys, 0, count);
                                Array.Copy(srcKeys, srcOffset, expectedDstKeys, dstOffset, amount);

                                long[] expectedDstSubTreePageIndices = new long[srcNode.SubTreeCount];
                                Array.Copy(srcSubTreePageIndices, 0, expectedDstSubTreePageIndices, 0, srcNode.SubTreeCount);
                                Array.Copy(srcSubTreePageIndices, srcOffset + 1, expectedDstSubTreePageIndices, dstOffset + 1, amount - 1);
                                expectedDstSubTreePageIndices[dstOffset] = leftNode.PageIndex;
                                expectedDstSubTreePageIndices[dstOffset + amount] = rightNode.PageIndex;

                                BTreeNode<long, string>.CopyNonLeafElements(srcNode, srcOffset, srcNode, dstOffset, amount, leftNode, rightNode);

                                for (long i = 0; i < count; i++)
                                    Assert.AreEqual(expectedDstKeys[i], srcNode.GetKeyAt(i));

                                for (long i = 0; i < srcNode.SubTreeCount; i++)
                                    Assert.AreEqual(expectedDstSubTreePageIndices[i], srcNode.GetSubTreeAt(i).PageIndex);

                                //Make sure the values are correct too
                                for (long i = 0; i < srcNode.KeyValuePairCount; i++)
                                    Assert.AreEqual(srcNode.GetKeyAt(i).ToString(), srcNode.GetValueAt(i));
                            }
                        }
                    }
                }
            }
        }

        [TestMethod]
        public void CopyNonLeafElementsWorksWhenLeftOrRightSubTreeIsNull()
        {
            const long maxMoveAmount = 1024;
            const long capacity = 5;
            const long srcCount = 5;
            const long dstCount = 5;
            const long amount = 5;
            const long srcOffset = 0, dstOffset = 0;
            using (MemoryStream ms = new MemoryStream())
            {
                using (var storage = StreamingPageStorage.Create(ms, BTreeNode<long, string>.GetRequiredPageSize(MockLongSerializer.CDataSize, MockStringSerializer.CDataSize, capacity), 1, null, new CancellationToken(false), true))
                {
                    MockBTree bTree = new MockBTree(storage, new MockLongSerializer(), new MockStringSerializer(), maxMoveAmount);
                    Assert.AreEqual(maxMoveAmount, bTree.MaxMoveKeyValuePairCount);

                    Assert.IsTrue(BTreeNode<long, string>.TryCreateNew(bTree, false, out var srcNode));
                    Assert.IsTrue(BTreeNode<long, string>.TryCreateNew(bTree, false, out var dstNode));
                    Assert.IsTrue(BTreeNode<long, string>.TryCreateNew(bTree, true, out var leftNode));
                    Assert.IsTrue(BTreeNode<long, string>.TryCreateNew(bTree, true, out var rightNode));
                    srcNode.KeyValuePairCount = srcCount;
                    dstNode.KeyValuePairCount = dstCount;

                    long keyIncrementer = 0;
                    long[] srcKeys = new long[srcCount];
                    for (long i = 0; i < srcCount; i++)
                    {
                        srcKeys[i] = keyIncrementer++;
                        srcNode.SetKeyAt(i, srcKeys[i]);
                        srcNode.SetValueAt(i, srcKeys[i].ToString());
                    }

                    long[] srcSubTreePageIndices = new long[srcNode.SubTreeCount];
                    for (long i = 0; i < srcSubTreePageIndices.Length; i++)
                    {
                        Assert.IsTrue(BTreeNode<long, string>.TryCreateNew(bTree, false, out var subTreeRootNode));
                        srcSubTreePageIndices[i] = subTreeRootNode.PageIndex;
                        srcNode.SetSubTreeAt(i, subTreeRootNode);
                    }

                    long[] dstKeys = new long[dstCount];
                    for (long i = 0; i < dstCount; i++)
                    {
                        dstKeys[i] = keyIncrementer++;
                        dstNode.SetKeyAt(i, dstKeys[i]);
                        dstNode.SetValueAt(i, dstKeys[i].ToString());
                    }

                    long[] dstSubTreePageIndices = new long[dstNode.SubTreeCount];
                    for (long i = 0; i < dstSubTreePageIndices.Length; i++)
                    {
                        Assert.IsTrue(BTreeNode<long, string>.TryCreateNew(bTree, false, out var subTreeRootNode));
                        dstSubTreePageIndices[i] = subTreeRootNode.PageIndex;
                        dstNode.SetSubTreeAt(i, subTreeRootNode);
                    }

                    long[] expectedDstKeys = new long[dstCount];
                    Array.Copy(dstKeys, 0, expectedDstKeys, 0, dstCount);
                    Array.Copy(srcKeys, srcOffset, expectedDstKeys, dstOffset, amount);

                    long[] expectedDstSubTreePageIndices = new long[dstNode.SubTreeCount];
                    Array.Copy(dstSubTreePageIndices, 0, expectedDstSubTreePageIndices, 0, dstNode.SubTreeCount);
                    Array.Copy(srcSubTreePageIndices, srcOffset + 1, expectedDstSubTreePageIndices, dstOffset + 1, amount - 1);
                    expectedDstSubTreePageIndices[dstOffset] = leftNode.PageIndex;
                    expectedDstSubTreePageIndices[dstOffset + amount] = rightNode.PageIndex;

                    BTreeNode<long, string>.CopyNonLeafElements(srcNode, srcOffset, dstNode, dstOffset, amount, leftNode, rightNode);

                    //Ensure the source node was not modified
                    for (long i = 0; i < srcCount; i++)
                        Assert.AreEqual(srcKeys[i], srcNode.GetKeyAt(i));
                    for (long i = 0; i < srcNode.SubTreeCount; i++)
                        Assert.AreEqual(srcSubTreePageIndices[i], srcNode.GetSubTreeAt(i).PageIndex);

                    for (long i = 0; i < dstCount; i++)
                        Assert.AreEqual(expectedDstKeys[i], dstNode.GetKeyAt(i));

                    for (long i = 0; i < dstNode.SubTreeCount; i++)
                        Assert.AreEqual(expectedDstSubTreePageIndices[i], dstNode.GetSubTreeAt(i).PageIndex);

                    //Make sure the values are correct too
                    for (long i = 0; i < srcNode.KeyValuePairCount; i++)
                        Assert.AreEqual(srcNode.GetKeyAt(i).ToString(), srcNode.GetValueAt(i));
                    for (long i = 0; i < dstNode.KeyValuePairCount; i++)
                        Assert.AreEqual(dstNode.GetKeyAt(i).ToString(), dstNode.GetValueAt(i));
                }
            }
        }

        private static BTreeNode<long, string> CreateMockNode(MemoryStream memoryStream, out StreamingPageStorage storage, out MockBTree bTree, bool isLeaf = true, long keyValuePairCapacity = 1023, long pageCapacity = 3, long maxMoveCount = 2)
        {
            storage = StreamingPageStorage.Create(memoryStream, BTreeNode<long, string>.GetRequiredPageSize(sizeof(Int64), 32, keyValuePairCapacity), pageCapacity, null, new CancellationToken(false), true);
            bTree = new MockBTree(storage, new MockLongSerializer(), new MockStringSerializer(), maxMoveCount);
            Assert.IsTrue(BTreeNode<long, string>.TryCreateNew(bTree, isLeaf, out var ret));
            Assert.AreEqual(keyValuePairCapacity, ret.MaxKeyValuePairCapacity);
            return ret;
        }
        
        [TestMethod]
        public void InsertAtNonLeafThrowsWhenArgIsNull()
        {
            using (MemoryStream ms = new MemoryStream())
            {
                var node = CreateMockNode(ms, out var storage, out var tree, false, 5);
                using (storage)
                {
                    node.KeyValuePairCount = 3;
                    Assert.IsTrue(BTreeNode<long, string>.TryCreateNew(tree, true, out var subRootNode));
                    Assert.ThrowsException<ArgumentNullException>(() => {
                        node.InsertAtNonLeaf(null, new byte[node.ValueSize], 0, subRootNode, false);
                    });
                    Assert.ThrowsException<ArgumentNullException>(() => {
                        node.InsertAtNonLeaf(new byte[node.KeySize], null, 0, subRootNode, false);
                    });
                    Assert.ThrowsException<ArgumentNullException>(() => {
                        node.InsertAtNonLeaf(new byte[node.KeySize], new byte[node.ValueSize], 0, null, false);
                    });
                }
            }
        }

        [DataTestMethod]
        [DataRow(MockLongSerializer.CDataSize, MockStringSerializer.CDataSize - 1)]
        [DataRow(MockLongSerializer.CDataSize, MockStringSerializer.CDataSize + 1)]
        [DataRow(MockLongSerializer.CDataSize + 1, MockStringSerializer.CDataSize)]
        [DataRow(MockLongSerializer.CDataSize - 1, MockStringSerializer.CDataSize)]
        public void InsertAtNonLeafThrowsWhenBufferSizeIsWrong(long keySize, long valueSize)
        {
            using (MemoryStream ms = new MemoryStream())
            {
                var node = CreateMockNode(ms, out var storage, out var tree, false, 5);
                using (storage)
                {
                    Assert.IsTrue(BTreeNode<long, string>.TryCreateNew(tree, true, out var subRootNode));
                    node.KeyValuePairCount = 3;

                    Assert.ThrowsException<ArgumentException>(() => {
                        node.InsertAtNonLeaf(new byte[keySize], new byte[valueSize], 0, subRootNode, false);
                    });
                }
            }
        }

        [TestMethod]
        public void InsertAtNonLeafThrowsWhenFull()
        {
            using (MemoryStream ms = new MemoryStream())
            {
                var node = CreateMockNode(ms, out var storage, out var tree, false, 5);
                using (storage)
                {
                    Assert.IsTrue(BTreeNode<long, string>.TryCreateNew(tree, false, out var subRootNode));
                    node.KeyValuePairCount = node.MaxKeyValuePairCapacity;

                    Assert.ThrowsException<InvalidOperationException>(() => {
                        node.InsertAtNonLeaf(new byte[node.KeySize], new byte[node.ValueSize], 0, subRootNode, false);
                    });
                }
            }
        }

        [TestMethod]
        public void InsertAtNonLeafThrowsWhenEmpty()
        {
            using (MemoryStream ms = new MemoryStream())
            {
                var node = CreateMockNode(ms, out var storage, out var tree, false, 5);
                using (storage)
                {
                    Assert.IsTrue(BTreeNode<long, string>.TryCreateNew(tree, false, out var subRootNode));
                    node.KeyValuePairCount = 0;

                    Assert.ThrowsException<InvalidOperationException>(() => {
                        node.InsertAtNonLeaf(new byte[node.KeySize], new byte[node.ValueSize], 0, subRootNode, false);
                    });
                }
            }
        }

        [TestMethod]
        public void InsertAtNonLeafThrowsWhenNodeIsALeaf()
        {
            using (MemoryStream ms = new MemoryStream())
            {
                var node = CreateMockNode(ms, out var storage, out var tree, true, 5);
                using (storage)
                {
                    Assert.IsTrue(BTreeNode<long, string>.TryCreateNew(tree, false, out var subRootNode));
                    node.KeyValuePairCount = 2;

                    Assert.ThrowsException<InvalidOperationException>(() => {
                        node.InsertAtNonLeaf(new byte[node.KeySize], new byte[node.ValueSize], 0, subRootNode, false);
                    });
                }
            }
        }

        [TestMethod]
        public void InsertAtNonLeafThrowsWhenSubNodeBelongsToWrongTree()
        {
            using (MemoryStream ms = new MemoryStream())
            {
                var node = CreateMockNode(ms, out var storage, out var tree, false, 5);
                var subRootNode = CreateMockNode(ms, out var subStorage, out var wrongTree, false, 5);
                using (storage)
                {
                    using (subStorage)
                    {
                        node.KeyValuePairCount = 2;

                        Assert.ThrowsException<ArgumentException>(() => {
                            node.InsertAtNonLeaf(new byte[node.KeySize], new byte[node.ValueSize], 0, subRootNode, false);
                        });
                    }
                }
            }
        }

        [DataTestMethod]
        [DataRow(5, 1, -1)]
        [DataRow(5, 1, 2)]
        [DataRow(5, 1, 3)]
        [DataRow(5, 1, 4)]
        [DataRow(5, 2, -1)]
        [DataRow(5, 2, 3)]
        [DataRow(5, 2, 4)]
        [DataRow(5, 3, -1)]
        [DataRow(5, 3, 4)]
        [DataRow(5, 4, -1)]
        [DataRow(5, 4, 5)]
        [DataRow(7, 2, -1)]
        [DataRow(7, 2, 3)]
        [DataRow(7, 2, 4)]
        [DataRow(7, 2, 5)]
        [DataRow(7, 2, 6)]
        [DataRow(7, 4, -1)]
        [DataRow(7, 4, 5)]
        [DataRow(7, 4, 6)]
        public void InsertAtNonLeafThrowsWhenArgIsOutOfRange(long capacity, long initCount, long keyValuePairIndex)
        {
            using (MemoryStream ms = new MemoryStream())
            {
                var node = CreateMockNode(ms, out var storage, out var tree, false, capacity);
                using (storage)
                {
                    Assert.IsTrue(BTreeNode<long, string>.TryCreateNew(tree, false, out var subRootNode));
                    node.KeyValuePairCount = initCount;

                    Assert.ThrowsException<ArgumentOutOfRangeException>(() => {
                        node.InsertAtNonLeaf(new byte[node.KeySize], new byte[node.ValueSize], keyValuePairIndex, subRootNode, false);
                    });
                }
            }
        }

        [TestMethod]
        public void InsertAtNonLeafThrowsWhenReadOnly()
        {
            using (MemoryStream ms = new MemoryStream())
            {
                long nodeIndex, subTreeRootNodeIndex;
                using (var storage = StreamingPageStorage.Create(ms, 10240, 1, null, new CancellationToken(false), true))
                {
                    //Initialize the mock B-Tree while 'storage' is writable
                    MockBTree bTree = new MockBTree(storage, new MockLongSerializer(), new MockStringSerializer());

                    //Allocate the nodes
                    Assert.IsTrue(BTreeNode<long, string>.TryCreateNew(bTree, false, out var node));
                    nodeIndex = node.PageIndex;
                    Assert.IsTrue(BTreeNode<long, string>.TryCreateNew(bTree, false, out var subTreeRootNode));
                    subTreeRootNodeIndex = node.PageIndex;

                    //Initialize the count
                    node.KeyValuePairCount = 5;
                }

                //Re-load the storage as read-only
                using (var storage = StreamingPageStorage.Load(ms, true, true, true))
                {
                    //Re-load the MockBTree as read-only (determined by the storage being read-only)
                    var bTree = new MockBTree(storage, new MockLongSerializer(), new MockStringSerializer());

                    //Re-load the nodes
                    var node = new BTreeNode<long, string>(bTree, nodeIndex);
                    var subTreeRootNode = new BTreeNode<long, string>(bTree, subTreeRootNodeIndex);

                    Assert.ThrowsException<InvalidOperationException>(() => {
                        node.InsertAtNonLeaf(new byte[node.KeySize], new byte[node.ValueSize], 0, subTreeRootNode, true);
                    });
                }
            }
        }

        [DataTestMethod]
        [DataRow(5, 1, true)]
        [DataRow(5, 2, true)]
        [DataRow(5, 3, true)]
        [DataRow(5, 4, true)]
        [DataRow(63, 1, true)]
        [DataRow(63, 2, true)]
        [DataRow(63, 3, true)]
        [DataRow(63, 4, true)]
        [DataRow(63, 5, true)]
        [DataRow(63, 6, true)]
        [DataRow(63, 7, true)]
        [DataRow(63, 8, true)]
        [DataRow(63, 9, true)]
        [DataRow(63, 61, true)]
        [DataRow(63, 62, true)]
        [DataRow(5, 1, false)]
        [DataRow(5, 2, false)]
        [DataRow(5, 3, false)]
        [DataRow(5, 4, false)]
        [DataRow(63, 1, false)]
        [DataRow(63, 2, false)]
        [DataRow(63, 3, false)]
        [DataRow(63, 4, false)]
        [DataRow(63, 5, false)]
        [DataRow(63, 6, false)]
        [DataRow(63, 7, false)]
        [DataRow(63, 8, false)]
        [DataRow(63, 9, false)]
        [DataRow(63, 61, false)]
        [DataRow(63, 62, false)]
        public void InsertAtNonLeafWorks(long capacity, long initCount, bool insertSubTreeLeft)
        {
            for(long maxMoveCount = 1; maxMoveCount < capacity * 2 /* *2 to ensure it goes over*/; maxMoveCount *= 2)
            {
                for (long i = 0; i <= initCount; i++)//Test insertion at every possible index
                {
                    using (MemoryStream ms = new MemoryStream())
                    {
                        var node = CreateMockNode(ms, out var storage, out var tree, false, capacity, 3, maxMoveCount);
                        using (storage)
                        {
                            Assert.AreEqual(maxMoveCount, tree.MaxMoveKeyValuePairCount);
                            byte[] keyBuffer = new byte[node.KeySize];
                            byte[] valueBuffer = new byte[node.ValueSize];
                            tree.KeySerializer.Serialize(-1, keyBuffer);
                            tree.ValueSerializer.Serialize("Hello World", valueBuffer);

                            //Setup the key-value pairs
                            node.KeyValuePairCount = initCount;
                            for (long j = 0; j < initCount; j++)
                            {
                                node.SetKeyAt(j, j);
                                node.SetValueAt(j, j.ToString());
                            }

                            //Setup the sub-trees
                            List<long> subTreePSIndices = new List<long>();
                            long initSubTreeCount = initCount + 1;//+1 to go right
                            for (long j = 0; j < initSubTreeCount; j++)
                            {
                                Assert.IsTrue(BTreeNode<long, string>.TryCreateNew(tree, true, out var subTreeRoot));
                                subTreePSIndices.Add(subTreeRoot.PageIndex);
                                node.SetSubTreeAt(j, subTreeRoot);
                            }

                            Assert.IsTrue(BTreeNode<long, string>.TryCreateNew(tree, true, out var subTreeRootToInsert));

                            //Set where the 'expected' sub-tree will be located
                            if (insertSubTreeLeft)
                                subTreePSIndices.Insert((int)i, subTreeRootToInsert.PageIndex);
                            else
                                subTreePSIndices.Insert((int)i + 1, subTreeRootToInsert.PageIndex);

                            //Insert into the node
                            node.InsertAtNonLeaf(keyBuffer, valueBuffer, i, subTreeRootToInsert, insertSubTreeLeft);

                            //Make sure the key-value pairs are correct
                            for (long j = 0; j < initCount + 1; j++)
                            {
                                long expectedKey = j;
                                if (j > i)
                                    expectedKey--;
                                string expectedValue = expectedKey.ToString();
                                if (j == i)
                                {
                                    expectedKey = -1;
                                    expectedValue = "Hello World";
                                }

                                Assert.AreEqual(expectedKey, node.GetKeyAt(j));
                                Assert.AreEqual(expectedValue, node.GetValueAt(j));
                            }

                            //Make sure the sub-trees are correct
                            for (long j = 0; j < node.SubTreeCount; j++)
                            {
                                var expectedPageStorageIndex = subTreePSIndices[(int)j];
                                Assert.AreEqual(expectedPageStorageIndex, node.GetSubTreeAt(j).PageIndex);
                            }

                            Assert.AreEqual(initCount + 1, node.KeyValuePairCount);
                        }
                    }
                }
            }
        }

        [TestMethod]
        public void RemoveAtNonLeafThrowsWhenReadOnly()
        {
            using (MemoryStream ms = new MemoryStream())
            {
                long nodeIndex;
                using (var storage = StreamingPageStorage.Create(ms, 10240, 1, null, new CancellationToken(false), true))
                {
                    //Initialize the mock B-Tree while 'storage' is writable
                    MockBTree bTree = new MockBTree(storage, new MockLongSerializer(), new MockStringSerializer());

                    //Allocate the nodes
                    Assert.IsTrue(BTreeNode<long, string>.TryCreateNew(bTree, false, out var node));
                    nodeIndex = node.PageIndex;

                    //Initialize the count
                    node.KeyValuePairCount = 5;
                }

                //Re-load the storage as read-only
                using (var storage = StreamingPageStorage.Load(ms, true, true, true))
                {
                    //Re-load the MockBTree as read-only (determined by the storage being read-only)
                    var bTree = new MockBTree(storage, new MockLongSerializer(), new MockStringSerializer());

                    //Re-load the nodes
                    var node = new BTreeNode<long, string>(bTree, nodeIndex);

                    Assert.ThrowsException<InvalidOperationException>(() => {
                        node.RemoveAtNonLeaf(0, true);
                    });
                }
            }
        }

        [TestMethod]
        public void RemoveAtNonLeafThrowsWhenNodeIsALeaf()
        {
            using (MemoryStream ms = new MemoryStream())
            {
                var node = CreateMockNode(ms, out var storage, out var tree, true, 5);
                using (storage)
                {
                    node.KeyValuePairCount = 2;

                    Assert.ThrowsException<InvalidOperationException>(() => {
                        node.RemoveAtNonLeaf(0, true);
                    });
                }
            }
        }

        [DataTestMethod]
        [DataRow(5, 1, -1)]
        [DataRow(5, 1, 1)]
        [DataRow(5, 2, -1)]
        [DataRow(5, 2, 2)]
        [DataRow(5, 3, -1)]
        [DataRow(5, 3, 3)]
        [DataRow(5, 4, -1)]
        [DataRow(5, 4, 4)]
        [DataRow(5, 5, -1)]
        [DataRow(5, 5, 5)]
        [DataRow(7, 1, -1)]
        [DataRow(7, 1, 1)]
        [DataRow(7, 1, 2)]
        [DataRow(7, 7, -1)]
        [DataRow(7, 7, 7)]
        [DataRow(7, 7, 8)]
        public void RemoveAtNonLeafThrowsWhenIndexOutOfRange(long capacity, long initCount, long badIndex)
        {
            using (MemoryStream ms = new MemoryStream())
            {
                var node = CreateMockNode(ms, out var storage, out var tree, false, capacity);
                using (storage)
                {
                    Assert.IsTrue(BTreeNode<long, string>.TryCreateNew(tree, true, out var subRootNode));
                    node.KeyValuePairCount = initCount;

                    Assert.ThrowsException<ArgumentOutOfRangeException>(() => {
                        node.RemoveAtNonLeaf(badIndex, true);
                    });

                    Assert.ThrowsException<ArgumentOutOfRangeException>(() => {
                        node.RemoveAtNonLeaf(badIndex, false);
                    });
                }
            }
        }

        [DataTestMethod]
        [DataRow(5, 1, true)]
        [DataRow(5, 2, true)]
        [DataRow(5, 3, true)]
        [DataRow(5, 4, true)]
        [DataRow(5, 5, true)]
        [DataRow(7, 1, true)]
        [DataRow(7, 2, true)]
        [DataRow(7, 3, true)]
        [DataRow(7, 4, true)]
        [DataRow(7, 5, true)]
        [DataRow(7, 6, true)]
        [DataRow(7, 7, true)]
        [DataRow(63, 61, true)]
        [DataRow(63, 62, true)]
        [DataRow(63, 63, true)]
        [DataRow(5, 1, false)]
        [DataRow(5, 2, false)]
        [DataRow(5, 3, false)]
        [DataRow(5, 4, false)]
        [DataRow(5, 5, false)]
        [DataRow(7, 1, false)]
        [DataRow(7, 2, false)]
        [DataRow(7, 3, false)]
        [DataRow(7, 4, false)]
        [DataRow(7, 5, false)]
        [DataRow(7, 6, false)]
        [DataRow(7, 7, false)]
        [DataRow(63, 61, false)]
        [DataRow(63, 62, false)]
        [DataRow(63, 63, false)]
        public void RemoveAtNonLeafWorks(long capacity, long initCount, bool removeLeftSubTree)
        {
            for(long maxMoveCount = 1; maxMoveCount < capacity * 2/* *2 to go over*/; maxMoveCount *= 2)
            {
                for (long i = 0; i < initCount; i++)//Test removal at every possible index
                {
                    using (MemoryStream ms = new MemoryStream())
                    {
                        var node = CreateMockNode(ms, out var storage, out var tree, false, capacity, 3, maxMoveCount);
                        using (storage)
                        {
                            Assert.AreEqual(maxMoveCount, tree.MaxMoveKeyValuePairCount);
                            byte[] keyBuffer = new byte[node.KeySize];
                            byte[] valueBuffer = new byte[node.ValueSize];
                            tree.KeySerializer.Serialize(-1, keyBuffer);
                            tree.ValueSerializer.Serialize("Hello World", valueBuffer);

                            //Setup the key-value pairs
                            node.KeyValuePairCount = initCount;
                            for (long j = 0; j < initCount; j++)
                            {
                                node.SetKeyAt(j, j);
                                node.SetValueAt(j, j.ToString());
                            }

                            //Setup the sub-trees
                            List<long> subTreePSIndices = new List<long>();
                            long initSubTreeCount = initCount + 1;//+1 to go right
                            for (long j = 0; j < initSubTreeCount; j++)
                            {
                                Assert.IsTrue(BTreeNode<long, string>.TryCreateNew(tree, true, out var subTreeRoot));
                                subTreePSIndices.Add(subTreeRoot.PageIndex);
                                node.SetSubTreeAt(j, subTreeRoot);
                            }

                            //Determine where the sub-tree will be removed
                            if (removeLeftSubTree)
                                subTreePSIndices.RemoveAt((int)i);
                            else
                                subTreePSIndices.RemoveAt((int)i + 1);

                            //Remove into the node
                            node.RemoveAtNonLeaf(i, removeLeftSubTree);

                            //Make sure the key-value pairs are correct
                            for (long j = 0; j < initCount - 1; j++)
                            {
                                long expectedKey = j;
                                if (j >= i)
                                    expectedKey++;
                                string expectedValue = expectedKey.ToString();
                                Assert.AreEqual(expectedKey, node.GetKeyAt(j));
                                Assert.AreEqual(expectedValue, node.GetValueAt(j));
                            }

                            //Make sure the sub-trees are correct
                            for (long j = 0; j < node.SubTreeCount; j++)
                            {
                                var expectedPageStorageIndex = subTreePSIndices[(int)j];
                                Assert.AreEqual(expectedPageStorageIndex, node.GetSubTreeAt(j).PageIndex);
                            }

                            Assert.AreEqual(initCount - 1, node.KeyValuePairCount);
                        }
                    }
                }
            }
        }
        
        [TestMethod]
        public void CopyLeafElementsThrowsWhenArgIsNull()
        {
            using (MemoryStream ms = new MemoryStream())
            {
                using (var storage = StreamingPageStorage.Create(ms, BTreeNode<long, string>.GetRequiredPageSize(MockLongSerializer.CDataSize, MockStringSerializer.CDataSize, 5), 1, null, new CancellationToken(false), true))
                {
                    MockBTree bTree = new MockBTree(storage, new MockLongSerializer(), new MockStringSerializer());

                    Assert.IsTrue(BTreeNode<long, string>.TryCreateNew(bTree, true, out var node1));
                    Assert.IsTrue(BTreeNode<long, string>.TryCreateNew(bTree, true, out var node2));

                    Assert.ThrowsException<ArgumentNullException>(() => {
                        BTreeNode<long, string>.CopyLeafElements(null, 0, node2, 0, 1);
                    });
                    Assert.ThrowsException<ArgumentNullException>(() => {
                        BTreeNode<long, string>.CopyLeafElements(node1, 0, null, 0, 1);
                    });
                }
            }
        }

        [TestMethod]
        public void CopyLeafElementsThrowsWhenTwoArgsNotFromSameTree()
        {
            using (MemoryStream ms1 = new MemoryStream())
            {
                using (MemoryStream ms2 = new MemoryStream())
                {
                    using (var storage1 = StreamingPageStorage.Create(ms1, BTreeNode<long, string>.GetRequiredPageSize(MockLongSerializer.CDataSize, MockStringSerializer.CDataSize, 5), 1, null, new CancellationToken(false), true))
                    {
                        using (var storage2 = StreamingPageStorage.Create(ms2, BTreeNode<long, string>.GetRequiredPageSize(MockLongSerializer.CDataSize, MockStringSerializer.CDataSize, 5), 1, null, new CancellationToken(false), true))
                        {
                            MockBTree bTree1 = new MockBTree(storage1, new MockLongSerializer(), new MockStringSerializer());
                            MockBTree bTree2 = new MockBTree(storage2, new MockLongSerializer(), new MockStringSerializer());

                            Assert.IsTrue(BTreeNode<long, string>.TryCreateNew(bTree1, true, out var node1));
                            Assert.IsTrue(BTreeNode<long, string>.TryCreateNew(bTree2, true, out var node2));

                            Assert.ThrowsException<InvalidOperationException>(() => {
                                BTreeNode<long, string>.CopyLeafElements(node1, 0, node2, 0, 1);
                            });
                        }
                    }
                }
            }
        }
        
        [TestMethod]
        public void CopyLeafElementsThrowsWhenDestinationIsReadOnly()
        {
            using (MemoryStream ms = new MemoryStream())
            {
                long nodeIndex1, nodeIndex2;
                using (var storage = StreamingPageStorage.Create(ms, 10240, 1, null, new CancellationToken(false), true))
                {
                    //Initialize the mock B-Tree while 'storage' is writable
                    MockBTree bTree = new MockBTree(storage, new MockLongSerializer(), new MockStringSerializer());

                    Assert.IsTrue(BTreeNode<long, string>.TryCreateNew(bTree, true, out var node1));
                    Assert.IsTrue(BTreeNode<long, string>.TryCreateNew(bTree, true, out var node2));
                    nodeIndex1 = node1.PageIndex;
                    nodeIndex2 = node2.PageIndex;
                }

                //Re-load the storage as read-only
                using (var storage = StreamingPageStorage.Load(ms, true, true, true))
                {
                    //Re-load the MockBTree as read-only (determined by the storage being read-only)
                    var bTree = new MockBTree(storage, new MockLongSerializer(), new MockStringSerializer());

                    var node1 = new BTreeNode<long, string>(bTree, nodeIndex1);
                    var node2 = new BTreeNode<long, string>(bTree, nodeIndex2);

                    Assert.ThrowsException<InvalidOperationException>(() => {
                        BTreeNode<long, string>.CopyLeafElements(node1, 0, node2, 0, 1);
                    });
                }
            }
        }

        [DataTestMethod]
        [DataRow(5, 1, 1, -1, 0, 1)]
        [DataRow(5, 1, 1, 0, -1, 1)]
        [DataRow(5, 1, 1, 0, 0, -1)]
        [DataRow(5, 1, 1, 0, 0, 0)]
        [DataRow(5, 1, 1, 1, 0, 1)]//src+amount bad
        [DataRow(5, 1, 1, 0, 1, 1)]//src+amount bad
        [DataRow(5, 2, 2, 2, 0, 1)]//src+amount bad
        [DataRow(5, 2, 2, 0, 2, 1)]//src+amount bad
        [DataRow(5, 2, 2, 2, 0, 2)]//src+amount bad
        [DataRow(5, 2, 2, 0, 2, 2)]//src+amount bad
        public void CopyLeafElementsThrowsWhenArgIsOutOfRange(long capacity, long srcCount, long dstCount, long srcOffset, long dstOffset, long amount)
        {
            using (MemoryStream ms = new MemoryStream())
            {
                using (var storage = StreamingPageStorage.Create(ms, BTreeNode<long, string>.GetRequiredPageSize(MockLongSerializer.CDataSize, MockStringSerializer.CDataSize, capacity), 1, null, new CancellationToken(false), true))
                {
                    MockBTree bTree = new MockBTree(storage, new MockLongSerializer(), new MockStringSerializer());

                    Assert.IsTrue(BTreeNode<long, string>.TryCreateNew(bTree, true, out var node1));
                    Assert.IsTrue(BTreeNode<long, string>.TryCreateNew(bTree, true, out var node2));
                    node1.KeyValuePairCount = srcCount;
                    node2.KeyValuePairCount = dstCount;

                    Assert.ThrowsException<ArgumentOutOfRangeException>(() => {
                        BTreeNode<long, string>.CopyLeafElements(node1, srcOffset, node2, dstOffset, amount);
                    });
                }
            }
        }

        [DataTestMethod]
        [DataRow(true, false)]
        [DataRow(false, true)]
        [DataRow(false, false)]
        public void CopyLeafElementsThrowsWhenEitherArgIsNotALeaf(bool isSrcALeaf, bool isDstALeaf)
        {
            using (MemoryStream ms = new MemoryStream())
            {
                using (var storage = StreamingPageStorage.Create(ms, BTreeNode<long, string>.GetRequiredPageSize(MockLongSerializer.CDataSize, MockStringSerializer.CDataSize, 5), 1, null, new CancellationToken(false), true))
                {
                    MockBTree bTree = new MockBTree(storage, new MockLongSerializer(), new MockStringSerializer());

                    Assert.IsTrue(BTreeNode<long, string>.TryCreateNew(bTree, isSrcALeaf, out var node1));
                    Assert.IsTrue(BTreeNode<long, string>.TryCreateNew(bTree, isDstALeaf, out var node2));
                    node1.KeyValuePairCount = 5;
                    node2.KeyValuePairCount = 5;

                    Assert.ThrowsException<ArgumentException>(() => {
                        BTreeNode<long, string>.CopyLeafElements(node1, 0, node2, 0, 1);
                    });
                }
            }
        }

        [DataTestMethod]
        [DataRow(7, 7, 7, 7)]
        [DataRow(7, 7, 7, 6)]
        [DataRow(7, 7, 7, 5)]
        [DataRow(7, 7, 7, 2)]
        [DataRow(7, 7, 7, 1)]
        [DataRow(7, 6, 5, 5)]
        [DataRow(7, 6, 5, 4)]
        [DataRow(7, 6, 5, 3)]
        [DataRow(7, 6, 5, 2)]
        [DataRow(7, 6, 5, 1)]
        [DataRow(9, 5, 8, 5)]
        [DataRow(9, 5, 8, 4)]
        [DataRow(9, 5, 8, 3)]
        [DataRow(9, 5, 8, 2)]
        [DataRow(9, 5, 8, 1)]
        public void CopyLeafElementsBetweenDifferentNodesWorks(long capacity, long srcCount, long dstCount, long amount)
        {
            for (long maxMoveAmount = 1; maxMoveAmount < amount * 2 /* *2 to ensure it goes over*/; maxMoveAmount++)
            {
                for (long srcOffset = 0; srcOffset < srcCount - amount; srcOffset++)
                {
                    for (long dstOffset = 0; dstOffset < dstCount - amount; dstOffset++)
                    {
                        using (MemoryStream ms = new MemoryStream())
                        {
                            using (var storage = StreamingPageStorage.Create(ms, BTreeNode<long, string>.GetRequiredPageSize(MockLongSerializer.CDataSize, MockStringSerializer.CDataSize, capacity), 1, null, new CancellationToken(false), true))
                            {
                                MockBTree bTree = new MockBTree(storage, new MockLongSerializer(), new MockStringSerializer(), maxMoveAmount);
                                Assert.AreEqual(maxMoveAmount, bTree.MaxMoveKeyValuePairCount);

                                Assert.IsTrue(BTreeNode<long, string>.TryCreateNew(bTree, true, out var srcNode));
                                Assert.IsTrue(BTreeNode<long, string>.TryCreateNew(bTree, true, out var dstNode));
                                srcNode.KeyValuePairCount = srcCount;
                                dstNode.KeyValuePairCount = dstCount;

                                long keyIncrementer = 0;
                                long[] srcKeys = new long[srcCount];
                                for (long i = 0; i < srcCount; i++)
                                {
                                    srcKeys[i] = keyIncrementer++;
                                    srcNode.SetKeyAt(i, srcKeys[i]);
                                    srcNode.SetValueAt(i, srcKeys[i].ToString());
                                }

                                long[] dstKeys = new long[dstCount];
                                for (long i = 0; i < dstCount; i++)
                                {
                                    dstKeys[i] = keyIncrementer++;
                                    dstNode.SetKeyAt(i, dstKeys[i]);
                                    dstNode.SetValueAt(i, dstKeys[i].ToString());
                                }

                                long[] expectedDstKeys = new long[dstCount];
                                Array.Copy(dstKeys, 0, expectedDstKeys, 0, dstCount);
                                Array.Copy(srcKeys, srcOffset, expectedDstKeys, dstOffset, amount);

                                BTreeNode<long, string>.CopyLeafElements(srcNode, srcOffset, dstNode, dstOffset, amount);

                                //Ensure the source node was not modified
                                for (long i = 0; i < srcCount; i++)
                                    Assert.AreEqual(srcKeys[i], srcNode.GetKeyAt(i));

                                for (long i = 0; i < dstCount; i++)
                                    Assert.AreEqual(expectedDstKeys[i], dstNode.GetKeyAt(i));

                                //Make sure the values are correct too
                                for (long i = 0; i < srcNode.KeyValuePairCount; i++)
                                    Assert.AreEqual(srcNode.GetKeyAt(i).ToString(), srcNode.GetValueAt(i));
                                for (long i = 0; i < dstNode.KeyValuePairCount; i++)
                                    Assert.AreEqual(dstNode.GetKeyAt(i).ToString(), dstNode.GetValueAt(i));
                            }
                        }
                    }
                }
            }
        }

        [DataTestMethod]
        [DataRow(7, 7, 7)]
        [DataRow(7, 7, 6)]
        [DataRow(7, 7, 5)]
        [DataRow(7, 7, 2)]
        [DataRow(7, 7, 1)]
        [DataRow(7, 6, 5)]
        [DataRow(7, 6, 4)]
        [DataRow(7, 6, 3)]
        [DataRow(7, 6, 2)]
        [DataRow(7, 6, 1)]
        [DataRow(9, 5, 5)]
        [DataRow(9, 5, 4)]
        [DataRow(9, 5, 3)]
        [DataRow(9, 5, 2)]
        [DataRow(9, 5, 1)]
        public void CopyLeafElementsInSameNodeWorks(long capacity, long count, long amount)
        {
            for (long maxMoveCount = 1; maxMoveCount < capacity * 2/* *2 to ensure it goes over*/; maxMoveCount++)
            {
                for (long srcOffset = 0; srcOffset < count - amount; srcOffset++)
                {
                    for (long dstOffset = 0; dstOffset < count - amount; dstOffset++)
                    {
                        using (MemoryStream ms = new MemoryStream())
                        {
                            using (var storage = StreamingPageStorage.Create(ms, BTreeNode<long, string>.GetRequiredPageSize(MockLongSerializer.CDataSize, MockStringSerializer.CDataSize, capacity), 1, null, new CancellationToken(false), true))
                            {
                                MockBTree bTree = new MockBTree(storage, new MockLongSerializer(), new MockStringSerializer(), maxMoveCount);
                                Assert.AreEqual(maxMoveCount, bTree.MaxMoveKeyValuePairCount);

                                Assert.IsTrue(BTreeNode<long, string>.TryCreateNew(bTree, true, out var srcNode));
                                srcNode.KeyValuePairCount = count;

                                long keyIncrementer = 0;
                                long[] srcKeys = new long[count];
                                for (long i = 0; i < count; i++)
                                {
                                    srcKeys[i] = keyIncrementer++;
                                    srcNode.SetKeyAt(i, srcKeys[i]);
                                    srcNode.SetValueAt(i, srcKeys[i].ToString());
                                }

                                long[] expectedDstKeys = new long[count];
                                Array.Copy(srcKeys, 0, expectedDstKeys, 0, count);
                                Array.Copy(srcKeys, srcOffset, expectedDstKeys, dstOffset, amount);

                                BTreeNode<long, string>.CopyLeafElements(srcNode, srcOffset, srcNode, dstOffset, amount);

                                for (long i = 0; i < count; i++)
                                    Assert.AreEqual(expectedDstKeys[i], srcNode.GetKeyAt(i));

                                //Make sure the values are correct too
                                for (long i = 0; i < srcNode.KeyValuePairCount; i++)
                                    Assert.AreEqual(srcNode.GetKeyAt(i).ToString(), srcNode.GetValueAt(i));
                            }
                        }
                    }
                }
            }
        }
        
        [TestMethod]
        public void InsertAtLeafThrowsWhenArgIsNull()
        {
            using (MemoryStream ms = new MemoryStream())
            {
                var node = CreateMockNode(ms, out var storage, out var tree, true, 5);
                using (storage)
                {
                    node.KeyValuePairCount = 3;
                    Assert.IsTrue(BTreeNode<long, string>.TryCreateNew(tree, true, out var subRootNode));
                    Assert.ThrowsException<ArgumentNullException>(() => {
                        node.InsertAtLeaf(null, new byte[node.ValueSize], 0);
                    });
                    Assert.ThrowsException<ArgumentNullException>(() => {
                        node.InsertAtLeaf(new byte[node.KeySize], null, 0);
                    });
                }
            }
        }

        [DataTestMethod]
        [DataRow(MockLongSerializer.CDataSize, MockStringSerializer.CDataSize - 1)]
        [DataRow(MockLongSerializer.CDataSize, MockStringSerializer.CDataSize + 1)]
        [DataRow(MockLongSerializer.CDataSize + 1, MockStringSerializer.CDataSize)]
        [DataRow(MockLongSerializer.CDataSize - 1, MockStringSerializer.CDataSize)]
        public void InsertAtLeafThrowsWhenBufferSizeIsWrong(long keySize, long valueSize)
        {
            using (MemoryStream ms = new MemoryStream())
            {
                var node = CreateMockNode(ms, out var storage, out var tree, true, 5);
                using (storage)
                {
                    node.KeyValuePairCount = 3;

                    Assert.ThrowsException<ArgumentException>(() => {
                        node.InsertAtLeaf(new byte[keySize], new byte[valueSize], 0);
                    });
                }
            }
        }

        [TestMethod]
        public void InsertAtLeafThrowsWhenFull()
        {
            using (MemoryStream ms = new MemoryStream())
            {
                var node = CreateMockNode(ms, out var storage, out var tree, true, 5);
                using (storage)
                {
                    node.KeyValuePairCount = node.MaxKeyValuePairCapacity;

                    Assert.ThrowsException<InvalidOperationException>(() => {
                        node.InsertAtLeaf(new byte[node.KeySize], new byte[node.ValueSize], 0);
                    });
                }
            }
        }

        [TestMethod]
        public void InsertAtLeafThrowsWhenEmpty()
        {
            using (MemoryStream ms = new MemoryStream())
            {
                var node = CreateMockNode(ms, out var storage, out var tree, true, 5);
                using (storage)
                {
                    node.KeyValuePairCount = 0;

                    Assert.ThrowsException<InvalidOperationException>(() => {
                        node.InsertAtLeaf(new byte[node.KeySize], new byte[node.ValueSize], 0);
                    });
                }
            }
        }

        [TestMethod]
        public void InsertAtLeafThrowsWhenNodeIsNotALeaf()
        {
            using (MemoryStream ms = new MemoryStream())
            {
                var node = CreateMockNode(ms, out var storage, out var tree, false, 5);
                using (storage)
                {
                    node.KeyValuePairCount = 2;

                    Assert.ThrowsException<InvalidOperationException>(() => {
                        node.InsertAtLeaf(new byte[node.KeySize], new byte[node.ValueSize], 0);
                    });
                }
            }
        }
        
        [DataTestMethod]
        [DataRow(5, 1, -1)]
        [DataRow(5, 1, 2)]
        [DataRow(5, 1, 3)]
        [DataRow(5, 1, 4)]
        [DataRow(5, 2, -1)]
        [DataRow(5, 2, 3)]
        [DataRow(5, 2, 4)]
        [DataRow(5, 3, -1)]
        [DataRow(5, 3, 4)]
        [DataRow(5, 4, -1)]
        [DataRow(5, 4, 5)]
        [DataRow(7, 2, -1)]
        [DataRow(7, 2, 3)]
        [DataRow(7, 2, 4)]
        [DataRow(7, 2, 5)]
        [DataRow(7, 2, 6)]
        [DataRow(7, 4, -1)]
        [DataRow(7, 4, 5)]
        [DataRow(7, 4, 6)]
        public void InsertAtLeafThrowsWhenArgIsOutOfRange(long capacity, long initCount, long keyValuePairIndex)
        {
            using (MemoryStream ms = new MemoryStream())
            {
                var node = CreateMockNode(ms, out var storage, out var tree, true, capacity);
                using (storage)
                {
                    node.KeyValuePairCount = initCount;

                    Assert.ThrowsException<ArgumentOutOfRangeException>(() => {
                        node.InsertAtLeaf(new byte[node.KeySize], new byte[node.ValueSize], keyValuePairIndex);
                    });
                }
            }
        }

        [TestMethod]
        public void InsertAtLeafThrowsWhenReadOnly()
        {
            using (MemoryStream ms = new MemoryStream())
            {
                long nodeIndex;
                using (var storage = StreamingPageStorage.Create(ms, 10240, 1, null, new CancellationToken(false), true))
                {
                    //Initialize the mock B-Tree while 'storage' is writable
                    MockBTree bTree = new MockBTree(storage, new MockLongSerializer(), new MockStringSerializer());

                    //Allocate the nodes
                    Assert.IsTrue(BTreeNode<long, string>.TryCreateNew(bTree, true, out var node));
                    nodeIndex = node.PageIndex;

                    //Initialize the count
                    node.KeyValuePairCount = 5;
                }

                //Re-load the storage as read-only
                using (var storage = StreamingPageStorage.Load(ms, true, true, true))
                {
                    //Re-load the MockBTree as read-only (determined by the storage being read-only)
                    var bTree = new MockBTree(storage, new MockLongSerializer(), new MockStringSerializer());

                    //Re-load the nodes
                    var node = new BTreeNode<long, string>(bTree, nodeIndex);

                    Assert.ThrowsException<InvalidOperationException>(() => {
                        node.InsertAtLeaf(new byte[node.KeySize], new byte[node.ValueSize], 0);
                    });
                }
            }
        }

        [DataTestMethod]
        [DataRow(5, 1)]
        [DataRow(5, 2)]
        [DataRow(5, 3)]
        [DataRow(5, 4)]
        [DataRow(63, 1)]
        [DataRow(63, 2)]
        [DataRow(63, 3)]
        [DataRow(63, 4)]
        [DataRow(63, 5)]
        [DataRow(63, 6)]
        [DataRow(63, 7)]
        [DataRow(63, 8)]
        [DataRow(63, 9)]
        [DataRow(63, 61)]
        [DataRow(63, 62)]
        public void InsertAtLeafWorks(long capacity, long initCount)
        {
            for (long maxMoveCount = 1; maxMoveCount < capacity * 2 /* *2 to ensure it goes over*/; maxMoveCount *= 2)
            {
                for (long i = 0; i <= initCount; i++)//Test insertion at every possible index
                {
                    using (MemoryStream ms = new MemoryStream())
                    {
                        var node = CreateMockNode(ms, out var storage, out var tree, true, capacity, 3, maxMoveCount);
                        using (storage)
                        {
                            Assert.AreEqual(maxMoveCount, tree.MaxMoveKeyValuePairCount);
                            byte[] keyBuffer = new byte[node.KeySize];
                            byte[] valueBuffer = new byte[node.ValueSize];
                            tree.KeySerializer.Serialize(-1, keyBuffer);
                            tree.ValueSerializer.Serialize("Hello World", valueBuffer);

                            //Setup the key-value pairs
                            node.KeyValuePairCount = initCount;
                            for (long j = 0; j < initCount; j++)
                            {
                                node.SetKeyAt(j, j);
                                node.SetValueAt(j, j.ToString());
                            }

                            //Insert into the node
                            node.InsertAtLeaf(keyBuffer, valueBuffer, i);

                            //Make sure the key-value pairs are correct
                            for (long j = 0; j < initCount + 1; j++)
                            {
                                long expectedKey = j;
                                if (j > i)
                                    expectedKey--;
                                string expectedValue = expectedKey.ToString();
                                if (j == i)
                                {
                                    expectedKey = -1;
                                    expectedValue = "Hello World";
                                }

                                Assert.AreEqual(expectedKey, node.GetKeyAt(j));
                                Assert.AreEqual(expectedValue, node.GetValueAt(j));
                            }

                            Assert.AreEqual(initCount + 1, node.KeyValuePairCount);
                        }
                    }
                }
            }
        }

        [TestMethod]
        public void RemoveAtLeafThrowsWhenReadOnly()
        {
            using (MemoryStream ms = new MemoryStream())
            {
                long nodeIndex;
                using (var storage = StreamingPageStorage.Create(ms, 10240, 1, null, new CancellationToken(false), true))
                {
                    //Initialize the mock B-Tree while 'storage' is writable
                    MockBTree bTree = new MockBTree(storage, new MockLongSerializer(), new MockStringSerializer());

                    //Allocate the nodes
                    Assert.IsTrue(BTreeNode<long, string>.TryCreateNew(bTree, true, out var node));
                    nodeIndex = node.PageIndex;

                    //Initialize the count
                    node.KeyValuePairCount = 5;
                }

                //Re-load the storage as read-only
                using (var storage = StreamingPageStorage.Load(ms, true, true, true))
                {
                    //Re-load the MockBTree as read-only (determined by the storage being read-only)
                    var bTree = new MockBTree(storage, new MockLongSerializer(), new MockStringSerializer());

                    //Re-load the nodes
                    var node = new BTreeNode<long, string>(bTree, nodeIndex);

                    Assert.ThrowsException<InvalidOperationException>(() => {
                        node.RemoveAtLeaf(0);
                    });
                }
            }
        }

        [TestMethod]
        public void RemoveAtLeafThrowsWhenNodeIsNotALeaf()
        {
            using (MemoryStream ms = new MemoryStream())
            {
                var node = CreateMockNode(ms, out var storage, out var tree, false, 5);
                using (storage)
                {
                    Assert.IsTrue(BTreeNode<long, string>.TryCreateNew(tree, true, out var subRootNode));
                    node.KeyValuePairCount = 2;

                    Assert.ThrowsException<InvalidOperationException>(() => {
                        node.RemoveAtLeaf(0);
                    });
                }
            }
        }

        [DataTestMethod]
        [DataRow(5, 1, -1)]
        [DataRow(5, 1, 1)]
        [DataRow(5, 2, -1)]
        [DataRow(5, 2, 2)]
        [DataRow(5, 3, -1)]
        [DataRow(5, 3, 3)]
        [DataRow(5, 4, -1)]
        [DataRow(5, 4, 4)]
        [DataRow(5, 5, -1)]
        [DataRow(5, 5, 5)]
        [DataRow(7, 1, -1)]
        [DataRow(7, 1, 1)]
        [DataRow(7, 1, 2)]
        [DataRow(7, 7, -1)]
        [DataRow(7, 7, 7)]
        [DataRow(7, 7, 8)]
        public void RemoveAtLeafThrowsWhenIndexOutOfRange(long capacity, long initCount, long badIndex)
        {
            using (MemoryStream ms = new MemoryStream())
            {
                var node = CreateMockNode(ms, out var storage, out var tree, true, capacity);
                using (storage)
                {
                    Assert.IsTrue(BTreeNode<long, string>.TryCreateNew(tree, true, out var subRootNode));
                    node.KeyValuePairCount = initCount;

                    Assert.ThrowsException<ArgumentOutOfRangeException>(() => {
                        node.RemoveAtLeaf(badIndex);
                    });
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
        [DataRow(7, 4)]
        [DataRow(7, 5)]
        [DataRow(7, 6)]
        [DataRow(7, 7)]
        [DataRow(63, 61)]
        [DataRow(63, 62)]
        [DataRow(63, 63)]
        public void RemoveAtLeafWorks(long capacity, long initCount)
        {
            for (long maxMoveCount = 1; maxMoveCount < capacity * 2/* *2 to go over*/; maxMoveCount *= 2)
            {
                for (long i = 0; i < initCount; i++)//Test removal at every possible index
                {
                    using (MemoryStream ms = new MemoryStream())
                    {
                        var node = CreateMockNode(ms, out var storage, out var tree, true, capacity, 3, maxMoveCount);
                        using(storage)
                        {
                            Assert.AreEqual(maxMoveCount, tree.MaxMoveKeyValuePairCount);
                            byte[] keyBuffer = new byte[node.KeySize];
                            byte[] valueBuffer = new byte[node.ValueSize];
                            tree.KeySerializer.Serialize(-1, keyBuffer);
                            tree.ValueSerializer.Serialize("Hello World", valueBuffer);

                            //Setup the key-value pairs
                            node.KeyValuePairCount = initCount;
                            for (long j = 0; j < initCount; j++)
                            {
                                node.SetKeyAt(j, j);
                                node.SetValueAt(j, j.ToString());
                            }

                            //Remove into the node
                            node.RemoveAtLeaf(i);

                            //Make sure the key-value pairs are correct
                            for (long j = 0; j < initCount - 1; j++)
                            {
                                long expectedKey = j;
                                if (j >= i)
                                    expectedKey++;
                                string expectedValue = expectedKey.ToString();
                                Assert.AreEqual(expectedKey, node.GetKeyAt(j));
                                Assert.AreEqual(expectedValue, node.GetValueAt(j));
                            }

                            Assert.AreEqual(initCount - 1, node.KeyValuePairCount);
                        }
                    }
                }
            }
        }
        
        [DataTestMethod]
        [DataRow(6, 1, -1)]
        [DataRow(6, 1, 1)]
        [DataRow(6, 1, 2)]
        [DataRow(6, 2, -1)]
        [DataRow(6, 2, 2)]
        [DataRow(6, 2, 3)]
        [DataRow(6, 3, -1)]
        [DataRow(6, 3, 3)]
        [DataRow(6, 3, 4)]
        [DataRow(6, 4, -1)]
        [DataRow(6, 4, 4)]
        [DataRow(6, 4, 5)]
        [DataRow(6, 5, -1)]
        [DataRow(6, 5, 5)]
        [DataRow(6, 5, 6)]
        [DataRow(6, 6, -1)]
        [DataRow(6, 6, 6)]
        [DataRow(6, 6, 7)]
        [DataRow(8, 8, -1)]
        [DataRow(8, 8, 8)]
        [DataRow(8, 8, 9)]
        [DataRow(16, 15, -1)]
        [DataRow(16, 15, 15)]
        [DataRow(16, 15, 16)]
        [DataRow(16, 15, 17)]
        public void SplitSubTreeAtThrowsWhenIndexOutOfRange(long subTreeCapacity, long subTreeCount, long badIndex)
        {
            using (MemoryStream ms = new MemoryStream())
            {
                long keyValueCapacity = subTreeCapacity - 1;
                long keyValueCount = subTreeCount - 1;
                var node = CreateMockNode(ms, out var storage, out var tree, false, keyValueCapacity);
                using (storage)
                {
                    node.KeyValuePairCount = keyValueCount;

                    Assert.IsTrue(BTreeNode<long, string>.TryCreateNew(tree, true, out var toSplit));
                    Assert.IsTrue(BTreeNode<long, string>.TryCreateNew(tree, true, out var newSplitNode));

                    toSplit.KeyValuePairCount = toSplit.MaxKeyValuePairCapacity;
                    node.SetSubTreeAt(0, toSplit);

                    Assert.ThrowsException<ArgumentOutOfRangeException>(() => {
                        node.SplitSubTreeAt(badIndex, newSplitNode);
                    });
                }
            }
        }

        [TestMethod]
        public void SplitSubTreeAtThrowsWhenNewNodeIsNull()
        {
            using (MemoryStream ms = new MemoryStream())
            {
                var node = CreateMockNode(ms, out var storage, out var tree, false, 7);
                using (storage)
                {
                    node.KeyValuePairCount = 5;

                    Assert.IsTrue(BTreeNode<long, string>.TryCreateNew(tree, true, out var toSplit));

                    toSplit.KeyValuePairCount = toSplit.MaxKeyValuePairCapacity;
                    node.SetSubTreeAt(0, toSplit);

                    Assert.ThrowsException<ArgumentNullException>(() => {
                        node.SplitSubTreeAt(0, null);
                    });
                }
            }
        }

        [TestMethod]
        public void SplitSubTreeAtThrowsWhenNewNodeIsNotEmpty()
        {
            using (MemoryStream ms = new MemoryStream())
            {
                var node = CreateMockNode(ms, out var storage, out var tree, false, 7);
                using (storage)
                {
                    node.KeyValuePairCount = 5;

                    Assert.IsTrue(BTreeNode<long, string>.TryCreateNew(tree, true, out var toSplit));
                    Assert.IsTrue(BTreeNode<long, string>.TryCreateNew(tree, true, out var newSplitNode));

                    toSplit.KeyValuePairCount = toSplit.MaxKeyValuePairCapacity;
                    node.SetSubTreeAt(0, toSplit);

                    //Make the 'newSplitNode' non-empty
                    newSplitNode.KeyValuePairCount = 1;

                    Assert.ThrowsException<ArgumentException>(() => {
                        node.SplitSubTreeAt(0, newSplitNode);
                    });
                }
            }
        }

        [TestMethod]
        public void SplitSubTreeAtThrowsWhenThisNodeIsALeaf()
        {
            using (MemoryStream ms = new MemoryStream())
            {
                var node = CreateMockNode(ms, out var storage, out var tree, true, 7);
                using (storage)
                {
                    node.KeyValuePairCount = 5;

                    Assert.IsTrue(BTreeNode<long, string>.TryCreateNew(tree, true, out var newSplitNode));

                    Assert.ThrowsException<InvalidOperationException>(() => {
                        node.SplitSubTreeAt(0, newSplitNode);
                    });
                }
            }
        }

        [TestMethod]
        public void SplitSubTreeAtThrowsWhenReadOnly()
        {
            using (MemoryStream ms = new MemoryStream())
            {
                long nodeIndex, newSplitNodeIndex;
                using (var storage = StreamingPageStorage.Create(ms, 10240, 1, null, new CancellationToken(false), true))
                {
                    //Initialize the mock B-Tree while 'storage' is writable
                    MockBTree bTree = new MockBTree(storage, new MockLongSerializer(), new MockStringSerializer());

                    //Allocate the nodes
                    Assert.IsTrue(BTreeNode<long, string>.TryCreateNew(bTree, true, out var node));
                    nodeIndex = node.PageIndex;
                    Assert.IsTrue(BTreeNode<long, string>.TryCreateNew(bTree, true, out var newSplitNode));
                    newSplitNodeIndex = node.PageIndex;

                    //Initialize the count
                    node.KeyValuePairCount = 5;
                }

                //Re-load the storage as read-only
                using (var storage = StreamingPageStorage.Load(ms, true, true, true))
                {
                    //Re-load the MockBTree as read-only (determined by the storage being read-only)
                    var bTree = new MockBTree(storage, new MockLongSerializer(), new MockStringSerializer());

                    //Re-load the nodes
                    var node = new BTreeNode<long, string>(bTree, nodeIndex);
                    var newSplitNode = new BTreeNode<long, string>(bTree, newSplitNodeIndex);

                    Assert.ThrowsException<InvalidOperationException>(() => {
                        node.SplitSubTreeAt(0, newSplitNode);
                    });
                }
            }
        }

        [TestMethod]
        public void SplitSubTreeAtThrowsWhenThisNodeIsFull()
        {
            using (MemoryStream ms = new MemoryStream())
            {
                var node = CreateMockNode(ms, out var storage, out var tree, false, 7);
                using (storage)
                {
                    node.KeyValuePairCount = node.MaxKeyValuePairCapacity;
                    for(long i = 0; i < node.SubTreeCount; i++)
                    {
                        Assert.IsTrue(BTreeNode<long, string>.TryCreateNew(tree, true, out var subTreeNode));
                        node.SetSubTreeAt(i, subTreeNode);
                    }

                    Assert.IsTrue(BTreeNode<long, string>.TryCreateNew(tree, true, out var newSplitNode));

                    Assert.ThrowsException<InvalidOperationException>(() => {
                        node.SplitSubTreeAt(0, newSplitNode);
                    });
                }
            }
        }

        [DataTestMethod]
        [DataRow(5, 1, false, 1)]
        [DataRow(5, 1, false, 2)]
        [DataRow(5, 1, false, 3)]
        [DataRow(5, 2, false, 1)]
        [DataRow(5, 2, false, 2)]
        [DataRow(5, 3, false, 2)]
        [DataRow(5, 3, false, 1)]
        [DataRow(5, 3, false, 2)]
        [DataRow(5, 3, false, 3)]
        [DataRow(5, 4, false, 1)]
        [DataRow(5, 4, false, 2)]
        [DataRow(5, 4, false, 3)]
        [DataRow(5, 4, false, 4)]
        [DataRow(9, 1, false, 1)]
        [DataRow(9, 2, false, 1)]
        [DataRow(9, 2, false, 2)]
        [DataRow(9, 3, false, 1)]
        [DataRow(9, 3, false, 2)]
        [DataRow(9, 3, false, 3)]
        [DataRow(9, 4, false, 1)]
        [DataRow(9, 4, false, 2)]
        [DataRow(9, 4, false, 3)]
        [DataRow(9, 4, false, 4)]
        [DataRow(9, 5, false, 1)]
        [DataRow(9, 5, false, 2)]
        [DataRow(9, 5, false, 3)]
        [DataRow(9, 5, false, 4)]
        [DataRow(9, 5, false, 5)]
        [DataRow(9, 8, false, 1)]
        [DataRow(9, 8, false, 7)]
        [DataRow(9, 8, false, 8)]
        [DataRow(5, 1, true, 0)]
        [DataRow(5, 2, true, 0)]
        [DataRow(5, 3, true, 0)]
        [DataRow(5, 4, true, 0)]
        [DataRow(9, 1, true, 0)]
        [DataRow(9, 2, true, 0)]
        [DataRow(9, 3, true, 0)]
        [DataRow(9, 4, true, 0)]
        [DataRow(9, 5, true, 0)]
        [DataRow(9, 8, true, 0)]
        public void SplitSubTreeAtWorks(long capacity, long keyValueCount, bool isSubTreeALeaf, long subSubTreeKeyValuePairCount)
        {
            Assert.IsTrue(capacity > 2 && capacity % 2 != 0/*Must be an odd number*/);
            Assert.IsTrue(capacity > keyValueCount);//Need space for the insertion that results from split
            long subTreeCount = keyValueCount + 1;
            for (long subTreeIndex = 0; subTreeIndex < subTreeCount; subTreeIndex++)//Test all possible indices
            {
                using (MemoryStream ms = new MemoryStream())
                {
                    var node = CreateMockNode(ms, out var storage, out var tree, false, capacity);
                    using (storage)
                    {
                        long keyIncrementer = 0;
                        Dictionary<long, string> valuesByKey = new Dictionary<long, string>();
                        node.KeyValuePairCount = keyValueCount;
                        for (long i = 0; i < subTreeCount; i++)
                        {
                            //Create the sub-tree root node
                            Assert.IsTrue(BTreeNode<long, string>.TryCreateNew(tree, isSubTreeALeaf, out var subTreeNode));

                            if (i > 0)
                            {
                                //Add a key-value pair to this spot in the node
                                long key = keyIncrementer++;
                                node.SetKeyAt(i - 1, key);
                                string value = "root_" + key.ToString();
                                node.SetValueAt(i - 1, value);
                                valuesByKey.Add(key, value);
                            }

                            //Fill the sub-tree node full (necessary for split)
                            subTreeNode.KeyValuePairCount = subTreeNode.MaxKeyValuePairCapacity;
                            for (long j = 0; j < subTreeNode.MaxKeyValuePairCapacity; j++)
                            {
                                if (j == 0)
                                {
                                    if (!isSubTreeALeaf)
                                    {
                                        //Add the first left sub-sub-tree (it will be a leaf)
                                        Assert.IsTrue(BTreeNode<long, string>.TryCreateNew(tree, true, out var subSubTreeNode));

                                        //Fill it
                                        subSubTreeNode.KeyValuePairCount = subSubTreeKeyValuePairCount;
                                        for (long k = 0; k < subSubTreeKeyValuePairCount; k++)
                                        {
                                            var key_ = keyIncrementer++;
                                            subSubTreeNode.SetKeyAt(k, key_);
                                            string value_ = "subsub_" + key_.ToString();
                                            subSubTreeNode.SetValueAt(k, value_);
                                            valuesByKey.Add(key_, value_);
                                        }

                                        //Add it to the sub-tree
                                        subTreeNode.SetSubTreeAt(j, subSubTreeNode);
                                    }
                                }

                                //Add the key-value pair
                                long key = keyIncrementer++;
                                subTreeNode.SetKeyAt(j, key);
                                string value = "sub_" + key.ToString();
                                subTreeNode.SetValueAt(j, value);
                                valuesByKey.Add(key, value);

                                if (!isSubTreeALeaf)
                                {
                                    //Add the sub-tree to the right of this key-value pair
                                    Assert.IsTrue(BTreeNode<long, string>.TryCreateNew(tree, true, out var subSubTreeNode));

                                    //Fill it
                                    subSubTreeNode.KeyValuePairCount = subSubTreeKeyValuePairCount;
                                    for (long k = 0; k < subSubTreeKeyValuePairCount; k++)
                                    {
                                        var key_ = keyIncrementer++;
                                        subSubTreeNode.SetKeyAt(k, key_);
                                        string value_ = "subsub_" + key_.ToString();
                                        subSubTreeNode.SetValueAt(k, value_);
                                        valuesByKey.Add(key_, value_);
                                    }

                                    //Add it to the sub-tree
                                    subTreeNode.SetSubTreeAt(j + 1/*+1 to put it right of the key-value pair*/, subSubTreeNode);
                                }
                            }

                            //Add the sub-tree to the node
                            node.SetSubTreeAt(i, subTreeNode);
                        }

                        //Split node at 'subTreeIndex'
                        Assert.IsTrue(BTreeNode<long, string>.TryCreateNew(tree, true, out var newSplitNode));
                        node.SplitSubTreeAt(subTreeIndex, newSplitNode);

                        //Make sure the node's KeyValuePairCount incremented by one 
                        //(since the sub-tree's middle key-value pair was inserted to it)
                        Assert.AreEqual(keyValueCount + 1, node.KeyValuePairCount);

                        long finalKey = keyIncrementer;
                        //Reset the key incrementer
                        keyIncrementer = 0;

                        //Make sure the split was correct
                        for (long i = 0; i < node.KeyValuePairCount; i++)
                        {
                            var leftSubTree = node.GetSubTreeAt(i);
                            var rightSubTree = node.GetSubTreeAt(i + 1);

                            long expectedLeftSubTreeKVC = capacity;
                            long expectedRightSubTreeKVC = capacity;
                            if (i == subTreeIndex - 1)
                            {
                                expectedRightSubTreeKVC = capacity / 2;
                            }
                            else if (i == subTreeIndex)
                            {
                                expectedLeftSubTreeKVC = capacity / 2;
                                expectedRightSubTreeKVC = capacity / 2;
                            }
                            else if (i == subTreeIndex + 1)
                            {
                                expectedLeftSubTreeKVC = capacity / 2;
                            }

                            //Make sure the left and right sub-trees surrounding the key-value
                            //pair at 'i' are the expected size
                            Assert.AreEqual(expectedLeftSubTreeKVC, leftSubTree.KeyValuePairCount);
                            Assert.AreEqual(expectedRightSubTreeKVC, rightSubTree.KeyValuePairCount);

                            //If i==0, we have to check left+right subtree. 
                            //Otherwise, only check the right one (since left was already checked in previour iteration)
                            BTreeNode<long, string>[] subTreesToCheck;
                            if (i == 0)
                                subTreesToCheck = new BTreeNode<long, string>[] { leftSubTree, rightSubTree };
                            else
                                subTreesToCheck = new BTreeNode<long, string>[] { rightSubTree };

                            bool passedLeftSubTree = i > 0;//If we have to check left+right subtree, did we already pass the left one?
                            foreach (var subTree in subTreesToCheck)
                            {
                                if (passedLeftSubTree)
                                {
                                    //We already passed the left sub-tree,
                                    //so now validate the root node's key-value pair at i
                                    long expectedKey = keyIncrementer++;
                                    long gotKey = node.GetKeyAt(i);
                                    Assert.AreEqual(expectedKey, gotKey);
                                    string expectedValue = valuesByKey[expectedKey];
                                    string gotValue = node.GetValueAt(i);
                                    Assert.AreEqual(expectedValue, gotValue);
                                }

                                for (long j = 0; j < subTree.KeyValuePairCount; j++)
                                {
                                    if (j == 0)
                                    {
                                        if (!subTree.IsLeaf)
                                        {
                                            //Scan the left sub-sub-tree
                                            var subSubTree = subTree.GetSubTreeAt(j);
                                            Assert.IsTrue(subSubTree.IsLeaf);
                                            Assert.AreEqual(subSubTreeKeyValuePairCount, subSubTree.KeyValuePairCount);
                                            for (long k = 0; k < subSubTreeKeyValuePairCount; k++)
                                            {
                                                long expectedKey_ = keyIncrementer++;
                                                long gotKey_ = subSubTree.GetKeyAt(k);
                                                Assert.AreEqual(expectedKey_, gotKey_);
                                                string expectedValue_ = valuesByKey[expectedKey_];
                                                string gotValue_ = subSubTree.GetValueAt(k);
                                                Assert.AreEqual(expectedValue_, gotValue_);
                                            }
                                        }
                                    }

                                    //Scan the key-value pair in the subTree at j
                                    long expectedKey = keyIncrementer++;
                                    long gotKey = subTree.GetKeyAt(j);
                                    Assert.AreEqual(expectedKey, gotKey);
                                    string expectedValue = valuesByKey[expectedKey];
                                    string gotValue = subTree.GetValueAt(j);
                                    Assert.AreEqual(expectedValue, gotValue);

                                    if (!subTree.IsLeaf)
                                    {
                                        //Scan the right sub-sub-tree
                                        var subSubTree = subTree.GetSubTreeAt(j + 1/*+1 to take the RIGHT subtree*/);
                                        Assert.IsTrue(subSubTree.IsLeaf);
                                        Assert.AreEqual(subSubTreeKeyValuePairCount, subSubTree.KeyValuePairCount);
                                        for (long k = 0; k < subSubTreeKeyValuePairCount; k++)
                                        {
                                            long expectedKey_ = keyIncrementer++;
                                            long gotKey_ = subSubTree.GetKeyAt(k);
                                            Assert.AreEqual(expectedKey_, gotKey_);
                                            string expectedValue_ = valuesByKey[expectedKey_];
                                            string gotValue_ = subSubTree.GetValueAt(k);
                                            Assert.AreEqual(expectedValue_, gotValue_);
                                        }
                                    }
                                }

                                passedLeftSubTree = true;
                            }
                        }

                        //Make sure we accounted for all keys
                        Assert.AreEqual(finalKey, keyIncrementer);
                    }
                }
            }
        }
    }
}
