using Microsoft.VisualStudio.TestTools.UnitTesting;
using Storage;
using Storage.Data;
using StorageTest.Mocks;
using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading;

namespace StorageTest
{
    [TestClass]
    public class BTreeReaderTest
    {
        [TestMethod]
        public void ConstructorThrowsWhenArgIsNull()
        {
            Assert.ThrowsException<ArgumentNullException>(()=> {
                var reader = new BTreeReader<long, string>(null);
            });
        }

        [TestMethod]
        public void ConstructorThrowsWhenArgIsNotReadOnly()
        {
            using (MemoryStream ms = new MemoryStream())
            {
                using (MockStream stream = new MockStream(ms, true, true, true))
                {
                    long pageSize = BTreeNode<long, string>.GetRequiredPageSize(MockLongSerializer.CDataSize, MockStringSerializer.CDataSize, BTreeNode<long, string>.VeryMinKeyValuePairCapacity);
                    using (StreamingPageStorage storage = StreamingPageStorage.Create(stream, pageSize, 1, null, new CancellationToken(false), true))
                    {
                        MockBTree tree = new MockBTree(storage, new MockLongSerializer(), new MockStringSerializer());

                        Assert.ThrowsException<ArgumentException>(() => {
                            var reader = new BTreeReader<long, string>(tree);
                        });
                    }
                }
            }
        }

        [DataTestMethod]
        [DataRow(0)]
        [DataRow(1)]
        [DataRow(1024)]
        public void ConstructorWorks(long count)
        {
            using (MemoryStream ms = new MemoryStream())
            {
                using (MockStream stream = new MockStream(ms, true, true, true))
                {
                    long pageSize = BTreeNode<long, string>.GetRequiredPageSize(MockLongSerializer.CDataSize, MockStringSerializer.CDataSize, BTreeNode<long, string>.VeryMinKeyValuePairCapacity);
                    using (var storage = StreamingPageStorage.Create(stream, pageSize, 1, null, new CancellationToken(false), true))
                    {
                        var tree = new MockBTree(storage, new MockLongSerializer(), new MockStringSerializer());

                        //Initialize some data into the tree
                        for (long i = 0; i < count; i++)
                            Assert.IsTrue(tree.Insert(i, i.ToString(), false, out _));
                    }

                    //Load the tree as read-only
                    using (var storage = StreamingPageStorage.Load(stream, true, true, true))
                    {
                        var tree = new MockBTree(storage, new MockLongSerializer(), new MockStringSerializer());

                        BTreeReader<long, string> reader = new BTreeReader<long, string>(tree);
                        Assert.AreSame(tree, reader.BTree);

                        if (tree.Root != null)
                            Assert.AreEqual(tree.Root.PageIndex, reader.RootNode.PageIndex);
                        else
                            Assert.IsNull(reader.RootNode);
                    }
                }
            }
        }
    }
}
