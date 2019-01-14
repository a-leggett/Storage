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
    public class StorageDictionaryTest
    {
        [TestMethod]
        public void TryCreateThrowsWhenArgIsNull()
        {
            using (MemoryStream ms = new MemoryStream())
            {
                using (MockPageStorage storage = new MockPageStorage(ms, 1024, 1, false, false))
                {
                    Assert.ThrowsException<ArgumentNullException>(() => {
                        StorageDictionary<long, string>.TryCreate(null, new MockLongSerializer(), new MockStringSerializer(), out _);
                    });

                    Assert.ThrowsException<ArgumentNullException>(() => {
                        StorageDictionary<long, string>.TryCreate(storage, null, new MockStringSerializer(), out _);
                    });

                    Assert.ThrowsException<ArgumentNullException>(() => {
                        StorageDictionary<long, string>.TryCreate(storage, new MockLongSerializer(), null, out _);
                    });
                }
            }
        }

        [TestMethod]
        public void TryCreateThrowsWhenStorageIsReadOnly()
        {
            using (MemoryStream ms = new MemoryStream())
            {
                using (MockPageStorage storage = new MockPageStorage(ms, 1024, 1, true, true))
                {
                    Assert.ThrowsException<InvalidOperationException>(() => {
                        StorageDictionary<long, string>.TryCreate(storage, new MockLongSerializer(), new MockStringSerializer(), out _);
                    });
                }
            }
        }

        [TestMethod]
        public void TryCreateThrowsWhenPageSizeIsTooSmall()
        {
            using (MemoryStream ms = new MemoryStream())
            {
                long minPageSize = StorageDictionary<long, string>.GetVeryMinRequiredPageSize(MockLongSerializer.CDataSize, MockStringSerializer.CDataSize);
                using (MockPageStorage storage = new MockPageStorage(ms, minPageSize - 1, 1, false, false))
                {
                    Assert.ThrowsException<ArgumentException>(() => {
                        StorageDictionary<long, string>.TryCreate(storage, new MockLongSerializer(), new MockStringSerializer(), out _);
                    });
                }
            }
        }

        [TestMethod]
        public void TryCreateWorks()
        {
            using (MemoryStream ms = new MemoryStream())
            {
                using (MockPageStorage storage = new MockPageStorage(ms, 1024, 1, false, false))
                {
                    var keySerializer = new MockLongSerializer();
                    var valueSerializer = new MockStringSerializer();
                    StorageDictionary<long, string>.TryCreate(storage, keySerializer, valueSerializer, out var dictionaryPageIndex);

                    using (var dictionary = StorageDictionary<long, string>.Load(storage, keySerializer, valueSerializer, dictionaryPageIndex, false))
                    {
                        Assert.AreEqual(0, dictionary.Count);
                        Assert.IsFalse(dictionary.IsReadOnly);
                        Assert.AreSame(keySerializer, dictionary.KeySerializer);
                        Assert.AreSame(valueSerializer, dictionary.ValueSerializer);
                    }
                }
            }
        }

        [TestMethod]
        public void TryCreateInflatesIfNecessaryAndPossible()
        {
            using (MemoryStream ms = new MemoryStream())
            {
                using (MockPageStorage storage = new MockPageStorage(ms, 1024, 8, false, false))
                {
                    //Allocate the storage to full capacity
                    for (long i = 0; i < storage.PageCapacity; i++)
                        Assert.IsTrue(storage.TryAllocatePage(out _));

                    long initCapacity = storage.PageCapacity;

                    var keySerializer = new MockLongSerializer();
                    var valueSerializer = new MockStringSerializer();
                    StorageDictionary<long, string>.TryCreate(storage, keySerializer, valueSerializer, out var dictionaryPageIndex);

                    using (var dictionary = StorageDictionary<long, string>.Load(storage, keySerializer, valueSerializer, dictionaryPageIndex, false))
                    {
                        //Make sure the dictionary was created correctly
                        Assert.AreEqual(0, dictionary.Count);
                        Assert.IsFalse(dictionary.IsReadOnly);
                        Assert.AreSame(keySerializer, dictionary.KeySerializer);
                        Assert.AreSame(valueSerializer, dictionary.ValueSerializer);

                        //Make sure the capacity was inflated
                        Assert.IsTrue(storage.PageCapacity > initCapacity);
                    }
                }
            }
        }

        [TestMethod]
        public void TryCreateFailsIfRequiredInflationFails()
        {
            using (MemoryStream ms = new MemoryStream())
            {
                const long pageSize = 1024;
                const long capacity = 8;
                long maxStreamSizeBeforeFail = StreamingPageStorage.GetRequiredStreamSize(pageSize, capacity);

                using (MockSafeResizableStorage stream = new MockSafeResizableStorage(ms, true, true, true, null, maxStreamSizeBeforeFail))
                {
                    using (MockPageStorage storage = new MockPageStorage(stream, pageSize, capacity, false, false))
                    {
                        //Allocate the storage to full capacity
                        for (long i = 0; i < storage.PageCapacity; i++)
                            Assert.IsTrue(storage.TryAllocatePage(out _));
                        
                        //Ensure that we fail to create a dictionary (gracefully)
                        Assert.IsFalse(StorageDictionary<long, string>.TryCreate(storage, new MockLongSerializer(), new MockStringSerializer(), out long index));
                        Assert.AreEqual(-1, index);
                    }
                }
            }
        }

        [TestMethod]
        public void TryCreateFailsIfInflationIsRequiredButCapacityIsFixed()
        {
            using (MemoryStream ms = new MemoryStream())
            {
                using (MockPageStorage storage = new MockPageStorage(ms, 1024, 8, false, true))
                {
                    //Allocate the storage to full capacity
                    for (long i = 0; i < storage.PageCapacity; i++)
                        Assert.IsTrue(storage.TryAllocatePage(out _));

                    long initCapacity = storage.PageCapacity;

                    //Ensure that there is no attempt to inflate since capacity is fixed
                    storage.OnTryInflate += (srg, amount) => {
                        Assert.Fail("There must not be any attempt to inflate when capacity is fixed.");
                    };

                    //Ensure that we fail to create a dictionary (gracefully)
                    Assert.IsFalse(StorageDictionary<long, string>.TryCreate(storage, new MockLongSerializer(), new MockStringSerializer(), out long index));
                    Assert.AreEqual(-1, index);
                    Assert.AreEqual(initCapacity, storage.PageCapacity);
                }
            }
        }

        [TestMethod]
        public void LoadThrowsWhenArgIsNull()
        {
            using (MemoryStream ms = new MemoryStream())
            {
                using (MockPageStorage storage = new MockPageStorage(ms, 1024, 1, false, false))
                {
                    Assert.IsTrue(storage.TryAllocatePage(out long index));
                    Assert.ThrowsException<ArgumentNullException>(() => {
                        StorageDictionary<long, string>.Load(null, new MockLongSerializer(), new MockStringSerializer(), index, false);
                    });

                    Assert.ThrowsException<ArgumentNullException>(() => {
                        StorageDictionary<long, string>.Load(storage, null, new MockStringSerializer(), index, false);
                    });

                    Assert.ThrowsException<ArgumentNullException>(() => {
                        StorageDictionary<long, string>.Load(storage, new MockLongSerializer(), null, index, false);
                    });
                }
            }
        }

        [DataTestMethod]
        [DataRow(-1)]
        [DataRow(-2)]
        public void LoadThrowsWhenCachePageCountIsTooSmall(int cachePageCount)
        {
            using (MemoryStream ms = new MemoryStream())
            {
                using (MockPageStorage storage = new MockPageStorage(ms, 1024, 1, false, false))
                {
                    Assert.IsTrue(StorageDictionary<long,string>.TryCreate(storage, new MockLongSerializer(), new MockStringSerializer(), out long index)); 
                    Assert.ThrowsException<ArgumentOutOfRangeException>(() => {
                        StorageDictionary<long, string>.Load(storage, new MockLongSerializer(), new MockStringSerializer(), index, false, cachePageCount);
                    });
                }
            }
        }

        [DataTestMethod]
        [DataRow(0)]
        [DataRow(-1)]
        [DataRow(-2)]
        public void LoadThrowsWhenMaxMoveCountIsLessThanOne(long maxMoveCount)
        {
            using (MemoryStream ms = new MemoryStream())
            {
                using (MockPageStorage storage = new MockPageStorage(ms, 1024, 1, false, false))
                {
                    Assert.IsTrue(StorageDictionary<long, string>.TryCreate(storage, new MockLongSerializer(), new MockStringSerializer(), out long index));
                    Assert.ThrowsException<ArgumentOutOfRangeException>(() => {
                        StorageDictionary<long, string>.Load(storage, new MockLongSerializer(), new MockStringSerializer(), index, false, 8, maxMoveCount);
                    });
                }
            }
        }

        [TestMethod]
        public void LoadThrowsWhenStorageIsReadOnlyButArgIsNot()
        {
            using (MemoryStream ms = new MemoryStream())
            {
                long index;
                using (StreamingPageStorage storage = StreamingPageStorage.Create(ms, 1024, 1, null, new CancellationToken(false), true))
                {
                    Assert.IsTrue(StorageDictionary<long, string>.TryCreate(storage, new MockLongSerializer(), new MockStringSerializer(), out index));
                }

                using (StreamingPageStorage storage = StreamingPageStorage.Load(ms, true, true, true))
                {
                    Assert.ThrowsException<ArgumentException>(()=> {
                        StorageDictionary<long, string>.Load(storage, new MockLongSerializer(), new MockStringSerializer(), index, false);
                    });
                }
            }
        }

        [TestMethod]
        public void LoadThrowsWhenPageIndexIsNotOnStorage()
        {
            using (MemoryStream ms = new MemoryStream())
            {
                using (StreamingPageStorage storage = StreamingPageStorage.Create(ms, 1024, 1, null, new CancellationToken(false), true))
                {
                    Assert.ThrowsException<ArgumentException>(() =>
                    {
                        StorageDictionary<long, string>.Load(storage, new MockLongSerializer(), new MockStringSerializer(), -1, false);
                    });

                    Assert.ThrowsException<ArgumentException>(() =>
                    {
                        StorageDictionary<long, string>.Load(storage, new MockLongSerializer(), new MockStringSerializer(), storage.PageCapacity, false);
                    });
                }
            }
        }

        [TestMethod]
        public void LoadThrowsWhenPageIndexIsNotAllocated()
        {
            using (MemoryStream ms = new MemoryStream())
            {
                using (StreamingPageStorage storage = StreamingPageStorage.Create(ms, 1024, 10, null, new CancellationToken(false), true))
                {
                    Assert.ThrowsException<ArgumentException>(() =>
                    {
                        StorageDictionary<long, string>.Load(storage, new MockLongSerializer(), new MockStringSerializer(), 0, false);
                    });

                    Assert.ThrowsException<ArgumentException>(() =>
                    {
                        StorageDictionary<long, string>.Load(storage, new MockLongSerializer(), new MockStringSerializer(), storage.PageCapacity - 1, false);
                    });
                }
            }
        }

        [TestMethod]
        public void LoadThrowsWhenPageSizeIsTooSmall()
        {
            using (MemoryStream ms = new MemoryStream())
            {
                long veryMinPageSize = StorageDictionary<long, string>.GetVeryMinRequiredPageSize(MockLongSerializer.CDataSize, MockStringSerializer.CDataSize);
                using (StreamingPageStorage storage = StreamingPageStorage.Create(ms, veryMinPageSize - 1, 1, null, new CancellationToken(false), true))
                {
                    Assert.IsTrue(storage.TryAllocatePage(out long index));
                    Assert.ThrowsException<ArgumentException>(()=> {
                        StorageDictionary<long, string>.Load(storage, new MockLongSerializer(), new MockStringSerializer(), index, false);
                    });
                }
            }
        }

        [TestMethod]
        public void TryGetValueWorks()
        {
            using (MemoryStream ms = new MemoryStream())
            {
                using (StreamingPageStorage storage = StreamingPageStorage.Create(ms, 1024, 1, null, new CancellationToken(false), true))
                {
                    Assert.IsTrue(StorageDictionary<long, string>.TryCreate(storage, new MockLongSerializer(), new MockStringSerializer(), out long dictionaryPageIndex));

                    using (var dictionary = StorageDictionary<long, string>.Load(storage, new MockLongSerializer(), new MockStringSerializer(), dictionaryPageIndex, false))
                    {
                        for (int i = 0; i < 1024; i++)
                            Assert.IsTrue(dictionary.TryAdd(i, i.ToString()));

                        for (int i = 0; i < 1024; i++)
                        {
                            Assert.IsTrue(dictionary.TryGetValue(i, out var value));
                            Assert.AreEqual(i.ToString(), value);
                        }

                        Assert.IsFalse(dictionary.TryGetValue(-1, out var mustBeNull));
                        Assert.IsNull(mustBeNull);
                        Assert.IsFalse(dictionary.TryGetValue(1024, out mustBeNull));
                        Assert.IsNull(mustBeNull);
                    }
                }
            }
        }

        [TestMethod]
        public void TryAddThrowsWhenReadOnly()
        {
            using (MemoryStream ms = new MemoryStream())
            {
                using (StreamingPageStorage storage = StreamingPageStorage.Create(ms, 1024, 1, null, new CancellationToken(false), true))
                {
                    Assert.IsTrue(StorageDictionary<long, string>.TryCreate(storage, new MockLongSerializer(), new MockStringSerializer(), out long pageIndex));

                    //Load the dictionary as read-only
                    using (var dictionary = StorageDictionary<long, string>.Load(storage, new MockLongSerializer(), new MockStringSerializer(), pageIndex, true))
                    {
                        Assert.ThrowsException<InvalidOperationException>(() =>
                        {
                            dictionary.TryAdd(0, "Value");
                        });

                        Assert.AreEqual(0, dictionary.Count);
                        Assert.IsFalse(dictionary.ContainsKey(0));
                    }
                }
            }
        }

        [TestMethod]
        public void TryAddThrowsWhenKeyAlreadyExists()
        {
            using (MemoryStream ms = new MemoryStream())
            {
                using (StreamingPageStorage storage = StreamingPageStorage.Create(ms, 1024, 1, null, new CancellationToken(false), true))
                {
                    Assert.IsTrue(StorageDictionary<long, string>.TryCreate(storage, new MockLongSerializer(), new MockStringSerializer(), out long pageIndex));
                    using (var dictionary = StorageDictionary<long, string>.Load(storage, new MockLongSerializer(), new MockStringSerializer(), pageIndex, false))
                    {
                        Assert.IsTrue(dictionary.TryAdd(0, "Value"));

                        Assert.ThrowsException<ArgumentException>(()=> {
                            dictionary.TryAdd(0, "Other Value");
                        });

                        //Make sure the original key-value pair remains unchanged
                        Assert.AreEqual(1, dictionary.Count);
                        Assert.IsTrue(dictionary.TryGetValue(0, out var value));
                        Assert.AreEqual("Value", value);
                    }
                }
            }
        }
        
        [TestMethod]
        public void TryAddThrowsWhenCalledFromEnumeration()
        {
            using (MemoryStream ms = new MemoryStream())
            {
                using (StreamingPageStorage storage = StreamingPageStorage.Create(ms, 1024, 1, null, new CancellationToken(false), true))
                {
                    Assert.IsTrue(StorageDictionary<long, string>.TryCreate(storage, new MockLongSerializer(), new MockStringSerializer(), out long index));

                    using (var dictionary = StorageDictionary<long, string>.Load(storage, new MockLongSerializer(), new MockStringSerializer(), index, false))
                    {
                        const int count = 10;
                        for (int i = 0; i < count; i++)
                            Assert.IsTrue(dictionary.TryAdd(i, i.ToString()));

                        long keyValuePairIndex = 0;
                        foreach(var pair in dictionary)
                        {
                            Assert.ThrowsException<InvalidOperationException>(()=> {
                                dictionary.TryAdd(-1, "Value");
                            });

                            Assert.AreEqual(keyValuePairIndex, pair.Key);
                            Assert.AreEqual(keyValuePairIndex.ToString(), pair.Value);

                            keyValuePairIndex++;
                        }

                        //Make sure nothing changed during enumeration
                        Assert.AreEqual(count, dictionary.Count);
                        keyValuePairIndex = 0;
                        foreach (var pair in dictionary)
                        {
                            Assert.AreEqual(keyValuePairIndex, pair.Key);
                            Assert.AreEqual(keyValuePairIndex.ToString(), pair.Value);
                            keyValuePairIndex++;
                        }
                    }
                }
            }
        }

        [DataTestMethod]
        [DataRow(0)]
        [DataRow(1)]
        [DataRow(2)]
        [DataRow(9)]
        [DataRow(10)]
        [DataRow(11)]
        [DataRow(1024)]
        public void TryAddWorks(int cachePageCount)
        {
            using (MemoryStream ms = new MemoryStream())
            {
                using (StreamingPageStorage storage = StreamingPageStorage.Create(ms, 1024, 1, null, new CancellationToken(false), true))
                {
                    Assert.IsTrue(StorageDictionary<long, string>.TryCreate(storage, new MockLongSerializer(), new MockStringSerializer(), out long index));
                    const long count = 10;
                    using (var dictionary = StorageDictionary<long, string>.Load(storage, new MockLongSerializer(), new MockStringSerializer(), index, false, cachePageCount))
                    {
                        for (long i = 0; i < count; i++)
                        {
                            Assert.AreEqual(i, dictionary.Count);
                            Assert.IsTrue(dictionary.TryAdd(i, i.ToString()));
                            Assert.AreEqual(i + 1, dictionary.Count);
                            Assert.IsTrue(dictionary.TryGetValue(i, out var value));
                            Assert.AreEqual(i.ToString(), value);
                        }

                        for(long i = 0; i < count; i++)
                        {
                            Assert.IsTrue(dictionary.TryGetValue(i, out var value));
                            Assert.AreEqual(i.ToString(), value);
                        }
                    }

                    //Re-load it, to very everything reached main storage (after cache)
                    using (var dictionary = StorageDictionary<long, string>.Load(storage, new MockLongSerializer(), new MockStringSerializer(), index, false, cachePageCount))
                    {
                        Assert.AreEqual(count, dictionary.Count);
                        for (long i = 0; i < count; i++)
                        {
                            Assert.IsTrue(dictionary.TryGetValue(i, out var value));
                            Assert.AreEqual(i.ToString(), value);
                        }
                    }
                }
            }
        }
        
        [TestMethod]
        public void TryAddOrUpdateThrowsWhenReadOnly()
        {
            using (MemoryStream ms = new MemoryStream())
            {
                using (StreamingPageStorage storage = StreamingPageStorage.Create(ms, 1024, 1, null, new CancellationToken(false), true))
                {
                    Assert.IsTrue(StorageDictionary<long, string>.TryCreate(storage, new MockLongSerializer(), new MockStringSerializer(), out long pageIndex));
                    //Load the dictionary to install some initial key-value pairs
                    using (var dictionary = StorageDictionary<long, string>.Load(storage, new MockLongSerializer(), new MockStringSerializer(), pageIndex, false))
                    {
                        Assert.IsTrue(dictionary.TryAdd(0, "Initial Value"));
                    }

                    //Load the dictionary as read-only
                    using (var dictionary = StorageDictionary<long, string>.Load(storage, new MockLongSerializer(), new MockStringSerializer(), pageIndex, true))
                    {
                        //Try to update (while read-only)
                        Assert.ThrowsException<InvalidOperationException>(() =>
                        {
                            dictionary.TryAddOrUpdate(0, "Updated Value", out _);
                        });

                        //Try to add new key-value pair (while read-only)
                        Assert.ThrowsException<InvalidOperationException>(() =>
                        {
                            dictionary.TryAddOrUpdate(1, "New Value", out _);
                        });

                        //Make sure the failures did not change anything
                        Assert.AreEqual(1, dictionary.Count);
                        Assert.IsFalse(dictionary.ContainsKey(1));
                        Assert.IsTrue(dictionary.TryGetValue(0, out var originalValue));
                        Assert.AreEqual("Initial Value", originalValue);
                    }
                }
            }
        }
        
        [TestMethod]
        public void TryAddOrUpdateThrowsWhenCalledFromEnumeration()
        {
            using (MemoryStream ms = new MemoryStream())
            {
                using (StreamingPageStorage storage = StreamingPageStorage.Create(ms, 1024, 1, null, new CancellationToken(false), true))
                {
                    Assert.IsTrue(StorageDictionary<long, string>.TryCreate(storage, new MockLongSerializer(), new MockStringSerializer(), out long index));

                    using (var dictionary = StorageDictionary<long, string>.Load(storage, new MockLongSerializer(), new MockStringSerializer(), index, false))
                    {
                        const int count = 10;
                        for (int i = 0; i < count; i++)
                            Assert.IsTrue(dictionary.TryAdd(i, i.ToString()));

                        long keyValuePairIndex = 0;
                        foreach (var pair in dictionary)
                        {
                            Assert.ThrowsException<InvalidOperationException>(() => {
                                dictionary.TryAdd(-(keyValuePairIndex + 1), "New Value");
                            });

                            Assert.ThrowsException<InvalidOperationException>(() => {
                                dictionary.TryAdd(keyValuePairIndex, "Updated Value");
                            });

                            Assert.AreEqual(keyValuePairIndex, pair.Key);
                            Assert.AreEqual(keyValuePairIndex.ToString(), pair.Value);

                            keyValuePairIndex++;
                        }

                        //Make sure nothing changed during enumeration
                        Assert.AreEqual(count, dictionary.Count);
                        keyValuePairIndex = 0;
                        foreach (var pair in dictionary)
                        {
                            Assert.AreEqual(keyValuePairIndex, pair.Key);
                            Assert.AreEqual(keyValuePairIndex.ToString(), pair.Value);
                            keyValuePairIndex++;
                        }
                    }
                }
            }
        }

        [DataTestMethod]
        [DataRow(0)]
        [DataRow(1)]
        [DataRow(2)]
        [DataRow(9)]
        [DataRow(10)]
        [DataRow(11)]
        [DataRow(1024)]
        public void TryAddOrUpdateWorks(int cachePageCount)
        {
            using (MemoryStream ms = new MemoryStream())
            {
                using (StreamingPageStorage storage = StreamingPageStorage.Create(ms, 1024, 1, null, new CancellationToken(false), true))
                {
                    Assert.IsTrue(StorageDictionary<long, string>.TryCreate(storage, new MockLongSerializer(), new MockStringSerializer(), out long index));
                    const long count = 10;

                    //Add initial values
                    using (var dictionary = StorageDictionary<long, string>.Load(storage, new MockLongSerializer(), new MockStringSerializer(), index, false, cachePageCount))
                    {
                        for (long i = 0; i < count; i++)
                        {
                            Assert.AreEqual(i, dictionary.Count);
                            Assert.IsTrue(dictionary.TryAddOrUpdate(i, i.ToString(), out bool alreadyExists));
                            Assert.IsFalse(alreadyExists);
                            Assert.AreEqual(i + 1, dictionary.Count);
                            Assert.IsTrue(dictionary.TryGetValue(i, out var value));
                            Assert.AreEqual(i.ToString(), value);
                        }

                        for (long i = 0; i < count; i++)
                        {
                            Assert.IsTrue(dictionary.TryGetValue(i, out var value));
                            Assert.AreEqual(i.ToString(), value);
                        }
                    }

                    //Re-load it, to very everything reached main storage (after cache)
                    using (var dictionary = StorageDictionary<long, string>.Load(storage, new MockLongSerializer(), new MockStringSerializer(), index, false, cachePageCount))
                    {
                        Assert.AreEqual(count, dictionary.Count);
                        for (long i = 0; i < count; i++)
                        {
                            Assert.IsTrue(dictionary.TryGetValue(i, out var value));
                            Assert.AreEqual(i.ToString(), value);
                        }
                    }

                    //Update values
                    using (var dictionary = StorageDictionary<long, string>.Load(storage, new MockLongSerializer(), new MockStringSerializer(), index, false, cachePageCount))
                    {
                        for (long i = 0; i < count; i++)
                        {
                            Assert.IsTrue(dictionary.TryAddOrUpdate(i, i.ToString()+"_updated", out bool alreadyExists));
                            Assert.IsTrue(alreadyExists);
                            Assert.IsTrue(dictionary.TryGetValue(i, out var value));
                            Assert.AreEqual(i.ToString() + "_updated", value);
                        }

                        for (long i = 0; i < count; i++)
                        {
                            Assert.IsTrue(dictionary.TryGetValue(i, out var value));
                            Assert.AreEqual(i.ToString() + "_updated", value);
                        }
                    }

                    //Re-load it, to very everything reached main storage (after cache)
                    using (var dictionary = StorageDictionary<long, string>.Load(storage, new MockLongSerializer(), new MockStringSerializer(), index, false, cachePageCount))
                    {
                        Assert.AreEqual(count, dictionary.Count);
                        for (long i = 0; i < count; i++)
                        {
                            Assert.IsTrue(dictionary.TryGetValue(i, out var value));
                            Assert.AreEqual(i.ToString() + "_updated", value);
                        }
                    }
                }
            }
        }

        [TestMethod]
        public void UpdateValueThrowsWhenReadOnly()
        {
            using (MemoryStream ms = new MemoryStream())
            {
                using (StreamingPageStorage storage = StreamingPageStorage.Create(ms, 1024, 1, null, new CancellationToken(false), true))
                {
                    Assert.IsTrue(StorageDictionary<long, string>.TryCreate(storage, new MockLongSerializer(), new MockStringSerializer(), out long pageIndex));
                    //Load the dictionary to install some initial key-value pairs
                    using (var dictionary = StorageDictionary<long, string>.Load(storage, new MockLongSerializer(), new MockStringSerializer(), pageIndex, false))
                    {
                        Assert.IsTrue(dictionary.TryAdd(0, "Initial Value"));
                    }

                    //Load the dictionary as read-only
                    using (var dictionary = StorageDictionary<long, string>.Load(storage, new MockLongSerializer(), new MockStringSerializer(), pageIndex, true))
                    {
                        //Try to update (while read-only)
                        Assert.ThrowsException<InvalidOperationException>(() =>
                        {
                            dictionary.UpdateValue(0, "Updated Value");
                        });
                        
                        //Make sure the failures did not change anything
                        Assert.AreEqual(1, dictionary.Count);
                        Assert.IsFalse(dictionary.ContainsKey(1));
                        Assert.IsTrue(dictionary.TryGetValue(0, out var originalValue));
                        Assert.AreEqual("Initial Value", originalValue);
                    }
                }
            }
        }

        [TestMethod]
        public void UpdateValueThrowsWhenCalledFromEnumeration()
        {
            using (MemoryStream ms = new MemoryStream())
            {
                using (StreamingPageStorage storage = StreamingPageStorage.Create(ms, 1024, 1, null, new CancellationToken(false), true))
                {
                    Assert.IsTrue(StorageDictionary<long, string>.TryCreate(storage, new MockLongSerializer(), new MockStringSerializer(), out long index));

                    using (var dictionary = StorageDictionary<long, string>.Load(storage, new MockLongSerializer(), new MockStringSerializer(), index, false))
                    {
                        const int count = 10;
                        for (int i = 0; i < count; i++)
                            Assert.IsTrue(dictionary.TryAdd(i, i.ToString()));

                        long keyValuePairIndex = 0;
                        foreach (var pair in dictionary)
                        {
                            Assert.ThrowsException<InvalidOperationException>(() => {
                                dictionary.UpdateValue(keyValuePairIndex, "Updated Value");
                            });

                            Assert.AreEqual(keyValuePairIndex, pair.Key);
                            Assert.AreEqual(keyValuePairIndex.ToString(), pair.Value);

                            keyValuePairIndex++;
                        }

                        //Make sure nothing changed during enumeration
                        Assert.AreEqual(count, dictionary.Count);
                        keyValuePairIndex = 0;
                        foreach (var pair in dictionary)
                        {
                            Assert.AreEqual(keyValuePairIndex, pair.Key);
                            Assert.AreEqual(keyValuePairIndex.ToString(), pair.Value);
                            keyValuePairIndex++;
                        }
                    }
                }
            }
        }

        [TestMethod]
        public void UpdateValueThrowsWhenKeyDoesNotExist()
        {
            using (MemoryStream ms = new MemoryStream())
            {
                using (StreamingPageStorage storage = StreamingPageStorage.Create(ms, 1024, 1, null, new CancellationToken(false), true))
                {
                    Assert.IsTrue(StorageDictionary<long, string>.TryCreate(storage, new MockLongSerializer(), new MockStringSerializer(), out long index));

                    using (var dictionary = StorageDictionary<long, string>.Load(storage, new MockLongSerializer(), new MockStringSerializer(), index, false))
                    {
                        const int count = 10;
                        for (int i = 0; i < count; i++)
                            Assert.IsTrue(dictionary.TryAdd(i, i.ToString()));

                        //Try to update a key-value pair which does not exist
                        Assert.ThrowsException<KeyNotFoundException>(()=> {
                            dictionary.UpdateValue(-1, "Updated");
                        });

                        //Make sure nothing changed
                        Assert.AreEqual(count, dictionary.Count);
                        long keyValuePairIndex = 0;
                        foreach (var pair in dictionary)
                        {
                            Assert.AreEqual(keyValuePairIndex, pair.Key);
                            Assert.AreEqual(keyValuePairIndex.ToString(), pair.Value);
                            keyValuePairIndex++;
                        }
                    }
                }
            }
        }

        [DataTestMethod]
        [DataRow(0)]
        [DataRow(1)]
        [DataRow(2)]
        [DataRow(9)]
        [DataRow(10)]
        [DataRow(11)]
        [DataRow(1024)]
        public void UpdateValueWorks(int cachePageCount)
        {
            using (MemoryStream ms = new MemoryStream())
            {
                using (StreamingPageStorage storage = StreamingPageStorage.Create(ms, 1024, 1, null, new CancellationToken(false), true))
                {
                    Assert.IsTrue(StorageDictionary<long, string>.TryCreate(storage, new MockLongSerializer(), new MockStringSerializer(), out long index));
                    const long count = 10;

                    //Add initial values
                    using (var dictionary = StorageDictionary<long, string>.Load(storage, new MockLongSerializer(), new MockStringSerializer(), index, false, cachePageCount))
                    {
                        for (long i = 0; i < count; i++)
                            Assert.IsTrue(dictionary.TryAdd(i, i.ToString()));
                    }
                    
                    //Update values
                    using (var dictionary = StorageDictionary<long, string>.Load(storage, new MockLongSerializer(), new MockStringSerializer(), index, false, cachePageCount))
                    {
                        for (long i = 0; i < count; i++)
                        {
                            Assert.IsTrue(dictionary.TryAddOrUpdate(i, i.ToString() + "_updated", out bool alreadyExists));
                            Assert.IsTrue(alreadyExists);
                            Assert.IsTrue(dictionary.TryGetValue(i, out var value));
                            Assert.AreEqual(i.ToString() + "_updated", value);
                        }

                        for (long i = 0; i < count; i++)
                        {
                            Assert.IsTrue(dictionary.TryGetValue(i, out var value));
                            Assert.AreEqual(i.ToString() + "_updated", value);
                        }
                    }

                    //Re-load it, to very everything reached main storage (after cache)
                    using (var dictionary = StorageDictionary<long, string>.Load(storage, new MockLongSerializer(), new MockStringSerializer(), index, false, cachePageCount))
                    {
                        Assert.AreEqual(count, dictionary.Count);
                        for (long i = 0; i < count; i++)
                        {
                            Assert.IsTrue(dictionary.TryGetValue(i, out var value));
                            Assert.AreEqual(i.ToString() + "_updated", value);
                        }
                    }
                }
            }
        }

        [DataTestMethod]
        [DataRow(0)]
        [DataRow(1)]
        [DataRow(2)]
        [DataRow(9)]
        [DataRow(10)]
        [DataRow(11)]
        [DataRow(1024)]
        public void ContainsKeyWorks(int cachePageCount)
        {
            using (MemoryStream ms = new MemoryStream())
            {
                using (StreamingPageStorage storage = StreamingPageStorage.Create(ms, 1024, 1, null, new CancellationToken(false), true))
                {
                    Assert.IsTrue(StorageDictionary<long, string>.TryCreate(storage, new MockLongSerializer(), new MockStringSerializer(), out long index));
                    const long count = 10;

                    //Add values
                    using (var dictionary = StorageDictionary<long, string>.Load(storage, new MockLongSerializer(), new MockStringSerializer(), index, false, cachePageCount))
                    {
                        for (long i = 0; i < count; i++)
                        {
                            Assert.IsFalse(dictionary.ContainsKey(i));
                            Assert.IsTrue(dictionary.TryAdd(i, i.ToString()));
                            Assert.IsTrue(dictionary.ContainsKey(i));
                        }
                    }
                }
            }
        }

        [TestMethod]
        public void RemoveThrowsWhenReadOnly()
        {
            using (MemoryStream ms = new MemoryStream())
            {
                using (StreamingPageStorage storage = StreamingPageStorage.Create(ms, 1024, 1, null, new CancellationToken(false), true))
                {
                    Assert.IsTrue(StorageDictionary<long, string>.TryCreate(storage, new MockLongSerializer(), new MockStringSerializer(), out long index));

                    const int count = 10;
                    //Add values
                    using (var dictionary = StorageDictionary<long, string>.Load(storage, new MockLongSerializer(), new MockStringSerializer(), index, false))
                    {
                        for (int i = 0; i < count; i++)
                            Assert.IsTrue(dictionary.TryAdd(i, i.ToString()));
                    }

                    //Load as read-only
                    using (var dictionary = StorageDictionary<long, string>.Load(storage, new MockLongSerializer(), new MockStringSerializer(), index, true))
                    {
                        Assert.ThrowsException<InvalidOperationException>(()=> {
                            dictionary.Remove(0, out _);
                        });

                        //Make sure nothing changed
                        Assert.AreEqual(count, dictionary.Count);
                        for(int i = 0; i < count; i++)
                        {
                            Assert.IsTrue(dictionary.TryGetValue(i, out var value));
                            Assert.AreEqual(i.ToString(), value);
                        }
                    }
                }
            }
        }

        [TestMethod]
        public void RemoveThrowsWhenCalledFromEnumeration()
        {
            using (MemoryStream ms = new MemoryStream())
            {
                using (StreamingPageStorage storage = StreamingPageStorage.Create(ms, 1024, 1, null, new CancellationToken(false), true))
                {
                    Assert.IsTrue(StorageDictionary<long, string>.TryCreate(storage, new MockLongSerializer(), new MockStringSerializer(), out long index));

                    using (var dictionary = StorageDictionary<long, string>.Load(storage, new MockLongSerializer(), new MockStringSerializer(), index, false))
                    {
                        const int count = 10;
                        for (int i = 0; i < count; i++)
                            Assert.IsTrue(dictionary.TryAdd(i, i.ToString()));

                        long keyValuePairIndex = 0;
                        foreach (var pair in dictionary)
                        {
                            Assert.ThrowsException<InvalidOperationException>(() => {
                                dictionary.Remove(keyValuePairIndex, out _);
                            });

                            Assert.AreEqual(keyValuePairIndex, pair.Key);
                            Assert.AreEqual(keyValuePairIndex.ToString(), pair.Value);

                            keyValuePairIndex++;
                        }

                        //Make sure nothing changed during enumeration
                        Assert.AreEqual(count, dictionary.Count);
                        keyValuePairIndex = 0;
                        foreach (var pair in dictionary)
                        {
                            Assert.AreEqual(keyValuePairIndex, pair.Key);
                            Assert.AreEqual(keyValuePairIndex.ToString(), pair.Value);
                            keyValuePairIndex++;
                        }
                    }
                }
            }
        }

        [DataTestMethod]
        [DataRow(0)]
        [DataRow(1)]
        [DataRow(2)]
        [DataRow(9)]
        [DataRow(10)]
        [DataRow(11)]
        [DataRow(1024)]
        public void RemoveWorks(int cachePageCount)
        {
            using (MemoryStream ms = new MemoryStream())
            {
                using (StreamingPageStorage storage = StreamingPageStorage.Create(ms, 1024, 1, null, new CancellationToken(false), true))
                {
                    Assert.IsTrue(StorageDictionary<long, string>.TryCreate(storage, new MockLongSerializer(), new MockStringSerializer(), out long index));
                    const long count = 10;

                    //Add initial values
                    using (var dictionary = StorageDictionary<long, string>.Load(storage, new MockLongSerializer(), new MockStringSerializer(), index, false, cachePageCount))
                    {
                        for (long i = 0; i < count; i++)
                            Assert.IsTrue(dictionary.TryAdd(i, i.ToString()));
                    }

                    //Remove values
                    using (var dictionary = StorageDictionary<long, string>.Load(storage, new MockLongSerializer(), new MockStringSerializer(), index, false, cachePageCount))
                    {
                        for (long i = 0; i < count; i++)
                        {
                            Assert.IsTrue(dictionary.Remove(i, out var removedValue));
                            Assert.AreEqual(i.ToString(), removedValue);
                            Assert.AreEqual(count - (i + 1), dictionary.Count);
                            Assert.IsFalse(dictionary.ContainsKey(i));
                            Assert.IsFalse(dictionary.TryGetValue(i, out _));
                        }
                    }

                    //Re-load it, to very everything reached main storage (after cache)
                    using (var dictionary = StorageDictionary<long, string>.Load(storage, new MockLongSerializer(), new MockStringSerializer(), index, false, cachePageCount))
                    {
                        Assert.AreEqual(0, dictionary.Count);
                        for(long i = 0; i < count; i++)
                        {
                            Assert.IsFalse(dictionary.ContainsKey(i));
                            Assert.IsFalse(dictionary.TryGetValue(i, out _));
                        }
                    }
                }
            }
        }

        [DataTestMethod]
        [DataRow(0)]
        [DataRow(1)]
        [DataRow(2)]
        [DataRow(9)]
        [DataRow(10)]
        [DataRow(11)]
        [DataRow(1024)]
        public void RemoveReturnsFalseWhenKeyDoesNotExist(int cachePageCount)
        {
            using (MemoryStream ms = new MemoryStream())
            {
                using (StreamingPageStorage storage = StreamingPageStorage.Create(ms, 1024, 1, null, new CancellationToken(false), true))
                {
                    Assert.IsTrue(StorageDictionary<long, string>.TryCreate(storage, new MockLongSerializer(), new MockStringSerializer(), out long index));
                    const long count = 10;
                    
                    using (var dictionary = StorageDictionary<long, string>.Load(storage, new MockLongSerializer(), new MockStringSerializer(), index, false, cachePageCount))
                    {
                        //Add initial values
                        for (long i = 0; i < count; i++)
                            Assert.IsTrue(dictionary.TryAdd(i, i.ToString()));

                        //Try to remove key-value pairs that do not exist
                        for (long i = 0; i < count; i++)
                        {
                            Assert.IsFalse(dictionary.Remove(-(i + 1), out var removedValue));
                            Assert.IsNull(removedValue);
                        }

                        //Make sure initial values are unchanged
                        Assert.AreEqual(count, dictionary.Count);
                        for (long i = 0; i < count; i++)
                        {
                            Assert.IsTrue(dictionary.ContainsKey(i));
                            Assert.IsTrue(dictionary.TryGetValue(i, out var value));
                            Assert.AreEqual(i.ToString(), value);
                        }
                    }

                    //Re-load it to ensure nothing changed
                    using (var dictionary = StorageDictionary<long, string>.Load(storage, new MockLongSerializer(), new MockStringSerializer(), index, false, cachePageCount))
                    {
                        Assert.AreEqual(count, dictionary.Count);
                        for (long i = 0; i < count; i++)
                        {
                            Assert.IsTrue(dictionary.ContainsKey(i));
                            Assert.IsTrue(dictionary.TryGetValue(i, out var value));
                            Assert.AreEqual(i.ToString(), value);
                        }
                    }
                }
            }
        }

        [DataTestMethod]
        [DataRow(0, 0)]
        [DataRow(0, 1)]
        [DataRow(0, 2)]
        [DataRow(0, 100)]
        [DataRow(1, 0)]
        [DataRow(1, 1)]
        [DataRow(1, 2)]
        [DataRow(1, 3)]
        [DataRow(1, 100)]
        [DataRow(2, 0)]
        [DataRow(2, 1)]
        [DataRow(2, 2)]
        [DataRow(2, 3)]
        [DataRow(2, 100)]
        [DataRow(3, 0)]
        [DataRow(3, 1)]
        [DataRow(3, 2)]
        [DataRow(3, 3)]
        [DataRow(3, 4)]
        [DataRow(3, 100)]
        [DataRow(100, 0)]
        [DataRow(100, 1)]
        [DataRow(100, 2)]
        [DataRow(100, 99)]
        [DataRow(100, 100)]
        [DataRow(100, 101)]
        [DataRow(100, 1024)]
        [DataRow(1024, 0)]
        [DataRow(1024, 1023)]
        [DataRow(1024, 1024)]
        [DataRow(1024, 1025)]
        public void EnumerationWorks(int count, int cachePageCount)
        {
            using (MemoryStream ms = new MemoryStream())
            {
                using (StreamingPageStorage storage = StreamingPageStorage.Create(ms, 1024, 1, null, new CancellationToken(false), true))
                {
                    Assert.IsTrue(StorageDictionary<long, string>.TryCreate(storage, new MockLongSerializer(), new MockStringSerializer(), out long index));

                    using (var dictionary = StorageDictionary<long, string>.Load(storage, new MockLongSerializer(), new MockStringSerializer(), index, false))
                    {
                        long[] expectedKeys = new long[count];
                        KeyValuePair<long, string>[] expectedPairs = new KeyValuePair<long, string>[count];
                        for (int i = 0; i < count; i++)
                        {
                            Assert.IsTrue(dictionary.TryAdd(-i, (-i).ToString()));
                            expectedKeys[i] = (-i);
                        }

                        expectedKeys = expectedKeys.OrderBy(x => x).ToArray();
                        for(int i = 0; i < count; i++)
                        {
                            expectedPairs[i] = new KeyValuePair<long, string>(expectedKeys[i], expectedKeys[i].ToString());
                        }

                        CollectionAssert.AreEquivalent(expectedPairs, dictionary.ToArray());
                    }
                }
            }
        }

        [DataTestMethod]
        [DataRow(0)]
        [DataRow(1)]
        [DataRow(2)]
        [DataRow(3)]
        [DataRow(100)]
        public void EverythingWorksWhenPageSizeIsMinimum(int cachePageCount)
        {
            using (MemoryStream ms = new MemoryStream())
            {
                long veryMinPageSize = StorageDictionary<long, string>.GetVeryMinRequiredPageSize(MockLongSerializer.CDataSize, MockStringSerializer.CDataSize);
                using (StreamingPageStorage storage = StreamingPageStorage.Create(ms, veryMinPageSize, 1, null, new CancellationToken(false), true))
                {
                    Assert.IsTrue(StorageDictionary<long, string>.TryCreate(storage, new MockLongSerializer(), new MockStringSerializer(), out long index));

                    const long count = 4096;
                    using (var dictionary = StorageDictionary<long, string>.Load(storage, new MockLongSerializer(), new MockStringSerializer(), index, false, cachePageCount))
                    {
                        for(long i = 0; i < count; i++)
                        {
                            Assert.IsTrue(dictionary.TryAdd(i, i.ToString()+"_original"));
                            Assert.IsTrue(dictionary.TryAddOrUpdate(i, i.ToString() + "_addorupdated", out bool alreadyExists));
                            Assert.IsTrue(alreadyExists);
                            dictionary.UpdateValue(i, i.ToString());
                        }
                    }

                    //Re-load
                    using (var dictionary = StorageDictionary<long, string>.Load(storage, new MockLongSerializer(), new MockStringSerializer(), index, false, cachePageCount))
                    {
                        Assert.AreEqual(count, dictionary.Count);
                        for (long i = 0; i < count; i++)
                        {
                            Assert.IsTrue(dictionary.ContainsKey(i));
                            Assert.IsTrue(dictionary.TryGetValue(i, out var value));
                            Assert.AreEqual(i.ToString(), value);
                            Assert.IsTrue(dictionary.Remove(i, out var removedValue));
                            Assert.AreEqual(i.ToString(), removedValue);
                        }
                        Assert.AreEqual(0, dictionary.Count);
                    }

                    //Re-load again
                    using (var dictionary = StorageDictionary<long, string>.Load(storage, new MockLongSerializer(), new MockStringSerializer(), index, false, cachePageCount))
                    {
                        Assert.AreEqual(0, dictionary.Count);
                        for (long i = 0; i < count; i++)
                        {
                            Assert.IsFalse(dictionary.ContainsKey(i));
                            Assert.IsFalse(dictionary.TryGetValue(i, out var value));
                            Assert.IsNull(value);
                            Assert.IsFalse(dictionary.Remove(i, out var removedValue));
                            Assert.IsNull(removedValue);
                        }
                        Assert.AreEqual(0, dictionary.Count);
                    }
                }
            }
        }

        [DataTestMethod]
        [DataRow(StorageDictionary<long, string>.MinKeyValuePairCountPerNode, 1)]
        [DataRow(StorageDictionary<long, string>.MinKeyValuePairCountPerNode, 2)]
        [DataRow(StorageDictionary<long, string>.MinKeyValuePairCountPerNode, 3)]
        [DataRow(StorageDictionary<long, string>.MinKeyValuePairCountPerNode, 4)]
        [DataRow(StorageDictionary<long, string>.MinKeyValuePairCountPerNode + 2, 1)]
        [DataRow(StorageDictionary<long, string>.MinKeyValuePairCountPerNode + 2, 2)]
        [DataRow(StorageDictionary<long, string>.MinKeyValuePairCountPerNode + 2, 3)]
        [DataRow(StorageDictionary<long, string>.MinKeyValuePairCountPerNode + 2, 9)]
        [DataRow(StorageDictionary<long, string>.MinKeyValuePairCountPerNode + 4, 1)]
        [DataRow(StorageDictionary<long, string>.MinKeyValuePairCountPerNode + 4, 2)]
        [DataRow(StorageDictionary<long, string>.MinKeyValuePairCountPerNode + 4, 3)]
        [DataRow(StorageDictionary<long, string>.MinKeyValuePairCountPerNode + 4, 9)]
        public void EverythingWorksWhenThereIsPageSizePadding(long keyValuePairCountPerNode, long paddingAmount)
        {
            using (MemoryStream ms = new MemoryStream())
            {
                long pageSize = StorageDictionary<long, string>.GetRequiredPageSize(MockLongSerializer.CDataSize, MockStringSerializer.CDataSize, keyValuePairCountPerNode) + paddingAmount;
                using (StreamingPageStorage storage = StreamingPageStorage.Create(ms, pageSize, 1, null, new CancellationToken(false), true))
                {
                    Assert.IsTrue(StorageDictionary<long, string>.TryCreate(storage, new MockLongSerializer(), new MockStringSerializer(), out long index));

                    const long count = 1024;
                    using (var dictionary = StorageDictionary<long, string>.Load(storage, new MockLongSerializer(), new MockStringSerializer(), index, false))
                    {
                        for (long i = 0; i < count; i++)
                        {
                            Assert.IsTrue(dictionary.TryAdd(i, i.ToString() + "_original"));
                            Assert.IsTrue(dictionary.TryAddOrUpdate(i, i.ToString() + "_addorupdated", out bool alreadyExists));
                            Assert.IsTrue(alreadyExists);
                            dictionary.UpdateValue(i, i.ToString());
                        }
                    }

                    //Re-load
                    using (var dictionary = StorageDictionary<long, string>.Load(storage, new MockLongSerializer(), new MockStringSerializer(), index, false))
                    {
                        Assert.AreEqual(count, dictionary.Count);
                        for (long i = 0; i < count; i++)
                        {
                            Assert.IsTrue(dictionary.ContainsKey(i));
                            Assert.IsTrue(dictionary.TryGetValue(i, out var value));
                            Assert.AreEqual(i.ToString(), value);
                            Assert.IsTrue(dictionary.Remove(i, out var removedValue));
                            Assert.AreEqual(i.ToString(), removedValue);
                        }
                        Assert.AreEqual(0, dictionary.Count);
                    }

                    //Re-load again
                    using (var dictionary = StorageDictionary<long, string>.Load(storage, new MockLongSerializer(), new MockStringSerializer(), index, false))
                    {
                        Assert.AreEqual(0, dictionary.Count);
                        for (long i = 0; i < count; i++)
                        {
                            Assert.IsFalse(dictionary.ContainsKey(i));
                            Assert.IsFalse(dictionary.TryGetValue(i, out var value));
                            Assert.IsNull(value);
                            Assert.IsFalse(dictionary.Remove(i, out var removedValue));
                            Assert.IsNull(removedValue);
                        }
                        Assert.AreEqual(0, dictionary.Count);
                    }
                }
            }
        }

        [TestMethod]
        public void ReadAuxDataThrowsWhenBufferIsNull()
        {
            using (MemoryStream ms = new MemoryStream())
            {
                using (StreamingPageStorage storage = StreamingPageStorage.Create(ms, 1024, 1, null, new CancellationToken(false), true))
                {
                    Assert.IsTrue(StorageDictionary<long, string>.TryCreate(storage, new MockLongSerializer(), new MockStringSerializer(), out long index));

                    using (var dictionary = StorageDictionary<long, string>.Load(storage, new MockLongSerializer(), new MockStringSerializer(), index, false))
                    {
                        Assert.ThrowsException<ArgumentNullException>(()=> {
                            dictionary.ReadAuxData(0, null, 0, 1);
                        });
                    }
                }
            }
        }

        [TestMethod]
        public void ReadAuxDataThrowsWhenArgsAreOutOfRange()
        {
            using (MemoryStream ms = new MemoryStream())
            {
                using (StreamingPageStorage storage = StreamingPageStorage.Create(ms, 1024, 1, null, new CancellationToken(false), true))
                {
                    Assert.IsTrue(StorageDictionary<long, string>.TryCreate(storage, new MockLongSerializer(), new MockStringSerializer(), out long index));

                    using (var dictionary = StorageDictionary<long, string>.Load(storage, new MockLongSerializer(), new MockStringSerializer(), index, false))
                    {
                        Assert.ThrowsException<ArgumentOutOfRangeException>(() => {
                            dictionary.ReadAuxData(-1, new byte[100], 0, 1);
                        });

                        Assert.ThrowsException<ArgumentOutOfRangeException>(() => {
                            dictionary.ReadAuxData(0, new byte[100], -1, 1);
                        });

                        Assert.ThrowsException<ArgumentOutOfRangeException>(() => {
                            dictionary.ReadAuxData(0, new byte[100], 0, -1);
                        });

                        Assert.ThrowsException<ArgumentOutOfRangeException>(() => {
                            dictionary.ReadAuxData(dictionary.AuxDataSize, new byte[100], 0, 1);
                        });

                        Assert.ThrowsException<ArgumentOutOfRangeException>(() => {
                            dictionary.ReadAuxData(0, new byte[100], 1, 100);
                        });
                    }
                }
            }
        }

        [DataTestMethod]
        [DataRow(0)]
        [DataRow(1)]
        [DataRow(2)]
        [DataRow(100)]
        public void WriteThenReadAuxDataWorks(int cachePageCount)
        {
            using (MemoryStream ms = new MemoryStream())
            {
                using (StreamingPageStorage storage = StreamingPageStorage.Create(ms, 1024, 1, null, new CancellationToken(false), true))
                {
                    Assert.IsTrue(StorageDictionary<long, string>.TryCreate(storage, new MockLongSerializer(), new MockStringSerializer(), out long index));

                    long length;
                    using (var dictionary = StorageDictionary<long, string>.Load(storage, new MockLongSerializer(), new MockStringSerializer(), index, false, cachePageCount))
                    {
                        //Add some key-value pairs
                        for (long i = 0; i < 2048; i++)
                            Assert.IsTrue(dictionary.TryAdd(i, i.ToString()));

                        length = dictionary.AuxDataSize;
                        long sourceOffset = 7;
                        long sourceEndPadding = 9;
                        byte[] sourceBuffer = new byte[dictionary.AuxDataSize + sourceOffset + sourceEndPadding];

                        for (int i = 0; i < length; i++)
                            sourceBuffer[i + sourceOffset] = (byte)i;

                        dictionary.WriteAuxData(0, sourceBuffer, sourceOffset, length);

                        int dstOffset = 10;
                        int dstEndPadding = 15;
                        byte[] readBuffer = new byte[length + dstOffset + dstEndPadding];
                        dictionary.ReadAuxData(0, readBuffer, dstOffset, length);

                        for (int i = 0; i < length; i++)
                            Assert.AreEqual((byte)i, readBuffer[i + dstOffset]);

                        //Write all bytes with +1 offset
                        for(int i = 0; i < length; i++)
                        {
                            byte[] buffer = new byte[100];
                            buffer[50] = (byte)(i + 1);
                            dictionary.WriteAuxData(i, buffer, 50, 1);
                        }
                    }

                    //Re-load to ensure recent writes were written to base storage
                    using (var dictionary = StorageDictionary<long, string>.Load(storage, new MockLongSerializer(), new MockStringSerializer(), index, false, cachePageCount))
                    {
                        Assert.AreEqual(length, dictionary.AuxDataSize);
                        for (int i = 0; i < length; i++)
                        {
                            byte[] buffer = new byte[100];
                            dictionary.ReadAuxData(i, buffer, 50, 1);
                            Assert.AreEqual((byte)(i + 1), buffer[50]);
                        }

                        //Make sure key-value pairs were not corrupted
                        Assert.AreEqual(2048, dictionary.Count);
                        for(long i = 0; i < 2048; i++)
                        {
                            Assert.IsTrue(dictionary.TryGetValue(i, out var value));
                            Assert.AreEqual(i.ToString(), value);
                        }
                    }
                }
            }
        }

        [TestMethod]
        public void WriteAuxDataThrowsWhenReadOnly()
        {
            using (MemoryStream ms = new MemoryStream())
            {
                using (StreamingPageStorage storage = StreamingPageStorage.Create(ms, 1024, 1, null, new CancellationToken(false), true))
                {
                    Assert.IsTrue(StorageDictionary<long, string>.TryCreate(storage, new MockLongSerializer(), new MockStringSerializer(), out long index));
                    
                    using (var dictionary = StorageDictionary<long, string>.Load(storage, new MockLongSerializer(), new MockStringSerializer(), index, false))
                    {
                        byte[] buffer = new byte[dictionary.AuxDataSize];
                        for(int i = 0; i < buffer.Length; i++)
                            buffer[i] = (byte)i;

                        dictionary.WriteAuxData(0, buffer, 0, buffer.Length);
                    }

                    //Re-load as read-only
                    using (var dictionary = StorageDictionary<long, string>.Load(storage, new MockLongSerializer(), new MockStringSerializer(), index, true))
                    {
                        Assert.ThrowsException<InvalidOperationException>(()=> {
                            dictionary.WriteAuxData(0, new byte[1], 0, 1);
                        });

                        //Make sure nothing was changed
                        byte[] buffer = new byte[dictionary.AuxDataSize];
                        dictionary.ReadAuxData(0, buffer, 0, buffer.Length);
                        for (int i = 0; i < buffer.Length; i++)
                            Assert.AreEqual((byte)i, buffer[i]);
                    }
                }
            }
        }

        [TestMethod]
        public void WriteAuxDataThrowsWhenBufferIsNull()
        {
            using (MemoryStream ms = new MemoryStream())
            {
                using (StreamingPageStorage storage = StreamingPageStorage.Create(ms, 1024, 1, null, new CancellationToken(false), true))
                {
                    Assert.IsTrue(StorageDictionary<long, string>.TryCreate(storage, new MockLongSerializer(), new MockStringSerializer(), out long index));

                    using (var dictionary = StorageDictionary<long, string>.Load(storage, new MockLongSerializer(), new MockStringSerializer(), index, false))
                    {
                        Assert.ThrowsException<ArgumentNullException>(() => {
                            dictionary.WriteAuxData(0, null, 0, 1);
                        });
                    }
                }
            }
        }

        [TestMethod]
        public void WriteAuxDataThrowsWhenArgsAreOutOfRange()
        {
            using (MemoryStream ms = new MemoryStream())
            {
                using (StreamingPageStorage storage = StreamingPageStorage.Create(ms, 1024, 1, null, new CancellationToken(false), true))
                {
                    Assert.IsTrue(StorageDictionary<long, string>.TryCreate(storage, new MockLongSerializer(), new MockStringSerializer(), out long index));

                    using (var dictionary = StorageDictionary<long, string>.Load(storage, new MockLongSerializer(), new MockStringSerializer(), index, false))
                    {
                        Assert.ThrowsException<ArgumentOutOfRangeException>(() => {
                            dictionary.WriteAuxData(-1, new byte[100], 0, 1);
                        });

                        Assert.ThrowsException<ArgumentOutOfRangeException>(() => {
                            dictionary.WriteAuxData(0, new byte[100], -1, 1);
                        });

                        Assert.ThrowsException<ArgumentOutOfRangeException>(() => {
                            dictionary.WriteAuxData(0, new byte[100], 0, -1);
                        });

                        Assert.ThrowsException<ArgumentOutOfRangeException>(() => {
                            dictionary.WriteAuxData(dictionary.AuxDataSize, new byte[100], 0, 1);
                        });

                        Assert.ThrowsException<ArgumentOutOfRangeException>(() => {
                            dictionary.WriteAuxData(0, new byte[100], 1, 100);
                        });
                    }
                }
            }
        }
    }
}
