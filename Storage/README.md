# Storage.Net Library
This library provides various APIs to handle data that can be stored in non-volatile memory. It is rather generalized, so you may have to read through the code, documentation, or examples to get a better understanding of what it does. This readme only provides a brief summary of some of the main features, and additional features may even be added in the future.

#### Development 
This API was primarily developed to be used in some of my personal projects. As I frequently required certain common features (such as a B-Tree) to store data, I decided it would be better to create a dedicated API just for common data storage tasks.

#### License
This API is licensed under the terms of the MIT license, which is detailed in [License.txt](License.txt).

## Main Features

#### IPageStorage Interface
The `IPageStorage` interface is used to store fixed-length blocks (called pages) of data. An application can allocate or deallocate pages arbitrarily in runtime. The length of each block is determined by the implementation, and likely depends on the application's needs. An `IPageStorage` can be stored on any `System.IO.Stream` using the `StreamingPageStorage` class.

#### ISerializable Interface
The `ISerializable` interface (in the `Storage.Data` namespace) is used to serialize and deserialize fixed-size data to and from a byte array. This is distinct from `System.Runtime.Serialization.ISerializable`, which does not necessarily require the data size to be fixed.

#### StorageDictionary
The `StorageDictionary<TKey, TValue>` class is a B-Tree based dictionary that stores key-value pairs. Since the underlying data structure is a B-Tree, the keys must implement the `IComparable<TKey>` interface. The data is stored in an `IPageStorage` (if you want to store a `StorageDictionary` in a `System.IO.Stream`, use the `StreamingPageStorage` class). Keys and values are serialized using the `ISerializable` interface.