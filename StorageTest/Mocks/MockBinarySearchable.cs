using Storage;
using System;
using System.Collections.Generic;
using System.Linq;

namespace StorageTest.Mocks
{
    class MockBinarySearchable : IBinarySearchable<long, string>
    {
        public SortedList<long, string> SortedList { get; private set; }
        public MockBinarySearchable(long count, int seed, params long[] keysToAvoid)
        {
            if (count < 0)
                throw new ArgumentOutOfRangeException(nameof(count));
            this.Count = count;
            SortedList = new SortedList<long, string>((int)count);
            Random r = new Random(seed/*Fixed seed for consistent tests*/);
            for(long i = 0; i < count; i++)
            {
                long key = r.Next();
                while (keysToAvoid.Contains(key) || SortedList.ContainsKey(key))
                    key++;

                SortedList.Add(key, key.ToString());
            }
        }

        public MockBinarySearchable(IEnumerable<KeyValuePair<long, string>> pairs)
        {
            this.Count = pairs.Count();
            SortedList = new SortedList<long, string>();
            foreach (var pair in pairs)
                SortedList.Add(pair.Key, pair.Value);
        }

        public long Count { get; private set; }

        public long GetKeyAt(long index)
        {
            if (index < 0 || index >= Count)
                throw new ArgumentOutOfRangeException(nameof(index));

            return SortedList.ElementAt((int)index).Key;
        }

        public string GetValueAt(long index)
        {
            if (index < 0 || index >= Count)
                throw new ArgumentOutOfRangeException(nameof(index));

            return SortedList.ElementAt((int)index).Value;
        }
    }
}
