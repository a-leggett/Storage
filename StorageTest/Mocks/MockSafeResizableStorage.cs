using Storage;
using System;
using System.IO;

namespace StorageTest.Mocks
{
    class MockSafeResizableStorage : Stream, ISafeResizable
    {
        private bool canRead_;
        public override bool CanRead { get { return canRead_; } }

        private bool canSeek_;
        public override bool CanSeek { get { return canSeek_; } }

        private bool canWrite_;
        public override bool CanWrite { get { return canWrite_; } }

        private Stream baseStream_;
        public override long Length { get { return baseStream_.Length; } }

        public event Action<MockSafeResizableStorage, long> OnSetPosition;

        public override long Position { get { return baseStream_.Position; } set { OnSetPosition?.Invoke(this, value); baseStream_.Position = value; } }

        public long? MaxSize { get; private set; }

        public override void Flush()
        {
            baseStream_.Flush();
        }

        public event Action<MockSafeResizableStorage, byte[], int, int> OnRead;

        public override int Read(byte[] buffer, int offset, int count)
        {
            if (!CanRead)
                throw new InvalidOperationException("Cannot read from a read-only stream.");
            OnRead?.Invoke(this, buffer, offset, count);
            return baseStream_.Read(buffer, offset, count);
        }

        public event Action<MockSafeResizableStorage, long, SeekOrigin> OnSeek;

        public override long Seek(long offset, SeekOrigin origin)
        {
            if (!CanSeek)
                throw new InvalidOperationException("Cannot seek an unseekable stream.");
            OnSeek?.Invoke(this, offset, origin);
            return baseStream_.Seek(offset, origin);
        }

        public event Action<MockSafeResizableStorage, long> OnSetLength;

        public override void SetLength(long value)
        {
            if (value < 0)
                throw new ArgumentOutOfRangeException(nameof(value));

            OnSetLength?.Invoke(this, value);
            baseStream_.SetLength(value);
        }

        public event Action<MockSafeResizableStorage, byte[], int, int> OnWrite;

        public override void Write(byte[] buffer, int offset, int count)
        {
            if (!CanWrite)
                throw new InvalidOperationException("Cannot write to a read-only stream.");
            OnWrite?.Invoke(this, buffer, offset, count);
            baseStream_.Write(buffer, offset, count);
        }

        public event Action<MockSafeResizableStorage, long> OnTrySetSize;

        public bool ForceTrySetSizeFail { get; set; } = false;

        private long realMax_;

        public bool TrySetSize(long size)
        {
            if (size < 0)
                throw new ArgumentOutOfRangeException(nameof(size));

            OnTrySetSize?.Invoke(this, size);
            if (ForceTrySetSizeFail)
                return false;

            if (size > realMax_)
                return false;

            baseStream_.SetLength(size);
            return true;
        }

        public MockSafeResizableStorage(Stream baseStream, bool canRead, bool canWrite, bool canSeek, long? reportedMax, long realMax)
        {
            this.baseStream_ = baseStream ?? throw new ArgumentNullException(nameof(baseStream));
            this.canRead_ = canRead;
            this.canWrite_ = canWrite;
            this.canSeek_ = canSeek;
            this.realMax_ = realMax;
            this.MaxSize = reportedMax;
            if (this.CanRead && !baseStream.CanRead)
                throw new ArgumentException("The 'can read' argument disagrees with the 'CanRead' property of the base stream.", nameof(canRead));
            if (this.CanWrite && !baseStream.CanWrite)
                throw new ArgumentException("The 'can write' argument disagrees with the 'CanWrite' property of the base stream.", nameof(canWrite));
            if (this.CanSeek && !baseStream.CanSeek)
                throw new ArgumentException("The 'can seek' argument disagrees with the 'CanSeek' property of the base stream.", nameof(canSeek));
            if (realMax < 0)
                throw new ArgumentOutOfRangeException(nameof(realMax));
            if (reportedMax.HasValue && reportedMax.Value < 0)
                throw new ArgumentOutOfRangeException(nameof(reportedMax));
        }

        public event Action<MockSafeResizableStorage, bool> OnDispose;

        protected override void Dispose(bool disposing)
        {
            OnDispose?.Invoke(this, disposing);
            base.Dispose(disposing);
        }
    }
}
