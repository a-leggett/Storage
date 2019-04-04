using System;
using System.IO;

namespace StorageTest.Mocks
{
    class MockStream : Stream
    {
        private bool canRead_;
        public override bool CanRead { get { return canRead_; } }

        private bool canSeek_;
        public override bool CanSeek { get { return canSeek_; } }

        private bool canWrite_;
        public override bool CanWrite { get { return canWrite_; } }

        private Stream baseStream_;
        public override long Length { get { return baseStream_.Length; } }

        public event Action<MockStream, long> OnSetPosition;

        public override long Position { get { return baseStream_.Position; } set { OnSetPosition?.Invoke(this, value); baseStream_.Position = value; } }

        public override void Flush()
        {
            baseStream_.Flush();
        }

        public event Action<MockStream, byte[], int, int> OnRead;

        public override int Read(byte[] buffer, int offset, int count)
        {
            if (!CanRead)
                throw new InvalidOperationException("Cannot read from a read-only stream.");
            OnRead?.Invoke(this, buffer, offset, count);
            return baseStream_.Read(buffer, offset, count);
        }

        public event Action<MockStream, long, SeekOrigin> OnSeek;

        public override long Seek(long offset, SeekOrigin origin)
        {
            if (!CanSeek)
                throw new InvalidOperationException("Cannot seek an unseekable stream.");
            OnSeek?.Invoke(this, offset, origin);
            return baseStream_.Seek(offset, origin);
        }

        public event Action<MockStream, long> OnSetLength;

        public override void SetLength(long value)
        {
            OnSetLength?.Invoke(this, value);

            long originalLength = Length;
            long additionalAmount = value - Length;
            baseStream_.SetLength(value);
            if (additionalAmount > 0)
            {
                byte[] initPayload = new byte[additionalAmount];
                for(int i = 0; i < initPayload.Length; i++)
                    initPayload[i] = (byte)(i+(value / 2));

                long originalPos = baseStream_.Position;
                baseStream_.Position = originalLength;
                baseStream_.Write(initPayload, 0, initPayload.Length);
                baseStream_.Position = originalPos;
            }
        }

        public event Action<MockStream, byte[], int, int> OnWrite;

        public override void Write(byte[] buffer, int offset, int count)
        {
            if (!CanWrite)
                throw new InvalidOperationException("Cannot write to a read-only stream.");
            OnWrite?.Invoke(this, buffer, offset, count);
            baseStream_.Write(buffer, offset, count);
        }

        public event Action<MockStream> OnDispose;

        protected override void Dispose(bool disposing)
        {
            OnDispose?.Invoke(this);
            base.Dispose(disposing);
        }

        public MockStream(Stream baseStream, bool canRead, bool canWrite, bool canSeek)
        {
            this.baseStream_ = baseStream ?? throw new ArgumentNullException(nameof(baseStream));
            this.canRead_ = canRead;
            this.canWrite_ = canWrite;
            this.canSeek_ = canSeek;
            if (this.CanRead && !baseStream.CanRead)
                throw new ArgumentException("The 'can read' argument disagrees with the 'CanRead' property of the base stream.", nameof(canRead));
            if (this.CanWrite && !baseStream.CanWrite)
                throw new ArgumentException("The 'can write' argument disagrees with the 'CanWrite' property of the base stream.", nameof(canWrite));
            if (this.CanSeek && !baseStream.CanSeek)
                throw new ArgumentException("The 'can seek' argument disagrees with the 'CanSeek' property of the base stream.", nameof(canSeek));
        }
    }
}
