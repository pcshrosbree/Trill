using System;
using System.IO;
using System.Text;
using Microsoft.StreamProcessing.Serializer;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace SimpleTesting.Serializer
{
    [TestClass]
    public class PrimitiveRoundTripTests : TestWithConfigSettingsWithoutMemoryLeakDetection
    {
        [TestMethod, TestCategory("Gated")]
        public void StringRoundTrip()
        {
            // One byte, multi-byte UTF-8, and a string longer than any small pooled buffer.
            foreach (var value in new[] { string.Empty, "a", "ab", "héllo ✓ 𝄞", new string('x', 100_000) })
            {
                Assert.AreEqual(value, RoundTrip(value));

                // The decoder may rent the buffer the encoder just returned, which still holds
                // these bytes and would hide a short read. Fill it with other bytes first.
                var decoy = new string('#', Encoding.UTF8.GetByteCount(value));
                Assert.AreEqual(value, RoundTrip(value, oneByteAtATime: true, beforeDeserialize: () => RoundTrip(decoy)));
            }
        }

        [TestMethod, TestCategory("Gated")]
        public void FloatRoundTrip()
        {
            foreach (var value in new[] { 1.5f, -0.25f, float.MaxValue, float.Epsilon })
            {
                Assert.AreEqual(value, RoundTrip(value));
                Assert.AreEqual(value, RoundTrip(value, oneByteAtATime: true));
            }
        }

        [TestMethod, TestCategory("Gated")]
        public void DoubleRoundTrip()
        {
            foreach (var value in new[] { 1.5, -0.25, double.MaxValue, double.Epsilon })
            {
                Assert.AreEqual(value, RoundTrip(value));
                Assert.AreEqual(value, RoundTrip(value, oneByteAtATime: true));
            }
        }

        [TestMethod, TestCategory("Gated")]
        public void GuidRoundTrip()
        {
            var value = new Guid("0f1e2d3c-4b5a-6978-8796-a5b4c3d2e1f0");
            Assert.AreEqual(value, RoundTrip(value));
            Assert.AreEqual(value, RoundTrip(value, oneByteAtATime: true));
        }

        private static T RoundTrip<T>(T value, bool oneByteAtATime = false, Action beforeDeserialize = null)
        {
            var serializer = StreamSerializer.Create<T>();
            using (var stream = new MemoryStream())
            {
                serializer.Serialize(stream, value);
                stream.Seek(0, SeekOrigin.Begin);
                beforeDeserialize?.Invoke();
                return serializer.Deserialize(oneByteAtATime ? new OneByteReadStream(stream) : stream);
            }
        }

        // A stream may return fewer bytes than asked for; this one always returns at most one.
        private sealed class OneByteReadStream : Stream
        {
            private readonly Stream inner;

            public OneByteReadStream(Stream inner) => this.inner = inner;

            public override bool CanRead => true;
            public override bool CanSeek => false;
            public override bool CanWrite => false;
            public override long Length => throw new NotSupportedException();
            public override long Position { get => throw new NotSupportedException(); set => throw new NotSupportedException(); }

            public override int Read(byte[] buffer, int offset, int count) => this.inner.Read(buffer, offset, Math.Min(count, 1));
            public override int Read(Span<byte> buffer) => this.inner.Read(buffer.Slice(0, Math.Min(buffer.Length, 1)));
            public override int ReadByte() => this.inner.ReadByte();

            public override void Flush() { }
            public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
            public override void SetLength(long value) => throw new NotSupportedException();
            public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();
        }
    }
}
