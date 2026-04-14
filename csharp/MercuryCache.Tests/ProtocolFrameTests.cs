using System;
using System.Buffers.Binary;
using System.Text;
using Xunit;

namespace MercuryCache.Tests
{
    /// <summary>
    /// Validates the Mercury binary wire protocol frame layout used by
    /// TcpCacheNodeClient (C#) and TcpCacheServer (C++).
    ///
    /// Frame layout (big-endian):
    ///   [0-1]  magic   = 0x4D43 ('MC')
    ///   [2]    version = 1
    ///   [3]    opcode
    ///   [4]    flags
    ///   [5-8]  seq_id  (uint32)
    ///   [9-12] body_len (uint32)
    ///   [13]   ns_len
    ///   [14]   key_len
    ///   [15]   pad
    ///   [16..] body = ns_bytes + key_bytes [+ ttl(8) + ver(8)] + payload_bytes
    /// </summary>
    public class ProtocolFrameTests
    {
        private const ushort Magic   = 0x4D43;
        private const byte   Version = 1;
        private const int    HeaderSize = 16;

        // Mirror the private BuildFrame logic so we can unit-test the layout
        private static byte[] BuildFrame(
            byte opcode, byte flags, uint seqId,
            string ns, string key, string payload,
            long ttlMs = 0, long version = 0)
        {
            var nsBytes  = Encoding.UTF8.GetBytes(ns);
            var keyBytes = Encoding.UTF8.GetBytes(key);
            var payBytes = Encoding.UTF8.GetBytes(payload);

            bool hasTtl  = (flags & 0x08) != 0;
            int  bodyLen = nsBytes.Length + keyBytes.Length + payBytes.Length
                         + (hasTtl ? 16 : 0);
            var  buf     = new byte[HeaderSize + bodyLen];

            BinaryPrimitives.WriteUInt16BigEndian(buf.AsSpan(0),  Magic);
            buf[2] = Version;
            buf[3] = opcode;
            buf[4] = flags;
            BinaryPrimitives.WriteUInt32BigEndian(buf.AsSpan(5),  seqId);
            BinaryPrimitives.WriteUInt32BigEndian(buf.AsSpan(9),  (uint)bodyLen);
            buf[13] = (byte)nsBytes.Length;
            buf[14] = (byte)keyBytes.Length;
            buf[15] = 0;

            int pos = HeaderSize;
            nsBytes.CopyTo(buf, pos);  pos += nsBytes.Length;
            keyBytes.CopyTo(buf, pos); pos += keyBytes.Length;
            if (hasTtl)
            {
                BinaryPrimitives.WriteInt64BigEndian(buf.AsSpan(pos), ttlMs);   pos += 8;
                BinaryPrimitives.WriteInt64BigEndian(buf.AsSpan(pos), version); pos += 8;
            }
            payBytes.CopyTo(buf, pos);
            return buf;
        }

        [Fact]
        public void Frame_HasCorrectMagic()
        {
            var frame = BuildFrame(0x01, 0, 1, "ns", "key", "");
            ushort magic = BinaryPrimitives.ReadUInt16BigEndian(frame.AsSpan(0));
            Assert.Equal(Magic, magic);
        }

        [Fact]
        public void Frame_HasCorrectVersion()
        {
            var frame = BuildFrame(0x01, 0, 1, "ns", "key", "");
            Assert.Equal(Version, frame[2]);
        }

        [Fact]
        public void Frame_OpcodeAtByte3()
        {
            var frame = BuildFrame(0x03 /*PUT*/, 0, 42, "ns", "k", "v");
            Assert.Equal(0x03, frame[3]);
        }

        [Fact]
        public void Frame_SeqId_BigEndian()
        {
            uint seqId = 0xDEADBEEF;
            var frame = BuildFrame(0x01, 0, seqId, "n", "k", "");
            uint parsed = BinaryPrimitives.ReadUInt32BigEndian(frame.AsSpan(5));
            Assert.Equal(seqId, parsed);
        }

        [Fact]
        public void Frame_BodyLen_MatchesActualBody()
        {
            string ns  = "orders";
            string key = "order-42";
            string val = "hello";
            var frame = BuildFrame(0x03, 0, 1, ns, key, val);

            uint bodyLen = BinaryPrimitives.ReadUInt32BigEndian(frame.AsSpan(9));
            int  expected = Encoding.UTF8.GetByteCount(ns)
                          + Encoding.UTF8.GetByteCount(key)
                          + Encoding.UTF8.GetByteCount(val);

            Assert.Equal((uint)expected, bodyLen);
            Assert.Equal(HeaderSize + (int)bodyLen, frame.Length);
        }

        [Fact]
        public void Frame_NsLen_And_KeyLen_AtBytes13_14()
        {
            var frame = BuildFrame(0x01, 0, 1, "ns", "key-abc", "");
            Assert.Equal(2, frame[13]); // "ns" = 2 bytes
            Assert.Equal(7, frame[14]); // "key-abc" = 7 bytes
        }

        [Fact]
        public void Frame_WithTtlFlag_IncludesTtlAndVersionInBody()
        {
            long ttl = 300_000L;
            long ver = 7L;
            var frame = BuildFrame(0x03, 0x08 /*TtlSet*/, 1, "a", "b", "payload", ttl, ver);

            int nsLen  = frame[13]; // 1
            int keyLen = frame[14]; // 1
            int ttlOffset = HeaderSize + nsLen + keyLen;

            long parsedTtl = BinaryPrimitives.ReadInt64BigEndian(frame.AsSpan(ttlOffset));
            long parsedVer = BinaryPrimitives.ReadInt64BigEndian(frame.AsSpan(ttlOffset + 8));

            Assert.Equal(ttl, parsedTtl);
            Assert.Equal(ver, parsedVer);
        }

        [Fact]
        public void Frame_PayloadEncoded_AsUtf8_AtCorrectOffset()
        {
            string payload = "cached-value";
            var frame = BuildFrame(0x02 /*GetResp*/, 0, 1, "n", "k", payload);

            int nsLen     = frame[13];
            int keyLen    = frame[14];
            int bodyStart = HeaderSize;
            int payStart  = bodyStart + nsLen + keyLen;
            int payLen    = frame.Length - payStart;

            string decoded = Encoding.UTF8.GetString(frame, payStart, payLen);
            Assert.Equal(payload, decoded);
        }

        [Fact]
        public void Frame_PingHasEmptyBody()
        {
            var frame = BuildFrame(0x07 /*Ping*/, 0, 1, "", "", "");
            uint bodyLen = BinaryPrimitives.ReadUInt32BigEndian(frame.AsSpan(9));
            Assert.Equal(0u, bodyLen);
            Assert.Equal(HeaderSize, frame.Length);
        }

        [Fact]
        public void Frame_NotFoundFlag_IsBit1()
        {
            byte flags = 0x02; // NotFound
            var frame = BuildFrame(0x02, flags, 1, "", "", "");
            Assert.Equal(flags, frame[4]);
        }
    }
}
