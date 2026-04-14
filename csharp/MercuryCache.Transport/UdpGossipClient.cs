using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;

namespace MercuryCache.Transport
{
    /// <summary>
    /// UDP Gossip Client — C# side of the SWIM cluster membership protocol.
    ///
    /// This class lets the C# service layer participate in the same gossip
    /// ring as the C++ cache nodes, receiving membership change events without
    /// polling a centralised registry.
    ///
    /// SWIM protocol recap:
    ///   1. Every T_probe ms: pick a random member, send PING over UDP.
    ///   2. If no ACK in T_ack ms: ask K peers to PING on our behalf.
    ///   3. If still no ACK: mark SUSPECT → DEAD after T_dead ms.
    ///   4. All membership changes piggybacked on every outgoing message
    ///      (infection-style dissemination, O(log N) convergence).
    ///
    /// WHY UDP FOR THIS:
    ///   - No connection setup per probe (vs TCP 3-way handshake = ~0.3ms)
    ///   - Fire-and-forget — we retry at the protocol level, not TCP level
    ///   - Membership packets are &lt;512 bytes, well under MTU
    ///   - Consistent with how Consul, Cassandra, and Kubernetes handle gossip
    /// </summary>
    public sealed class UdpGossipClient : IDisposable
    {
        // ── Wire constants (must match GossipProtocol.h) ──────────────────────
        private const ushort GossipMagic   = 0x5357; // 'SW'
        private const byte   GossipVersion = 1;
        private const int    GossipPort    = 7946;

        private enum MsgType : byte
        {
            Ping    = 0x01, Ack    = 0x02, PingReq = 0x03,
            Nack    = 0x04, Join   = 0x05, Suspect = 0x06,
            Dead    = 0x07, Alive  = 0x08, Sync    = 0x09,
        }

        public enum MemberStatus { Alive, Suspect, Dead }

        public sealed class Member
        {
            public string       Address     { get; set; } = "";
            public MemberStatus Status      { get; set; } = MemberStatus.Alive;
            public uint         Incarnation { get; set; }
            public DateTime     LastSeen    { get; set; } = DateTime.UtcNow;
        }

        // ── Events ────────────────────────────────────────────────────────────
        public event Action<Member>? MemberJoined;
        public event Action<Member>? MemberFailed;
        public event Action<Member>? MemberRecovered;

        // ── State ─────────────────────────────────────────────────────────────
        private readonly string _myAddress;
        private readonly int    _gossipPort;
        private readonly ILogger<UdpGossipClient> _log;

        private readonly ConcurrentDictionary<string, Member> _members = new();
        private readonly ConcurrentDictionary<string, TaskCompletionSource<bool>>
                         _pendingAcks = new();

        private UdpClient?      _udp;
        private CancellationTokenSource? _cts;
        private Task?           _receiveTask;
        private Task?           _probeTask;
        private readonly Random _rng = new();

        public UdpGossipClient(string myAddress,
                                int gossipPort = GossipPort,
                                ILogger<UdpGossipClient>? log = null)
        {
            _myAddress  = myAddress;
            _gossipPort = gossipPort;
            _log        = log ?? Microsoft.Extensions.Logging.Abstractions
                                          .NullLogger<UdpGossipClient>.Instance;

            _members[myAddress] = new Member
            {
                Address = myAddress, Status = MemberStatus.Alive
            };
        }

        // ── Lifecycle ─────────────────────────────────────────────────────────
        public void Start()
        {
            _udp = new UdpClient(_gossipPort);
            _udp.Client.SetSocketOption(SocketOptionLevel.Socket,
                                         SocketOptionName.ReuseAddress, true);
            _cts = new CancellationTokenSource();
            _receiveTask = Task.Run(() => ReceiveLoopAsync(_cts.Token));
            _probeTask   = Task.Run(() => ProbeLoopAsync(_cts.Token));
            _log.LogInformation("[Gossip:{Addr}] UDP gossip started on :{Port}",
                _myAddress, _gossipPort);
        }

        /// <summary>Join the cluster via a known seed node.</summary>
        public void Join(string seedAddress)
        {
            var msg = BuildMessage(MsgType.Join, _myAddress, "",
                                   SerializeMembership());
            SendTo(seedAddress, msg);
            _log.LogInformation("[Gossip:{Addr}] Sent JOIN to {Seed}",
                _myAddress, seedAddress);
        }

        public IReadOnlyList<Member> LiveMembers =>
            _members.Values
                    .Where(m => m.Status == MemberStatus.Alive
                                && m.Address != _myAddress)
                    .ToList();

        public int MemberCount => _members.Count;

        public string DumpState()
        {
            var sb = new System.Text.StringBuilder();
            sb.AppendLine($"Gossip state [{_myAddress}]:");
            foreach (var m in _members.Values)
                sb.AppendLine($"  {m.Status,-8} {m.Address}  inc={m.Incarnation}");
            return sb.ToString();
        }

        // ── Receive loop ──────────────────────────────────────────────────────
        private async Task ReceiveLoopAsync(CancellationToken ct)
        {
            while (!ct.IsCancellationRequested)
            {
                try
                {
                    var result = await _udp!.ReceiveAsync(ct);
                    var (type, sender, target, payload) =
                        DecodeMessage(result.Buffer);

                    HandleMessage(type, sender, target, payload);
                }
                catch (OperationCanceledException) { break; }
                catch (Exception ex)
                {
                    _log.LogDebug(ex, "[Gossip] receive error");
                }
            }
        }

        private void HandleMessage(MsgType type, string sender,
                                    string target, string payload)
        {
            switch (type)
            {
                case MsgType.Ping:
                    UpdateMember(sender, MemberStatus.Alive);
                    MergePayload(payload);
                    var ack = BuildMessage(MsgType.Ack, _myAddress, "",
                                           BuildPiggyback());
                    SendTo(sender, ack);
                    break;

                case MsgType.Ack:
                    UpdateMember(sender, MemberStatus.Alive);
                    MergePayload(payload);
                    if (_pendingAcks.TryGetValue(sender, out var tcs))
                        tcs.TrySetResult(true);
                    break;

                case MsgType.PingReq:
                    // Probe target and forward result back
                    _ = Task.Run(async () => {
                        var probe = BuildMessage(MsgType.Ping, _myAddress, "", "");
                        SendTo(target, probe);
                        await Task.Delay(300);
                        var fwd = BuildMessage(MsgType.Ack, target, "", "");
                        SendTo(sender, fwd);
                    });
                    break;

                case MsgType.Join:
                    UpdateMember(sender, MemberStatus.Alive);
                    MergePayload(payload);
                    var sync = BuildMessage(MsgType.Sync, _myAddress, "",
                                            SerializeMembership());
                    SendTo(sender, sync);
                    break;

                case MsgType.Sync:
                    MergePayload(payload);
                    UpdateMember(sender, MemberStatus.Alive);
                    break;

                case MsgType.Suspect:
                    if (target == _myAddress) RefuteSuspicion();
                    else MarkSuspect(target);
                    break;

                case MsgType.Dead:
                    if (target != _myAddress) MarkDead(target);
                    break;

                case MsgType.Alive:
                    UpdateMember(target, MemberStatus.Alive);
                    break;
            }
        }

        // ── Probe loop ────────────────────────────────────────────────────────
        private async Task ProbeLoopAsync(CancellationToken ct)
        {
            while (!ct.IsCancellationRequested)
            {
                try
                {
                    await Task.Delay(1000, ct); // T_probe = 1s

                    string? target = PickRandom();
                    if (target == null) continue;

                    // 1. Direct PING
                    var tcs = new TaskCompletionSource<bool>(
                        TaskCreationOptions.RunContinuationsAsynchronously);
                    _pendingAcks[target] = tcs;

                    var ping = BuildMessage(MsgType.Ping, _myAddress, "",
                                            BuildPiggyback());
                    SendTo(target, ping);

                    bool acked = await Task.WhenAny(tcs.Task,
                                     Task.Delay(300, ct)) == tcs.Task
                                 && await tcs.Task;

                    if (!acked)
                    {
                        // 2. Indirect PING-REQ via 3 peers
                        var peers = PickRandom(3, exclude: target);
                        var indirectTcs = new TaskCompletionSource<bool>(
                            TaskCreationOptions.RunContinuationsAsynchronously);
                        _pendingAcks[target] = indirectTcs;

                        foreach (var peer in peers)
                        {
                            var req = BuildMessage(MsgType.PingReq,
                                                   _myAddress, target, "");
                            SendTo(peer, req);
                        }

                        bool indirectAcked = await Task.WhenAny(
                            indirectTcs.Task, Task.Delay(600, ct))
                            == indirectTcs.Task && await indirectTcs.Task;

                        if (!indirectAcked)
                        {
                            MarkSuspect(target);
                            _log.LogWarning("[Gossip] {Target} is SUSPECT", target);

                            // After one more probe cycle without ack → DEAD
                            await Task.Delay(1000, ct);
                            _pendingAcks.TryGetValue(target, out var deathTcs);
                            bool resolved = deathTcs?.Task.IsCompleted == true;
                            if (!resolved)
                            {
                                MarkDead(target);
                                _log.LogWarning("[Gossip] {Target} is DEAD", target);
                                // Spread the news
                                var deadMsg = BuildMessage(MsgType.Dead,
                                    _myAddress, target, "");
                                foreach (var peer in PickRandom(3, exclude: target))
                                    SendTo(peer, deadMsg);
                            }
                        }
                    }

                    _pendingAcks.TryRemove(target, out _);
                }
                catch (OperationCanceledException) { break; }
                catch (Exception ex)
                {
                    _log.LogDebug(ex, "[Gossip] probe error");
                }
            }
        }

        // ── Member management ─────────────────────────────────────────────────
        private void UpdateMember(string address, MemberStatus status)
        {
            _members.AddOrUpdate(address,
                new Member { Address = address, Status = status, LastSeen = DateTime.UtcNow },
                (_, existing) =>
                {
                    bool changed = existing.Status != status;
                    existing.Status  = status;
                    existing.LastSeen = DateTime.UtcNow;
                    if (changed)
                    {
                        if (status == MemberStatus.Alive)   MemberRecovered?.Invoke(existing);
                        else if (status == MemberStatus.Dead) MemberFailed?.Invoke(existing);
                    }
                    return existing;
                });
        }

        private void MarkSuspect(string addr) => UpdateMember(addr, MemberStatus.Suspect);
        private void MarkDead(string addr)    => UpdateMember(addr, MemberStatus.Dead);

        private void RefuteSuspicion()
        {
            if (_members.TryGetValue(_myAddress, out var m))
            {
                m.Incarnation++;
                // Broadcast ALIVE to override the suspect rumour
                var alive = BuildMessage(MsgType.Alive, _myAddress, _myAddress, "");
                foreach (var peer in PickRandom(3))
                    SendTo(peer, alive);
            }
        }

        // ── Serialisation ─────────────────────────────────────────────────────
        private string SerializeMembership()
        {
            var sb = new StringBuilder();
            foreach (var m in _members.Values)
                sb.Append($"{m.Address}|{(int)m.Status}|{m.Incarnation};");
            return sb.ToString();
        }

        private string BuildPiggyback()
        {
            // Limit to 5 updates to stay under MTU
            var sb = new StringBuilder();
            int count = 0;
            foreach (var m in _members.Values)
            {
                if (++count > 5) break;
                sb.Append($"{m.Address}|{(int)m.Status}|{m.Incarnation};");
            }
            return sb.ToString();
        }

        private void MergePayload(string payload)
        {
            if (string.IsNullOrEmpty(payload)) return;
            foreach (var token in payload.Split(';',
                StringSplitOptions.RemoveEmptyEntries))
            {
                var parts = token.Split('|');
                if (parts.Length < 3) continue;
                string addr   = parts[0];
                var    status = (MemberStatus)int.Parse(parts[1]);
                uint   inc    = uint.Parse(parts[2]);
                _members.AddOrUpdate(addr,
                    new Member { Address = addr, Status = status,
                                 Incarnation = inc, LastSeen = DateTime.UtcNow },
                    (_, existing) => {
                        if (inc >= existing.Incarnation)
                        {
                            existing.Status      = status;
                            existing.Incarnation = inc;
                            existing.LastSeen    = DateTime.UtcNow;
                        }
                        return existing;
                    });
            }
        }

        // ── Wire encode/decode ────────────────────────────────────────────────
        private static byte[] BuildMessage(MsgType type, string sender,
                                            string target, string payload)
        {
            var buf = new System.IO.MemoryStream();
            // magic(2) + version(1) + type(1) + incarnation(4) +
            // senderLen(1) + sender + targetLen(1) + target + payloadLen(2) + payload
            buf.Write(ToNetworkBytes((ushort)GossipMagic));
            buf.WriteByte(GossipVersion);
            buf.WriteByte((byte)type);
            buf.Write(ToNetworkBytes(0u)); // incarnation
            WriteStringField(buf, sender);
            WriteStringField(buf, target);
            var payBytes = Encoding.UTF8.GetBytes(payload);
            buf.Write(ToNetworkBytes((ushort)Math.Min(payBytes.Length, 65535)));
            buf.Write(payBytes, 0, Math.Min(payBytes.Length, 65535));
            return buf.ToArray();
        }

        private static (MsgType, string, string, string) DecodeMessage(byte[] data)
        {
            int pos = 0;
            ushort magic = ReadU16(data, ref pos);
            if (magic != GossipMagic)
                return (MsgType.Ping, "", "", "");
            pos++; // version
            var type = (MsgType)data[pos++];
            pos += 4; // incarnation
            string sender  = ReadStringField(data, ref pos);
            string target  = ReadStringField(data, ref pos);
            ushort plen    = ReadU16(data, ref pos);
            string payload = pos + plen <= data.Length
                ? Encoding.UTF8.GetString(data, pos, plen) : "";
            return (type, sender, target, payload);
        }

        private static byte[] ToNetworkBytes(ushort v) =>
            new[] { (byte)(v >> 8), (byte)(v & 0xFF) };
        private static byte[] ToNetworkBytes(uint v) =>
            new[] { (byte)(v>>24),(byte)(v>>16),(byte)(v>>8),(byte)(v&0xFF) };
        private static void WriteStringField(System.IO.Stream s, string v)
        {
            var b = Encoding.UTF8.GetBytes(v);
            s.WriteByte((byte)Math.Min(b.Length, 255));
            s.Write(b, 0, Math.Min(b.Length, 255));
        }
        private static string ReadStringField(byte[] data, ref int pos)
        {
            if (pos >= data.Length) return "";
            int len = data[pos++];
            if (pos + len > data.Length) return "";
            var s = Encoding.UTF8.GetString(data, pos, len);
            pos += len; return s;
        }
        private static ushort ReadU16(byte[] data, ref int pos)
        {
            ushort v = (ushort)((data[pos] << 8) | data[pos+1]);
            pos += 2; return v;
        }

        // ── Routing helpers ───────────────────────────────────────────────────
        private void SendTo(string address, byte[] data)
        {
            var colon = address.LastIndexOf(':');
            if (colon < 0) return;
            string host = address[..colon];
            int    port = int.Parse(address[(colon+1)..]);
            try { _udp?.Send(data, data.Length, host, port); }
            catch { /* best-effort */ }
        }

        private string? PickRandom(string? exclude = null)
        {
            var candidates = _members.Values
                .Where(m => m.Status == MemberStatus.Alive
                            && m.Address != _myAddress
                            && m.Address != exclude)
                .Select(m => m.Address)
                .ToList();
            return candidates.Count == 0 ? null
                : candidates[_rng.Next(candidates.Count)];
        }

        private List<string> PickRandom(int k, string? exclude = null)
        {
            var candidates = _members.Values
                .Where(m => m.Status == MemberStatus.Alive
                            && m.Address != _myAddress
                            && m.Address != exclude)
                .Select(m => m.Address)
                .OrderBy(_ => _rng.Next())
                .Take(k)
                .ToList();
            return candidates;
        }

        public void Dispose()
        {
            _cts?.Cancel();
            _udp?.Dispose();
            _cts?.Dispose();
        }
    }
}
