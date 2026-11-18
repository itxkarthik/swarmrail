package tracker

import (
	"context"
	"encoding/binary"
	"errors"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// fakeUDPTracker is a scriptable BEP 15 tracker on a loopback socket.
type fakeUDPTracker struct {
	conn          *net.UDPConn
	peers         []Peer // encoded as 6- or 18-byte entries by address family
	ipv6          bool
	dropFirst     atomic.Int32 // datagrams to ignore before answering
	strayFirst    bool         // send a reply with a wrong transaction ID first
	connectError  string       // answer connect with an error action
	connects      atomic.Int32
	lastKey       atomic.Uint32
	lastAnnounces atomic.Int32
}

// startFakeUDPTracker applies configure before the serving goroutine starts,
// so the fake's settings need no locking.
func startFakeUDPTracker(t *testing.T, network, address string, configure func(*fakeUDPTracker)) *fakeUDPTracker {
	t.Helper()
	addr, err := net.ResolveUDPAddr(network, address)
	if err != nil {
		t.Skipf("%s unavailable: %v", network, err)
	}
	conn, err := net.ListenUDP(network, addr)
	if err != nil {
		t.Skipf("%s unavailable: %v", network, err)
	}
	f := &fakeUDPTracker{conn: conn, ipv6: network == "udp6"}
	if configure != nil {
		configure(f)
	}
	t.Cleanup(func() { conn.Close() })
	go f.serve()
	return f
}

func (f *fakeUDPTracker) hostPort() string { return f.conn.LocalAddr().String() }

func (f *fakeUDPTracker) serve() {
	buf := make([]byte, 2048)
	for {
		n, client, err := f.conn.ReadFromUDP(buf)
		if err != nil {
			return
		}
		if n < 16 || f.dropFirst.Add(-1) >= 0 {
			continue
		}
		action := binary.BigEndian.Uint32(buf[8:12])
		txID := binary.BigEndian.Uint32(buf[12:16])
		if f.strayFirst {
			stray := make([]byte, 16)
			binary.BigEndian.PutUint32(stray[0:4], action)
			binary.BigEndian.PutUint32(stray[4:8], txID+1)
			_, _ = f.conn.WriteToUDP(stray, client)
		}

		var resp []byte
		switch action {
		case ActionConnect:
			f.connects.Add(1)
			if f.connectError != "" {
				resp = make([]byte, 8, 8+len(f.connectError))
				binary.BigEndian.PutUint32(resp[0:4], ActionError)
				binary.BigEndian.PutUint32(resp[4:8], txID)
				resp = append(resp, f.connectError...)
				break
			}
			resp = make([]byte, 16)
			binary.BigEndian.PutUint32(resp[0:4], ActionConnect)
			binary.BigEndian.PutUint32(resp[4:8], txID)
			binary.BigEndian.PutUint64(resp[8:16], 123456789)
		case ActionAnnounce:
			if n < 98 {
				continue
			}
			f.lastAnnounces.Add(1)
			f.lastKey.Store(binary.BigEndian.Uint32(buf[88:92]))
			resp = make([]byte, 20)
			binary.BigEndian.PutUint32(resp[0:4], ActionAnnounce)
			binary.BigEndian.PutUint32(resp[4:8], txID)
			binary.BigEndian.PutUint32(resp[8:12], 1800)
			for _, p := range f.peers {
				ip := p.IP.To4()
				if f.ipv6 {
					ip = p.IP.To16()
				}
				resp = append(resp, ip...)
				resp = binary.BigEndian.AppendUint16(resp, p.Port)
			}
		default:
			continue
		}
		_, _ = f.conn.WriteToUDP(resp, client)
	}
}

func udpAnnounce(t *testing.T, f *fakeUDPTracker, baseTimeout time.Duration) (AnnounceResponse, error) {
	t.Helper()
	addr, err := resolveUDPAddrContext(context.Background(), f.hostPort())
	if err != nil {
		t.Fatal(err)
	}
	tr, err := dialUDPTracker(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer tr.Close()
	tr.baseTimeout = baseTimeout
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return tr.announce(ctx, AnnounceRequest{Port: 6881, Left: 1000})
}

func TestUDPAnnounceParsesIPv4Peers(t *testing.T) {
	f := startFakeUDPTracker(t, "udp4", "127.0.0.1:0", func(g *fakeUDPTracker) {
		g.peers = []Peer{{IP: net.IPv4(127, 0, 0, 1), Port: 6881}, {IP: net.IPv4(10, 0, 0, 2), Port: 6882}}
	})

	resp, err := udpAnnounce(t, f, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if resp.Interval != 30*time.Minute {
		t.Fatalf("interval = %s, want 30m", resp.Interval)
	}
	if len(resp.Peers) != 2 || resp.Peers[1].IP.String() != "10.0.0.2" || resp.Peers[1].Port != 6882 {
		t.Fatalf("peers = %v", resp.Peers)
	}
	if f.lastKey.Load() != sessionKey {
		t.Fatalf("key = %d, want the stable session key %d", f.lastKey.Load(), sessionKey)
	}
}

// BEP 15: peers are 18 bytes when the tracker is reached over IPv6. They were
// previously decoded as three garbage IPv4 peers each.
func TestUDPAnnounceParsesIPv6Peers(t *testing.T) {
	f := startFakeUDPTracker(t, "udp6", "[::1]:0", func(g *fakeUDPTracker) {
		g.peers = []Peer{{IP: net.ParseIP("2001:db8::1"), Port: 51413}}
	})

	resp, err := udpAnnounce(t, f, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.Peers) != 1 || resp.Peers[0].IP.String() != "2001:db8::1" || resp.Peers[0].Port != 51413 {
		t.Fatalf("peers = %v, want [2001:db8::1]:51413", resp.Peers)
	}
}

func TestUDPAnnounceRetransmitsLostPackets(t *testing.T) {
	f := startFakeUDPTracker(t, "udp4", "127.0.0.1:0", func(g *fakeUDPTracker) {
		g.dropFirst.Store(2) // lose the first connect and the first announce
		g.peers = []Peer{{IP: net.IPv4(127, 0, 0, 1), Port: 6881}}
	})

	resp, err := udpAnnounce(t, f, 50*time.Millisecond)
	if err != nil {
		t.Fatalf("announce failed despite retransmission: %v", err)
	}
	if len(resp.Peers) != 1 {
		t.Fatalf("peers = %v", resp.Peers)
	}
}

func TestUDPAnnounceIgnoresStrayDatagrams(t *testing.T) {
	f := startFakeUDPTracker(t, "udp4", "127.0.0.1:0", func(g *fakeUDPTracker) {
		g.strayFirst = true
		g.peers = []Peer{{IP: net.IPv4(127, 0, 0, 1), Port: 6881}}
	})

	if _, err := udpAnnounce(t, f, time.Second); err != nil {
		t.Fatalf("stray datagram broke the exchange: %v", err)
	}
}

func TestUDPConnectReportsTrackerErrorMessage(t *testing.T) {
	f := startFakeUDPTracker(t, "udp4", "127.0.0.1:0", func(g *fakeUDPTracker) {
		g.connectError = "torrent not registered with this tracker\x1b[2J"
	})

	_, err := udpAnnounce(t, f, time.Second)
	if err == nil {
		t.Fatal("expected tracker error")
	}
	if !strings.Contains(err.Error(), "torrent not registered with this tracker") {
		t.Fatalf("error = %q, want the full tracker message", err)
	}
	if strings.Contains(err.Error(), "\x1b") {
		t.Fatalf("error contains a raw control character: %q", err)
	}
}

func TestUDPAnnounceAcceptsLargeReplies(t *testing.T) {
	f := startFakeUDPTracker(t, "udp4", "127.0.0.1:0", func(g *fakeUDPTracker) {
		for i := range 300 { // more than fits in a 1500-byte buffer
			g.peers = append(g.peers, Peer{IP: net.IPv4(10, 0, byte(i>>8), byte(i)), Port: 6881})
		}
	})

	resp, err := udpAnnounce(t, f, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.Peers) != 300 {
		t.Fatalf("got %d peers, want 300", len(resp.Peers))
	}
}

func TestUDPAnnounceGivesUpAfterRetries(t *testing.T) {
	f := startFakeUDPTracker(t, "udp4", "127.0.0.1:0", func(g *fakeUDPTracker) {
		g.dropFirst.Store(1 << 30)
	})

	_, err := udpAnnounce(t, f, 10*time.Millisecond)
	if !errors.Is(err, errUDPTimeout) {
		t.Fatalf("error = %v, want errUDPTimeout", err)
	}
	if got := f.connects.Load(); got != 0 {
		t.Fatalf("tracker answered %d connects, want 0", got)
	}
}

func TestUDPAnnounceViaDiscover(t *testing.T) {
	f := startFakeUDPTracker(t, "udp4", "127.0.0.1:0", func(g *fakeUDPTracker) {
		g.peers = []Peer{{IP: net.IPv4(127, 0, 0, 1), Port: 6881}}
	})

	result, err := Discover(context.Background(), []string{"udp://" + f.hostPort() + "/announce"}, AnnounceRequest{Port: 6881})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Peers) != 1 {
		t.Fatalf("peers = %v", result.Peers)
	}
}
