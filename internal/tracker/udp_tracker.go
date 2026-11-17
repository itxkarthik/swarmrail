package tracker

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math/rand/v2"
	"net"
	"strconv"
	"strings"
	"time"
)

// BEP 15 actions.
const (
	ActionConnect  = 0
	ActionAnnounce = 1
	ActionScrape   = 2
	ActionError    = 3
)

// MagicConnectionID is the protocol_id sent in every BEP 15 connect request.
const MagicConnectionID uint64 = 0x41727101980

const (
	// udpBaseTimeout is the first retransmission timeout; each retry doubles
	// it. BEP 15 suggests 15 s, but announces here run inside a startup
	// budget of a few seconds, so a shorter first timeout is the only way a
	// lost packet can be retried at all. The caller's context still bounds
	// the whole exchange.
	udpBaseTimeout = 2 * time.Second
	// udpMaxAttempts caps transmissions of one request (BEP 15 allows up to
	// nine). With doubling this spans 2+4+8+16 = 30 s.
	udpMaxAttempts = 4
	// udpConnectionLifetime is how long a connection ID may be reused.
	// BEP 15 allows one minute; renew a little early.
	udpConnectionLifetime = 55 * time.Second
	// udpMaxDatagram is large enough for any UDP payload, so replies with
	// many peers are never truncated.
	udpMaxDatagram = 64 << 10
	// maxTrackerMessage bounds tracker-supplied text shown to the user.
	maxTrackerMessage = 256
)

var (
	errUDPTimeout   = errors.New("udp tracker did not respond")
	errUDPMalformed = errors.New("malformed udp tracker response")
)

// sessionKey identifies this client to trackers for the whole process, so a
// tracker can recognise us after an IP address change (BEP 15 "key").
var sessionKey = rand.Uint32()

// udpTracker speaks BEP 15 to one tracker over a connected UDP socket, which
// makes the kernel discard datagrams from any other address.
type udpTracker struct {
	conn         *net.UDPConn
	ipv6         bool // peers are 18 bytes when the tracker is reached over IPv6
	connectionID uint64
	connectedAt  time.Time
	baseTimeout  time.Duration
}

func dialUDPTracker(addr *net.UDPAddr) (*udpTracker, error) {
	conn, err := net.DialUDP("udp", nil, addr)
	if err != nil {
		return nil, err
	}
	return &udpTracker{
		conn:        conn,
		ipv6:        addr.IP.To4() == nil,
		baseTimeout: udpBaseTimeout,
	}, nil
}

func (t *udpTracker) Close() error { return t.conn.Close() }

// connect obtains a connection ID unless the current one is still valid.
func (t *udpTracker) connect(ctx context.Context) error {
	if t.connectionID != 0 && time.Since(t.connectedAt) < udpConnectionLifetime {
		return nil
	}
	txID := rand.Uint32()
	req := make([]byte, 16)
	binary.BigEndian.PutUint64(req[0:8], MagicConnectionID)
	binary.BigEndian.PutUint32(req[8:12], ActionConnect)
	binary.BigEndian.PutUint32(req[12:16], txID)

	resp, err := t.roundTrip(ctx, req, txID)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	if err := checkAction(resp, ActionConnect); err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	if len(resp) < 16 {
		return fmt.Errorf("connect: %w: %d bytes", errUDPMalformed, len(resp))
	}
	t.connectionID = binary.BigEndian.Uint64(resp[8:16])
	t.connectedAt = time.Now()
	return nil
}

// announce sends a BEP 15 announce and parses the reply.
func (t *udpTracker) announce(ctx context.Context, request AnnounceRequest) (AnnounceResponse, error) {
	if err := t.connect(ctx); err != nil {
		return AnnounceResponse{}, err
	}

	txID := rand.Uint32()
	key := request.Key
	if key == 0 {
		key = sessionKey
	}
	// connection_id(8) action(4) transaction_id(4) info_hash(20) peer_id(20)
	// downloaded(8) left(8) uploaded(8) event(4) ip(4) key(4) num_want(4) port(2)
	req := make([]byte, 98)
	binary.BigEndian.PutUint64(req[0:8], t.connectionID)
	binary.BigEndian.PutUint32(req[8:12], ActionAnnounce)
	binary.BigEndian.PutUint32(req[12:16], txID)
	copy(req[16:36], request.InfoHash[:])
	copy(req[36:56], request.PeerID[:])
	binary.BigEndian.PutUint64(req[56:64], request.Downloaded)
	binary.BigEndian.PutUint64(req[64:72], request.Left)
	binary.BigEndian.PutUint64(req[72:80], request.Uploaded)
	binary.BigEndian.PutUint32(req[80:84], uint32(request.Event))
	binary.BigEndian.PutUint32(req[84:88], 0) // ip: use the sender address
	binary.BigEndian.PutUint32(req[88:92], key)
	binary.BigEndian.PutUint32(req[92:96], 0xFFFFFFFF) // num_want: tracker default
	binary.BigEndian.PutUint16(req[96:98], request.Port)

	resp, err := t.roundTrip(ctx, req, txID)
	if err != nil {
		return AnnounceResponse{}, fmt.Errorf("announce: %w", err)
	}
	if err := checkAction(resp, ActionAnnounce); err != nil {
		return AnnounceResponse{}, fmt.Errorf("announce: %w", err)
	}
	if len(resp) < 20 {
		return AnnounceResponse{}, fmt.Errorf("announce: %w: %d bytes", errUDPMalformed, len(resp))
	}

	peerSize := 6
	if t.ipv6 {
		peerSize = 18
	}
	peerBytes := resp[20:]
	if len(peerBytes)%peerSize != 0 {
		return AnnounceResponse{}, fmt.Errorf("announce: %w: %d peer bytes is not a multiple of %d", errUDPMalformed, len(peerBytes), peerSize)
	}
	peers := make([]Peer, 0, len(peerBytes)/peerSize)
	for off := 0; off < len(peerBytes); off += peerSize {
		ipLen := peerSize - 2
		ip := make(net.IP, ipLen) // copy: peerBytes aliases the read buffer
		copy(ip, peerBytes[off:off+ipLen])
		port := binary.BigEndian.Uint16(peerBytes[off+ipLen : off+peerSize])
		if port == 0 {
			continue
		}
		peers = append(peers, Peer{IP: ip, Port: port})
	}

	interval := time.Duration(binary.BigEndian.Uint32(resp[8:12])) * time.Second
	if interval <= 0 {
		interval = DefaultAnnounceInterval
	}
	return AnnounceResponse{Peers: peers, Interval: interval}, nil
}

// roundTrip sends req and returns the first reply carrying txID. It
// retransmits with an exponentially growing timeout (BEP 15) and ignores
// datagrams with another transaction ID, such as late replies to earlier
// attempts.
func (t *udpTracker) roundTrip(ctx context.Context, req []byte, txID uint32) ([]byte, error) {
	buf := make([]byte, udpMaxDatagram)
	timeout := t.baseTimeout
	for range udpMaxAttempts {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if _, err := t.conn.Write(req); err != nil {
			return nil, err
		}
		if err := t.conn.SetReadDeadline(operationDeadline(ctx, timeout)); err != nil {
			return nil, err
		}
		for {
			n, err := t.conn.Read(buf)
			var ne net.Error
			if errors.As(err, &ne) && ne.Timeout() {
				break // retransmit
			}
			if err != nil {
				return nil, err
			}
			if n < 8 || binary.BigEndian.Uint32(buf[4:8]) != txID {
				continue // stray or duplicate datagram
			}
			return buf[:n:n], nil
		}
		timeout *= 2
	}
	if err := ctx.Err(); err != nil {
		return nil, errors.Join(errUDPTimeout, err)
	}
	return nil, errUDPTimeout
}

// checkAction reports a tracker error reply, or a reply for the wrong action.
// The error action is checked first so its message is never mistaken for a
// short or malformed reply.
func checkAction(resp []byte, want uint32) error {
	switch action := binary.BigEndian.Uint32(resp[0:4]); action {
	case want:
		return nil
	case ActionError:
		return fmt.Errorf("tracker error: %s", sanitizeTrackerText(resp[8:]))
	default:
		return fmt.Errorf("%w: action %d, want %d", errUDPMalformed, action, want)
	}
}

// sanitizeTrackerText makes tracker-supplied text safe to print: it is
// truncated and quoted so control characters cannot reach the terminal.
func sanitizeTrackerText(b []byte) string {
	if len(b) > maxTrackerMessage {
		b = b[:maxTrackerMessage]
	}
	return strconv.Quote(string(b))
}

func announceUDP(ctx context.Context, hostPort string, request AnnounceRequest) (AnnounceResponse, error) {
	addr, err := resolveUDPAddrContext(ctx, hostPort)
	if err != nil {
		return AnnounceResponse{}, fmt.Errorf("resolving UDP tracker address: %w", err)
	}
	t, err := dialUDPTracker(addr)
	if err != nil {
		return AnnounceResponse{}, fmt.Errorf("opening UDP tracker socket: %w", err)
	}
	defer t.Close()

	response, err := t.announce(ctx, request)
	if err != nil {
		return AnnounceResponse{}, fmt.Errorf("udp tracker: %w", err)
	}
	return response, nil
}

func resolveUDPAddrContext(ctx context.Context, address string) (*net.UDPAddr, error) {
	host, portText, err := net.SplitHostPort(address)
	if err != nil {
		return nil, err
	}
	port, err := strconv.Atoi(portText)
	if err != nil || port < 1 || port > 65535 {
		return nil, fmt.Errorf("invalid UDP tracker port %q", portText)
	}

	zone := ""
	if zoneIndex := strings.LastIndexByte(host, '%'); zoneIndex >= 0 {
		zone = host[zoneIndex+1:]
		host = host[:zoneIndex]
	}
	if ip := net.ParseIP(host); ip != nil {
		return &net.UDPAddr{IP: ip, Port: port, Zone: zone}, nil
	}

	addresses, err := net.DefaultResolver.LookupIPAddr(ctx, host)
	if err != nil {
		return nil, err
	}
	if len(addresses) == 0 {
		return nil, fmt.Errorf("no addresses found for %s", host)
	}
	return &net.UDPAddr{IP: addresses[0].IP, Port: port, Zone: addresses[0].Zone}, nil
}

func operationDeadline(ctx context.Context, fallback time.Duration) time.Time {
	deadline := time.Now().Add(fallback)
	if contextDeadline, ok := ctx.Deadline(); ok && contextDeadline.Before(deadline) {
		return contextDeadline
	}
	return deadline
}
