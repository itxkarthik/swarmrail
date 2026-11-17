package tracker

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/itxkarthik/swarmrail/internal/bencode"
)

type Peer struct {
	IP   net.IP
	Port uint16
}

// AnnounceHTTP sends an HTTP announce request to the tracker and returns the list of peers.
func AnnounceHTTP(announceURL string, infoHash [20]byte, peerID [20]byte, port uint16, totalLength int64) ([]Peer, error) {
	ctx, cancel := context.WithTimeout(context.Background(), defaultAnnounceTimeout)
	defer cancel()
	return AnnounceHTTPContext(ctx, announceURL, infoHash, peerID, port, totalLength)
}

// AnnounceHTTPContext sends an HTTP announce that can be cancelled by ctx.
func AnnounceHTTPContext(ctx context.Context, announceURL string, infoHash [20]byte, peerID [20]byte, port uint16, totalLength int64) ([]Peer, error) {
	response, err := announceHTTP(ctx, announceURL, AnnounceRequest{
		InfoHash: infoHash,
		PeerID:   peerID,
		Port:     port,
		Left:     uint64(totalLength),
		Event:    EventStarted,
	})
	return response.Peers, err
}

func announceHTTP(ctx context.Context, announceURL string, request AnnounceRequest) (AnnounceResponse, error) {
	parsedURL, err := url.Parse(announceURL)
	if err != nil {
		return AnnounceResponse{}, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, buildAnnounceURL(parsedURL, request), nil)
	if err != nil {
		return AnnounceResponse{}, err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return AnnounceResponse{}, err
	}
	// Close the response body when done
	defer resp.Body.Close()
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return AnnounceResponse{}, fmt.Errorf("tracker returned HTTP %s", resp.Status)
	}
	// Read the response body
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return AnnounceResponse{}, err
	}
	// Parse the response body
	return parseHTTPResponse(body)
}

// buildAnnounceURL appends the BEP 3 announce parameters to base.
//
// info_hash and peer_id are raw bytes, so they are percent-encoded byte by
// byte. url.Values would use form encoding and turn 0x20 into '+', which
// some trackers decode as a literal plus, giving a different infohash. The
// tracker's own query (often a private passkey) is kept exactly as given
// rather than parsed and re-encoded, which could reorder or re-escape it.
func buildAnnounceURL(base *url.URL, request AnnounceRequest) string {
	key := request.Key
	if key == 0 {
		key = sessionKey
	}
	params := url.Values{}
	params.Set("port", strconv.FormatUint(uint64(request.Port), 10))
	params.Set("uploaded", strconv.FormatUint(request.Uploaded, 10))
	params.Set("downloaded", strconv.FormatUint(request.Downloaded, 10))
	params.Set("left", strconv.FormatUint(request.Left, 10))
	params.Set("compact", "1")
	params.Set("key", fmt.Sprintf("%08x", key))
	if event := request.Event.HTTPValue(); event != "" {
		params.Set("event", event)
	}

	query := "info_hash=" + escapeBinary(request.InfoHash[:]) +
		"&peer_id=" + escapeBinary(request.PeerID[:]) +
		"&" + params.Encode()
	u := *base
	if u.RawQuery != "" {
		query = u.RawQuery + "&" + query
	}
	u.RawQuery = query
	u.Fragment = ""
	return u.String()
}

// escapeBinary percent-encodes every byte outside the RFC 3986 unreserved
// set (ALPHA / DIGIT / "-" / "." / "_" / "~").
func escapeBinary(b []byte) string {
	const hex = "0123456789ABCDEF"
	var sb strings.Builder
	sb.Grow(len(b) * 3)
	for _, c := range b {
		if 'a' <= c && c <= 'z' || 'A' <= c && c <= 'Z' || '0' <= c && c <= '9' ||
			c == '-' || c == '.' || c == '_' || c == '~' {
			sb.WriteByte(c)
			continue
		}
		sb.WriteByte('%')
		sb.WriteByte(hex[c>>4])
		sb.WriteByte(hex[c&0x0F])
	}
	return sb.String()
}

// parseHTTPResponse parses the HTTP response body and returns a list of peers.
func parseHTTPResponse(body []byte) (AnnounceResponse, error) {
	// Decode the response body
	decoded, _, err := bencode.Decode(body)
	if err != nil {
		return AnnounceResponse{}, err
	}

	// Check the response type
	top, ok := decoded.(map[string]any)
	if !ok {
		return AnnounceResponse{}, fmt.Errorf("invalid tracker response: expected dict")
	}
	if failure, ok := top["failure reason"].(string); ok {
		return AnnounceResponse{}, fmt.Errorf("tracker failure: %s", sanitizeTrackerText([]byte(failure)))
	}

	// Check for peers in the response
	peersVal, ok := top["peers"]
	if !ok {
		return AnnounceResponse{}, fmt.Errorf("invalid tracker response: missing peers")
	}
	interval := DefaultAnnounceInterval
	if seconds, ok := top["interval"].(int64); ok && seconds > 0 {
		interval = time.Duration(seconds) * time.Second
	}
	if seconds, ok := top["min interval"].(int64); ok && seconds > 0 {
		interval = max(interval, time.Duration(seconds)*time.Second)
	}

	// Unmarshal the peers based on their type
	switch peers := peersVal.(type) {
	case string:
		parsed, err := unmarshalCompactPeers([]byte(peers))
		return AnnounceResponse{Peers: parsed, Interval: interval}, err
	case []any:
		parsed, err := unmarshalPeerList(peers)
		return AnnounceResponse{Peers: parsed, Interval: interval}, err
	default:
		return AnnounceResponse{}, fmt.Errorf("invalid tracker response: unsupported peers type %T", peersVal)
	}
}

// unmarshalCompactPeers parses compact peer data (6 bytes per peer).
func unmarshalCompactPeers(peersBin []byte) ([]Peer, error) {
	const peerSize = 6
	if len(peersBin)%peerSize != 0 {
		return nil, fmt.Errorf("received malformed peers: length is not a multiple of %d", peerSize)
	}
	numPeers := len(peersBin) / peerSize
	peers := make([]Peer, numPeers)
	for i := range numPeers {
		offset := i * peerSize
		peers[i].IP = net.IP(peersBin[offset : offset+4])
		peers[i].Port = binary.BigEndian.Uint16(peersBin[offset+4 : offset+6])
	}
	return peers, nil
}

// unmarshalPeerList parses a non-compact peers list from a tracker response.
func unmarshalPeerList(peersList []any) ([]Peer, error) {
	peers := make([]Peer, 0, len(peersList))
	for _, entry := range peersList {
		peerMap, ok := entry.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("invalid peers entry: expected dict")
		}
		ipStr, ok := peerMap["ip"].(string)
		if !ok {
			return nil, fmt.Errorf("invalid peers entry: missing ip")
		}
		portVal, ok := peerMap["port"].(int64)
		if !ok {
			return nil, fmt.Errorf("invalid peers entry: missing port")
		}
		peers = append(peers, Peer{IP: net.ParseIP(ipStr), Port: uint16(portVal)})
	}
	return peers, nil
}
