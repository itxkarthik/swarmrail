package tracker

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
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
	// Parse the announce URL
	parsedURL, err := url.Parse(announceURL)
	if err != nil {
		return AnnounceResponse{}, err
	}
	// Set the query parameters
	query := parsedURL.Query()
	query.Set("info_hash", string(request.InfoHash[:]))
	query.Set("peer_id", string(request.PeerID[:]))
	query.Set("port", fmt.Sprintf("%d", request.Port))
	query.Set("uploaded", fmt.Sprintf("%d", request.Uploaded))
	query.Set("downloaded", fmt.Sprintf("%d", request.Downloaded))
	query.Set("left", fmt.Sprintf("%d", request.Left))
	query.Set("compact", "1")
	if event := request.Event.HTTPValue(); event != "" {
		query.Set("event", event)
	} else {
		query.Del("event")
	}
	// Encode the query parameters and set the URL
	parsedURL.RawQuery = query.Encode()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, parsedURL.String(), nil)
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
		return AnnounceResponse{}, fmt.Errorf("tracker failure: %s", failure)
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
