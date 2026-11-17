package tracker

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
)

func TestEscapeBinaryUsesPercentEncoding(t *testing.T) {
	got := escapeBinary([]byte{' ', 'A', '+', 0xFF, '~', '-', '.', '_', '/', 'z', '9'})
	if want := "%20A%2B%FF~-._%2Fz9"; got != want {
		t.Fatalf("escapeBinary = %q, want %q", got, want)
	}
}

// url.Values encodes 0x20 as '+'. Trackers that read '+' literally then see
// a different infohash and reply "torrent not registered".
func TestAnnounceURLNeverEncodesSpaceAsPlus(t *testing.T) {
	var infoHash [20]byte
	copy(infoHash[:], " A\xff+")
	base, _ := url.Parse("http://tracker.example/announce")
	got := buildAnnounceURL(base, AnnounceRequest{InfoHash: infoHash, Port: 6881})
	if !strings.Contains(got, "info_hash=%20A%FF%2B") {
		t.Fatalf("announce URL %q does not percent-encode the infohash", got)
	}
}

func TestAnnounceURLPreservesTrackerQuery(t *testing.T) {
	const rawQuery = "passkey=Ab%2Fcd+e&z=1&a=2"
	base, _ := url.Parse("https://tracker.example/announce?" + rawQuery + "#frag")
	got := buildAnnounceURL(base, AnnounceRequest{Port: 6881, Event: EventStarted})
	if !strings.HasPrefix(got, "https://tracker.example/announce?"+rawQuery+"&info_hash=") {
		t.Fatalf("tracker query was not preserved byte for byte: %q", got)
	}
	if strings.Contains(got, "#") {
		t.Fatalf("fragment leaked into the request: %q", got)
	}
	if !strings.Contains(got, "event=started") || !strings.Contains(got, "compact=1") {
		t.Fatalf("missing announce parameters: %q", got)
	}
}

func TestHTTPAnnounceInfoHashRoundTripsThroughServer(t *testing.T) {
	var infoHash, peerID [20]byte
	for i := range infoHash {
		infoHash[i] = byte(i * 13) // includes 0x00, 0x20-adjacent and high bytes
	}
	infoHash[3] = ' '
	infoHash[4] = '+'
	copy(peerID[:], "-SR0001-abcdefghijkl")

	var gotHash, gotPeer, gotKey string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Decode the raw query the way a strict RFC 3986 tracker would:
		// '+' stays '+'.
		for _, part := range strings.Split(r.URL.RawQuery, "&") {
			name, value, _ := strings.Cut(part, "=")
			decoded, err := url.PathUnescape(value)
			if err != nil {
				t.Errorf("unescaping %s: %v", name, err)
			}
			switch name {
			case "info_hash":
				gotHash = decoded
			case "peer_id":
				gotPeer = decoded
			case "key":
				gotKey = decoded
			}
		}
		_, _ = w.Write([]byte("d8:intervali60e5:peers0:e"))
	}))
	defer server.Close()

	if _, err := announceHTTP(context.Background(), server.URL+"/announce", AnnounceRequest{InfoHash: infoHash, PeerID: peerID, Port: 6881}); err != nil {
		t.Fatal(err)
	}
	if gotHash != string(infoHash[:]) {
		t.Fatalf("tracker decoded info_hash %x, want %x", gotHash, infoHash)
	}
	if gotPeer != string(peerID[:]) {
		t.Fatalf("tracker decoded peer_id %q, want %q", gotPeer, peerID)
	}
	if len(gotKey) != 8 {
		t.Fatalf("key = %q, want 8 hex digits", gotKey)
	}
}

func TestHTTPFailureReasonIsQuoted(t *testing.T) {
	_, err := parseHTTPResponse([]byte("d14:failure reason9:bad\x1b[2Jxxe"))
	if err == nil || strings.Contains(err.Error(), "\x1b") {
		t.Fatalf("error = %q, want the failure reason with control characters escaped", err)
	}
}
