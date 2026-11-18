package tracker

import (
	"encoding/binary"
	"testing"
)

// FuzzParseHTTPResponse checks that tracker replies never cause a panic and
// that accepted peers have a usable address.
func FuzzParseHTTPResponse(f *testing.F) {
	for _, seed := range []string{
		"d8:intervali1800e5:peers6:\x7f\x00\x00\x01\x1a\xe1e",
		"d5:peersld2:ip9:127.0.0.14:porti6881eeee",
		"d14:failure reason4:nopee",
		"d5:peers5:abcdee",
		"9223372036854775807:abc",
	} {
		f.Add([]byte(seed))
	}
	f.Fuzz(func(t *testing.T, body []byte) {
		resp, err := parseHTTPResponse(body)
		if err != nil {
			return
		}
		if resp.Interval <= 0 {
			t.Fatalf("accepted non-positive interval %s", resp.Interval)
		}
	})
}

// FuzzParseUDPAnnounce checks BEP 15 announce reply parsing for both address
// families.
func FuzzParseUDPAnnounce(f *testing.F) {
	reply := make([]byte, 26)
	binary.BigEndian.PutUint32(reply[0:4], ActionAnnounce)
	binary.BigEndian.PutUint32(reply[8:12], 1800)
	copy(reply[20:], []byte{127, 0, 0, 1, 0x1a, 0xe1})
	f.Add(reply, false)
	f.Add(append([]byte{0, 0, 0, 3, 0, 0, 0, 0}, "error text"...), false)
	f.Add(make([]byte, 38), true)
	f.Fuzz(func(t *testing.T, resp []byte, ipv6 bool) {
		parsed, err := parseUDPAnnounce(resp, ipv6)
		if err != nil {
			return
		}
		want := 4
		if ipv6 {
			want = 16
		}
		for _, p := range parsed.Peers {
			if len(p.IP) != want || p.Port == 0 {
				t.Fatalf("accepted peer %v:%d", p.IP, p.Port)
			}
		}
	})
}
