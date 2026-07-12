package tracker

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/itxkarthik/GoTorrent/internal/bencode"
)

func TestDiscoverPreservesIntervalAndAnnounceState(t *testing.T) {
	var captured map[string]string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		captured = map[string]string{
			"uploaded":   r.URL.Query().Get("uploaded"),
			"downloaded": r.URL.Query().Get("downloaded"),
			"left":       r.URL.Query().Get("left"),
			"event":      r.URL.Query().Get("event"),
		}
		body, err := bencode.Marshal(map[string]any{
			"interval": int64(120),
			"peers":    "",
		})
		if err != nil {
			t.Errorf("encoding response: %v", err)
			return
		}
		_, _ = w.Write(body)
	}))
	defer server.Close()

	discovery, err := Discover(context.Background(), []string{server.URL}, AnnounceRequest{
		Uploaded:   11,
		Downloaded: 22,
		Left:       33,
		Event:      EventCompleted,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(discovery.Results) != 1 || discovery.Results[0].Interval != 2*time.Minute {
		t.Fatalf("results = %#v, want one result with two-minute interval", discovery.Results)
	}
	for key, want := range map[string]string{
		"uploaded": "11", "downloaded": "22", "left": "33", "event": "completed",
	} {
		if got := captured[key]; got != want {
			t.Fatalf("query %s = %q, want %q", key, got, want)
		}
	}
}

func TestParseHTTPResponseUsesDefaultInterval(t *testing.T) {
	body, err := bencode.Marshal(map[string]any{"peers": ""})
	if err != nil {
		t.Fatal(err)
	}
	response, err := parseHTTPResponse(body)
	if err != nil {
		t.Fatal(err)
	}
	if response.Interval != DefaultAnnounceInterval {
		t.Fatalf("interval = %s, want %s", response.Interval, DefaultAnnounceInterval)
	}
}
