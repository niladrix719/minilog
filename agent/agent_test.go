package agent

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/niladrix719/minilog/internalapi"
)

// backoff doubles from min, caps at max, and must not overflow for huge
// attempt counts: an outage lasting days means attempt reaches the millions.
func TestBackoff(t *testing.T) {
	min, max := 100*time.Millisecond, 10*time.Second
	for _, c := range []struct {
		attempt int
		want    time.Duration
	}{
		{0, 100 * time.Millisecond},
		{1, 200 * time.Millisecond},
		{3, 800 * time.Millisecond},
		{7, 10 * time.Second}, // 100ms << 7 = 12.8s, capped
		{100, 10 * time.Second},
		{1_000_000, 10 * time.Second},
	} {
		if got := backoff(c.attempt, min, max); got != c.want {
			t.Fatalf("backoff(%d) = %v, want %v", c.attempt, got, c.want)
		}
	}
}

// sendBlock must hit the real insert path with the version arg, ship the block
// bytes untouched, and classify the outcome four ways. The classification is
// the whole point: it decides whether runSender acks, retries, or drops.
func TestSendBlock(t *testing.T) {
	var gotPath, gotVersion string
	var gotBody []byte
	status := http.StatusNoContent
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		gotVersion = r.URL.Query().Get(internalapi.VersionArg)
		gotBody, _ = io.ReadAll(r.Body)
		http.Error(w, "reason the log line should carry", status)
	}))
	d := &destination{
		addr:   srv.Listener.Addr().String(),
		client: &http.Client{Transport: &http.Transport{}},
	}
	block := []byte("opaque block bytes")

	for _, c := range []struct {
		status int
		want   sendResult
	}{
		{http.StatusNoContent, sendStored},
		{http.StatusBadRequest, sendRejected}, // will never succeed: drop it
		{http.StatusNotFound, sendRejected},
		{http.StatusInternalServerError, sendServerError}, // node is up but broken: retry
		{http.StatusServiceUnavailable, sendServerError},
		{http.StatusTooManyRequests, sendServerError}, // 4xx, but transient: must NOT be dropped
	} {
		status = c.status
		got, _ := d.sendBlock(block)
		if got != c.want {
			t.Fatalf("status %d: got result %v, want %v", c.status, got, c.want)
		}
	}
	if gotPath != internalapi.InsertPath || gotVersion != internalapi.ProtocolVersion {
		t.Fatalf("request went to %q version=%q, want %q version=%q",
			gotPath, gotVersion, internalapi.InsertPath, internalapi.ProtocolVersion)
	}
	if !bytes.Equal(gotBody, block) {
		t.Fatalf("body %q, want the block unchanged", gotBody)
	}

	srv.Close() // no listener: connection refused, no HTTP response at all
	if got, err := d.sendBlock(block); got != sendUnavailable || err == nil {
		t.Fatalf("dead node: got (%v, %v), want (sendUnavailable, non-nil error)", got, err)
	}
}
