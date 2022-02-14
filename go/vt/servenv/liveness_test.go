package servenv

import (
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestLivenessHandler(t *testing.T) {
	server := httptest.NewServer(nil)
	defer server.Close()

	resp, err := http.Get(server.URL + "/debug/liveness")
	if err != nil {
		t.Fatalf("http.Get: %v", err)
	}
	defer resp.Body.Close()

	// Make sure we can read the body, even though it's empty.
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("io.ReadAll: %v", err)
	}
	t.Logf("body: %q", body)
}
