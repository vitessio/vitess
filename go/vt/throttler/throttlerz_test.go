package throttler

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestThrottlerzHandler_MissingSlash(t *testing.T) {
	request, _ := http.NewRequest("GET", "/throttlerz", nil)
	response := httptest.NewRecorder()
	m := newManager()

	throttlerzHandler(response, request, m)

	if got, want := response.Body.String(), "invalid /throttlerz path"; !strings.Contains(got, want) {
		t.Fatalf("/throttlerz without the slash does not work (the Go HTTP server does automatically redirect in practice though). got = %v, want = %v", got, want)
	}
}

func TestThrottlerzHandler_List(t *testing.T) {
	f := &managerTestFixture{}
	if err := f.setUp(); err != nil {
		t.Fatal(err)
	}
	defer f.tearDown()

	request, _ := http.NewRequest("GET", "/throttlerz/", nil)
	response := httptest.NewRecorder()

	throttlerzHandler(response, request, f.m)

	if got, want := response.Body.String(), `<a href="/throttlerz/t1">t1</a>`; !strings.Contains(got, want) {
		t.Fatalf("list does not include 't1'. got = %v, want = %v", got, want)
	}
	if got, want := response.Body.String(), `<a href="/throttlerz/t2">t2</a>`; !strings.Contains(got, want) {
		t.Fatalf("list does not include 't1'. got = %v, want = %v", got, want)
	}
}

func TestThrottlerzHandler_Details(t *testing.T) {
	f := &managerTestFixture{}
	if err := f.setUp(); err != nil {
		t.Fatal(err)
	}
	defer f.tearDown()

	request, _ := http.NewRequest("GET", "/throttlerz/t1", nil)
	response := httptest.NewRecorder()

	throttlerzHandler(response, request, f.m)

	if got, want := response.Body.String(), `<title>Details for Throttler 't1'</title>`; !strings.Contains(got, want) {
		t.Fatalf("details for 't1' not shown. got = %v, want = %v", got, want)
	}
}
