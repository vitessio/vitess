package throttler

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestThrottlerzHandler_InvalidSubPage(t *testing.T) {
	request, _ := http.NewRequest("GET", "/throttlerz/name/invalid", nil)
	response := httptest.NewRecorder()
	m := newManager()

	throttlerzHandler(response, request, m)

	if got, want := response.Body.String(), "invalid sub page: invalid for throttler: name"; !strings.Contains(got, want) {
		t.Fatalf("should show an error message because of an invalid sub page. got = %v, want = %v", got, want)
	}
}

func TestThrottlerzHandler_InvalidPath(t *testing.T) {
	request, _ := http.NewRequest("GET", "/throttlerz/name/invalid/wrong", nil)
	response := httptest.NewRecorder()
	m := newManager()

	throttlerzHandler(response, request, m)

	if got, want := response.Body.String(), "expected paths"; !strings.Contains(got, want) {
		t.Fatalf("should show an error message because of an invalid path. got = %v, want = %v", got, want)
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

func TestThrottlerzHandler_Log_NonExistantThrottler(t *testing.T) {
	request, _ := http.NewRequest("GET", "/throttlerz/t1/log", nil)
	response := httptest.NewRecorder()

	throttlerzHandler(response, request, newManager())

	if got, want := response.Body.String(), `throttler: t1 does not exist`; !strings.Contains(got, want) {
		t.Fatalf("/log page for non-existant t1 should not succeed. got = %v, want = %v", got, want)
	}
}

func TestThrottlerzHandler_Log(t *testing.T) {
	f := &managerTestFixture{}
	if err := f.setUp(); err != nil {
		t.Fatal(err)
	}
	defer f.tearDown()

	testcases := []struct {
		desc string
		r    result
		want string
	}{
		{
			"increased rate",
			resultIncreased,
			`    <tr class="low">
      <td>00:00:01
      <td>increased
      <td>100
      <td>100
      <td>cell1-0000000101
      <td>1s
      <td>1.2s
      <td>99
      <td>good
      <td>
      <td>95
      <td>0
      <td>I
      <td>I
      <td>I
      <td>n/a
      <td>n/a
      <td>99
      <td>0
      <td>0
      <td>0
      <td>increased the rate`,
		},
		{
			"decreased rate",
			resultDecreased,
			`    <tr class="medium">
      <td>00:00:05
      <td>decreased
      <td>200
      <td>100
      <td>cell1-0000000101
      <td>2s
      <td>3.8s
      <td>200
      <td>bad
      <td>
      <td>95
      <td>200
      <td>I
      <td>D
      <td>D
      <td>1s
      <td>3.8s
      <td>200
      <td>150
      <td>10
      <td>20
      <td>decreased the rate`,
		},
		{
			"emergency state decreased the rate",
			resultEmergency,
			`    <tr class="high">
      <td>00:00:10
      <td>decreased
      <td>100
      <td>50
      <td>cell1-0000000101
      <td>23s
      <td>5.1s
      <td>100
      <td>bad
      <td>
      <td>95
      <td>100
      <td>D
      <td>E
      <td>E
      <td>2s
      <td>5.1s
      <td>0
      <td>0
      <td>0
      <td>0
      <td>emergency state decreased the rate`,
		},
	}

	for _, tc := range testcases {
		request, _ := http.NewRequest("GET", "/throttlerz/t1/log", nil)
		response := httptest.NewRecorder()

		f.t1.maxReplicationLagModule.results.add(tc.r)
		throttlerzHandler(response, request, f.m)

		got := response.Body.String()
		if !strings.Contains(got, tc.want) {
			t.Fatalf("testcase '%v': result not shown in log. got = %v, want = %v", tc.desc, got, tc.want)
		}
	}
}
