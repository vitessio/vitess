/*
Copyright 2018 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package locker

import (
	"strings"
	"testing"
	"time"

	"github.com/youtube/vitess/go/vt/vttablet/tabletserver/tabletenv"
	"golang.org/x/net/context"
)

func TestOpenClose(t *testing.T) {
	l := newTestLocker()
	l.Close()

	_, err := l.Lock(context.Background(), "", 0)
	want := "cannot perform locking function, probably because this is not a master any more"
	verifyError(t, "Lock", err, want)

	err = l.Release("")
	verifyError(t, "Lock", err, want)
}

func TestLock(t *testing.T) {
	l := newTestLocker()

	// First lock.
	start := time.Now()
	result, err := l.Lock(context.Background(), "", 0)
	checkError(t, err)
	if want, got := result, true; got != want {
		t.Errorf("Lock: %v, want %v", got, want)
	}
	if diff := time.Now().Sub(start); diff > 10*time.Millisecond {
		t.Errorf("Lock time: %v, want <10ms", diff)
	}

	// Second lock: specify long timeout.
	start = time.Now()
	result, err = l.Lock(context.Background(), "", 20*time.Millisecond)
	checkError(t, err)
	if want, got := result, true; got != want {
		t.Errorf("Lock: %v, want %v", got, want)
	}
	if diff := time.Now().Sub(start); diff < 5*time.Millisecond || diff > 15*time.Millisecond {
		t.Errorf("Lock time: %v, 1ms< want <10ms", diff)
	}
}

func TestLockTimeout(t *testing.T) {
	l := newTestLocker()

	// First lock.
	start := time.Now()
	result, err := l.Lock(context.Background(), "", 0)
	checkError(t, err)
	if want, got := result, true; got != want {
		t.Errorf("Lock: %v, want %v", got, want)
	}
	if diff := time.Now().Sub(start); diff > 10*time.Millisecond {
		t.Errorf("Lock time: %v, want <10ms", diff)
	}

	// Second lock: specify a short timeout.
	start = time.Now()
	result, err = l.Lock(context.Background(), "", 1*time.Millisecond)
	checkError(t, err)
	if want, got := result, false; got != want {
		t.Errorf("Lock: %v, want %v", got, want)
	}
	if diff := time.Now().Sub(start); diff < 1*time.Millisecond || diff > 10*time.Millisecond {
		t.Errorf("Lock time: %v, 1ms< want <10ms", diff)
	}

	// Third lock: see if context timeout gets overridden.
	start = time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
	defer cancel()
	result, err = l.Lock(ctx, "", 1*time.Millisecond)
	checkError(t, err)
	if want, got := result, false; got != want {
		t.Errorf("Lock: %v, want %v", got, want)
	}
	if diff := time.Now().Sub(start); diff < 1*time.Millisecond || diff > 10*time.Millisecond {
		t.Errorf("Lock time: %v, 1ms< want <10ms", diff)
	}

	// Fourth lock: see if context timeout overrides longer timeout
	start = time.Now()
	ctx, cancel = context.WithTimeout(context.Background(), 1*time.Millisecond)
	defer cancel()
	result, err = l.Lock(ctx, "", 5*time.Millisecond)
	checkError(t, err)
	if want, got := result, false; got != want {
		t.Errorf("Lock: %v, want %v", got, want)
	}
	if diff := time.Now().Sub(start); diff < 1*time.Millisecond || diff > 10*time.Millisecond {
		t.Errorf("Lock time: %v, 1ms< want <10ms", diff)
	}

	// Fifth lock: set a large timeout and see it succeed
	start = time.Now()
	ctx, cancel = context.WithTimeout(context.Background(), 1*time.Millisecond)
	defer cancel()
	result, err = l.Lock(ctx, "", 5*time.Millisecond)
	checkError(t, err)
	if want, got := result, false; got != want {
		t.Errorf("Lock: %v, want %v", got, want)
	}
	if diff := time.Now().Sub(start); diff < 1*time.Millisecond || diff > 10*time.Millisecond {
		t.Errorf("Lock time: %v, 1ms< want <10ms", diff)
	}
}

func TestRelease(t *testing.T) {
	l := newTestLocker()

	start := time.Now()
	result, err := l.Lock(context.Background(), "", 0)
	checkError(t, err)
	if want, got := result, true; got != want {
		t.Errorf("Lock: %v, want %v", got, want)
	}
	if diff := time.Now().Sub(start); diff > 10*time.Millisecond {
		t.Errorf("Lock time: %v, want <10ms", diff)
	}

	// Release
	err = l.Release("")
	checkError(t, err)

	// Re-release
	err = l.Release("")
	checkError(t, err)

	// New lock. Should be immediate
	start = time.Now()
	result, err = l.Lock(context.Background(), "", 0)
	checkError(t, err)
	if want, got := result, true; got != want {
		t.Errorf("Lock: %v, want %v", got, want)
	}
	if diff := time.Now().Sub(start); diff > 10*time.Millisecond {
		t.Errorf("Lock time: %v, want <10ms", diff)
	}
}

func newTestLocker() *Locker {
	l := NewLocker(tabletenv.TabletConfig{
		TransactionTimeout: 0.01,
	})
	l.Open()
	return l
}

func verifyError(t *testing.T, prefix string, err error, want string) {
	t.Helper()
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("%s error: %v, must contina %s", prefix, err, want)
	}
}

func checkError(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}
