// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tabletserver

import (
	"testing"
	"time"
)

func TestDeadline(t *testing.T) {
	dl := Deadline(time.Now())
	_, err := dl.Timeout()
	if err != TimeoutErr {
		t.Errorf("got %v, want %v", err, TimeoutErr)
	}

	dl = Deadline(time.Now().Add(-1 * time.Minute))
	_, err = dl.Timeout()
	if err != TimeoutErr {
		t.Errorf("got %v, want %v", err, TimeoutErr)
	}

	dl = Deadline(time.Now().Add(1 * time.Minute))
	timeout, err := dl.Timeout()
	if err != nil {
		t.Errorf("Error: %v", err)
	}
	if timeout < 500*time.Millisecond {
		t.Errorf("got %v, want >500ms", timeout)
	}
}
