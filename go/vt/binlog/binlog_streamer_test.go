// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package binlog

import (
	"strings"
	"testing"

	"github.com/youtube/vitess/go/vt/mysqlctl"
)

func TestNewBinlogStreamer(t *testing.T) {
	var triggered bool

	*binlogStreamer = "fake"
	binlogStreamers = map[string]newBinlogStreamerFunc{
		"fake": func(string, *mysqlctl.Mysqld) BinlogStreamer {
			triggered = true
			return nil
		},
	}

	NewBinlogStreamer("", nil)

	if !triggered {
		t.Errorf("NewBinlogStreamer() failed to call the right newBinlogStreamerFunc.")
	}
}

func TestNewBinlogStreamerUnknown(t *testing.T) {
	*binlogStreamer = "unknown"
	binlogStreamers = map[string]newBinlogStreamerFunc{}
	want := "unknown BinlogStreamer implementation: \"unknown\""

	defer func() {
		err := recover()
		if err == nil {
			t.Errorf("no panicked error, want %#v", want)
		}
		got, ok := err.(error)
		if !ok || !strings.HasPrefix(got.Error(), want) {
			t.Errorf("wrong error, got %#v, want %#v", got, want)
		}
	}()

	NewBinlogStreamer("", nil)
}
