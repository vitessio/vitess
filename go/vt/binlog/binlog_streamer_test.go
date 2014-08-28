// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package binlog

import (
	"strings"
	"testing"

	"github.com/youtube/vitess/go/vt/binlog/proto"
	"github.com/youtube/vitess/go/vt/mysqlctl"
	myproto "github.com/youtube/vitess/go/vt/mysqlctl/proto"
)

func TestNewBinlogStreamer(t *testing.T) {
	var triggered bool

	*binlogStreamer = "fake"
	binlogStreamers = map[string]newBinlogStreamerFunc{
		"fake": func(string, *mysqlctl.Mysqld, myproto.GTID, sendTransactionFunc) BinlogStreamer {
			triggered = true
			return nil
		},
	}

	NewBinlogStreamer("", nil, nil, nil)

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

	NewBinlogStreamer("", nil, nil, nil)
}

func TestGetStatementCategory(t *testing.T) {
	table := map[string]int{
		"":  proto.BL_UNRECOGNIZED,
		" ": proto.BL_UNRECOGNIZED,
		" UPDATE we don't try to fix leading spaces": proto.BL_UNRECOGNIZED,
		"FOOBAR unknown query prefix":                proto.BL_UNRECOGNIZED,

		"BEGIN":    proto.BL_BEGIN,
		"COMMIT":   proto.BL_COMMIT,
		"ROLLBACK": proto.BL_ROLLBACK,
		"INSERT something (something, something)": proto.BL_DML,
		"UPDATE something SET something=nothing":  proto.BL_DML,
		"DELETE something":                        proto.BL_DML,
		"CREATE something":                        proto.BL_DDL,
		"ALTER something":                         proto.BL_DDL,
		"DROP something":                          proto.BL_DDL,
		"TRUNCATE something":                      proto.BL_DDL,
		"RENAME something":                        proto.BL_DDL,
		"SET something=nothing":                   proto.BL_SET,
	}

	for input, want := range table {
		if got := getStatementCategory([]byte(input)); got != want {
			t.Errorf("getStatementCategory(%v) = %v, want %v", input, got, want)
		}
	}
}
