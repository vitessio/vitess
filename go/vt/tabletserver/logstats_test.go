// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tabletserver

import (
	"net/url"
	"strings"
	"testing"
	"time"

	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/vt/callinfo"

	vtrpcpb "github.com/youtube/vitess/go/vt/proto/vtrpc"
)

func TestLogStats(t *testing.T) {
	logStats := NewLogStats(context.Background(), "test")
	logStats.AddRewrittenSQL("sql1", time.Now())

	if !strings.Contains(logStats.RewrittenSQL(), "sql1") {
		t.Fatalf("RewrittenSQL should contains sql: sql1")
	}

	if logStats.SizeOfResponse() != 0 {
		t.Fatalf("there is no rows in log stats, estimated size should be 0 bytes")
	}

	logStats.Rows = [][]sqltypes.Value{{sqltypes.MakeString([]byte("a"))}}
	if logStats.SizeOfResponse() <= 0 {
		t.Fatalf("log stats has some rows, should have positive response size")
	}

	params := map[string][]string{"full": {}}

	logStats.Format(url.Values(params))
}

func TestLogStatsFormatBindVariables(t *testing.T) {
	logStats := NewLogStats(context.Background(), "test")
	logStats.BindVariables = make(map[string]interface{})
	logStats.BindVariables["key_1"] = "val_1"
	logStats.BindVariables["key_2"] = 789

	formattedStr := logStats.FmtBindVariables(true)
	if !strings.Contains(formattedStr, "key_1") ||
		!strings.Contains(formattedStr, "val_1") {
		t.Fatalf("bind variable 'key_1': 'val_1' is not formatted")
	}
	if !strings.Contains(formattedStr, "key_2") ||
		!strings.Contains(formattedStr, "789") {
		t.Fatalf("bind variable 'key_2': '789' is not formatted")
	}

	logStats.BindVariables["key_3"] = []byte("val_3")
	formattedStr = logStats.FmtBindVariables(false)
	if !strings.Contains(formattedStr, "key_1") {
		t.Fatalf("bind variable 'key_1' is not formatted")
	}
	if !strings.Contains(formattedStr, "key_2") ||
		!strings.Contains(formattedStr, "789") {
		t.Fatalf("bind variable 'key_2': '789' is not formatted")
	}
	if !strings.Contains(formattedStr, "key_3") {
		t.Fatalf("bind variable 'key_3' is not formatted")
	}
}

func TestLogStatsFormatQuerySources(t *testing.T) {
	logStats := NewLogStats(context.Background(), "test")
	if logStats.FmtQuerySources() != "none" {
		t.Fatalf("should return none since log stats does not have any query source, but got: %s", logStats.FmtQuerySources())
	}

	logStats.QuerySources |= QuerySourceMySQL
	if !strings.Contains(logStats.FmtQuerySources(), "mysql") {
		t.Fatalf("'mysql' should be in formated query sources")
	}

	logStats.QuerySources |= QuerySourceConsolidator
	if !strings.Contains(logStats.FmtQuerySources(), "consolidator") {
		t.Fatalf("'consolidator' should be in formated query sources")
	}
}

func TestLogStatsContextHTML(t *testing.T) {
	html := "HtmlContext"
	callInfo := &fakeCallInfo{
		html: html,
	}
	ctx := callinfo.NewContext(context.Background(), callInfo)
	logStats := NewLogStats(ctx, "test")
	if string(logStats.ContextHTML()) != html {
		t.Fatalf("expect to get html: %s, but got: %s", html, string(logStats.ContextHTML()))
	}
}

func TestLogStatsErrorStr(t *testing.T) {
	logStats := NewLogStats(context.Background(), "test")
	if logStats.ErrorStr() != "" {
		t.Fatalf("should not get error in stats, but got: %s", logStats.ErrorStr())
	}
	errStr := "unknown error"
	logStats.Error = &TabletError{
		ErrorCode: vtrpcpb.ErrorCode_UNKNOWN_ERROR,
		Message:   errStr,
	}
	if !strings.Contains(logStats.ErrorStr(), errStr) {
		t.Fatalf("expect string '%s' in error message, but got: %s", errStr, logStats.ErrorStr())
	}
}

func TestLogStatsRemoteAddrUsername(t *testing.T) {
	logStats := NewLogStats(context.Background(), "test")
	addr, user := logStats.RemoteAddrUsername()
	if addr != "" {
		t.Fatalf("remote addr should be empty")
	}
	if user != "" {
		t.Fatalf("username should be empty")
	}

	remoteAddr := "1.2.3.4"
	username := "vt"
	callInfo := &fakeCallInfo{
		remoteAddr: remoteAddr,
		username:   username,
	}
	ctx := callinfo.NewContext(context.Background(), callInfo)
	logStats = NewLogStats(ctx, "test")
	addr, user = logStats.RemoteAddrUsername()
	if addr != remoteAddr {
		t.Fatalf("expected to get remote addr: %s, but got: %s", remoteAddr, addr)
	}
	if user != username {
		t.Fatalf("expected to get username: %s, but got: %s", username, user)
	}
}
