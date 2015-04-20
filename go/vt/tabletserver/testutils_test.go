// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tabletserver

import (
	"fmt"
	"html/template"
	"math/rand"
	"reflect"
	"testing"
	"time"
)

type fakeCallInfo struct {
	remoteAddr string
	username   string
	text       string
	html       string
}

func (fci *fakeCallInfo) RemoteAddr() string {
	return fci.remoteAddr
}

func (fci *fakeCallInfo) Username() string {
	return fci.username
}

func (fci *fakeCallInfo) Text() string {
	return fci.text
}

func (fci *fakeCallInfo) HTML() template.HTML {
	return template.HTML(fci.html)
}

type testUtils struct{}

func (util *testUtils) checkEqual(t *testing.T, expected interface{}, result interface{}) {
	if !reflect.DeepEqual(expected, result) {
		t.Fatalf("expect to get: %v, but got: %v", expected, result)
	}
}

func newTestSchemaInfo(
	queryCacheSize int,
	reloadTime time.Duration,
	idleTimeout time.Duration) *SchemaInfo {
	randID := rand.Int63()
	return NewSchemaInfo(
		queryCacheSize,
		fmt.Sprintf("TestSchemaInfo-%d-", randID),
		map[string]string{
			debugQueryPlansKey: fmt.Sprintf("/debug/query_plans_%d", randID),
			debugQueryStatsKey: fmt.Sprintf("/debug/query_stats_%d", randID),
			debugTableStatsKey: fmt.Sprintf("/debug/table_stats_%d", randID),
			debugSchemaKey:     fmt.Sprintf("/debug/schema_%d", randID),
		},
		reloadTime,
		idleTimeout)
}
