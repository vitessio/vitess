// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package mysqlctl

import (
	"testing"
)

func testRedacted(t *testing.T, source, expected string) {
	if r := redactMasterPassword(source); r != expected {
		t.Errorf("redactMasterPassword bad result: %v\nWas expecting:%v", r, expected)
	}
}

func TestRedactMasterPassword(t *testing.T) {

	// regular test case
	testRedacted(t, `CHANGE MASTER TO
  MASTER_PASSWORD = 'AAA',
  MASTER_CONNECT_RETRY = 1
`,
		`CHANGE MASTER TO
  MASTER_PASSWORD = '***',
  MASTER_CONNECT_RETRY = 1
`)

	// empty password
	testRedacted(t, `CHANGE MASTER TO
  MASTER_PASSWORD = '',
  MASTER_CONNECT_RETRY = 1
`,
		`CHANGE MASTER TO
  MASTER_PASSWORD = '',
  MASTER_CONNECT_RETRY = 1
`)

	// no beginning match
	testRedacted(t, "aaaaaaaaaaaaaa", "aaaaaaaaaaaaaa")

	// no end match
	testRedacted(t, `CHANGE MASTER TO
  MASTER_PASSWORD = 'AAA`, `CHANGE MASTER TO
  MASTER_PASSWORD = 'AAA`)
}
