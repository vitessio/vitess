// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package vttest

import "testing"

func TestLocalLaunch(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode.")
	}

	defer LocalTeardown()
	err := LocalLaunch(
		[]string{"-80", "80-"},
		1,
		0,
		"test_keyspace",
		"create table a(id int, primary key(id))",
		`{"Keyspaces":{"test_keyspace":{"Tables":{"test_table":""}}}}`,
	)
	if err != nil {
		t.Error(err)
	}
}
