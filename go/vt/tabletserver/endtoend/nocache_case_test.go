// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package endtoend

import (
	"testing"

	"github.com/youtube/vitess/go/vt/tabletserver/endtoend/framework"
)

var frameworkErrors = `union failed:
Result mismatch:
'[[1 1] [1 2]]' does not match
'[[2 1] [1 2]]'
RowsAffected mismatch: 2, want 1
Rewritten mismatch:
'[select eid, id from vtocc_a where 1 != 1 union select eid, id from vtocc_b where 1 != 1 select /* union */ eid, id from vtocc_a union select eid, id from vtocc_b]' does not match
'[select eid id from vtocc_a where 1 != 1 union select eid, id from vtocc_b where 1 != 1 select /* union */ eid, id from vtocc_a union select eid, id from vtocc_b]'
Plan mismatch: PASS_SELECT, want aa
Hits mismatch on table stats: 0, want 1
Hits mismatch on query info: 0, want 1
Misses mismatch on table stats: 0, want 2
Misses mismatch on query info: 0, want 2
Absent mismatch on table stats: 0, want 3
Absent mismatch on query info: 0, want 3`

func TestTheFramework(t *testing.T) {
	client := framework.NewDefaultClient()

	expectFail := framework.TestCase{
		Name:  "union",
		Query: "select /* union */ eid, id from vtocc_a union select eid, id from vtocc_b",
		Result: [][]string{
			[]string{"2", "1"},
			[]string{"1", "2"},
		},
		RowsAffected: 1,
		Rewritten: []string{
			"select eid id from vtocc_a where 1 != 1 union select eid, id from vtocc_b where 1 != 1",
			"select /* union */ eid, id from vtocc_a union select eid, id from vtocc_b",
		},
		Plan:   "aa",
		Table:  "bb",
		Hits:   1,
		Misses: 2,
		Absent: 3,
	}
	err := expectFail.Test("", client)
	if err == nil || err.Error() != frameworkErrors {
		t.Errorf("Framework result: \n%q\nexpecting\n%q", err.Error(), frameworkErrors)
	}
}
