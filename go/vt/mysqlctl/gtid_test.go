// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package mysqlctl

import (
	"github.com/youtube/vitess/go/vt/mysqlctl/proto"
)

// fakeGTID is used in the mysql_flavor_*_test.go files.
type fakeGTID struct {
	flavor, value string
}

func (f fakeGTID) String() string                   { return f.value }
func (f fakeGTID) Flavor() string                   { return f.flavor }
func (fakeGTID) TryCompare(proto.GTID) (int, error) { return 0, nil }
