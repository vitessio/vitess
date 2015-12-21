// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package simpleacl

import (
	"testing"

	"github.com/youtube/vitess/go/vt/tableacl/testlib"
)

func TestSimpleAcl(t *testing.T) {
	testlib.TestSuite(t, &Factory{})
}
