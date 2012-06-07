// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package stats

// StrFunc converts any function that returns a JSON string into
// an expvar.Var compatible object.
type StrFunc func() string

func (f StrFunc) String() string {
	return f()
}
