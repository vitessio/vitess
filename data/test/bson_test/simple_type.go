// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package mytype

type MyType struct {
	Float64 float64
	String  string
	Bool    bool
	Int64   int64
	Int32   int32
	Int     int
	Uint64  uint64
	Uint32  uint32
	Uint    uint
	Bytes   []byte
	Ptr     *int64
	Strings *[]*string
	Map     map[string]int64
}
