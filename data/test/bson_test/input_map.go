// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package mytype

type MyType struct {
	Map          map[string]string
	MapBytes     map[string][]byte
	MapPtr       map[string]*string
	MapSlice     map[string][]string
	MapMap       map[string]map[string]int64
	MapCustom    map[string]Custom
	MapCustomPtr map[string]*Custom
	CustomMap    map[Custom]string
	MapExternal  map[pkg.Custom]string
}
