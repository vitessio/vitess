// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package jscfg

// A few simple macros for keeping JSON config files human readable
// and consistent.

import (
	"encoding/json"
	"fmt"

	"code.google.com/p/vitess/go/ioutil2"
)

func ToJson(x interface{}) string {
	data, err := json.MarshalIndent(x, "  ", "  ")
	// This is not strictly the spirit of panic. This is meant to be used
	// where it would be a programming error to have json encoding fail.
	if err != nil {
		panic(err)
	}
	return string(data)
}

// Atomically write a marshaled structure to disk.
func WriteJson(filename string, x interface{}) error {
	data, err := json.MarshalIndent(x, "  ", "  ")
	if err != nil {
		return fmt.Errorf("WriteJson fialed: %v %v", filename, err)
	}
	return ioutil2.WriteFileAtomic(filename, data, 0660)
}
