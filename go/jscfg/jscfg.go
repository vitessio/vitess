// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package jscfg

// A few simple macros for keeping JSON config files human readable
// and consistent.

import (
	"encoding/json"
	"fmt"
	"io/ioutil"

	"code.google.com/p/vitess/go/ioutil2"
)

func ToJson(val interface{}) string {
	data, err := json.MarshalIndent(val, "", "  ")
	// This is not strictly the spirit of panic. This is meant to be used
	// where it would be a programming error to have json encoding fail.
	if err != nil {
		panic(err)
	}
	return string(data)
}

// Atomically write a marshaled structure to disk.
func WriteJson(filename string, val interface{}) error {
	data, err := json.MarshalIndent(val, "  ", "  ")
	if err != nil {
		return fmt.Errorf("WriteJson failed: %v %v", filename, err)
	}
	return ioutil2.WriteFileAtomic(filename, data, 0660)
}

func ReadJson(filename string, val interface{}) error {
	data, err := ioutil.ReadFile(filename)
	if err != nil {
		return fmt.Errorf("ReadJson failed: %T %v", val, err)
	}
	if err = json.Unmarshal(data, val); err != nil {
		return fmt.Errorf("ReadJson failed: %T %v %v", val, filename, err)
	}
	return nil
}
