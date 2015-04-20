// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package jscfg implements a simple API for reading and writing JSON files.
package jscfg

import (
	"encoding/json"
	"fmt"
	"io/ioutil"

	"github.com/youtube/vitess/go/ioutil2"
)

// ToJSON converts a structure to JSON, or panics
func ToJSON(val interface{}) string {
	data, err := json.MarshalIndent(val, "", "  ")
	// This is not strictly the spirit of panic. This is meant to be used
	// where it would be a programming error to have json encoding fail.
	if err != nil {
		panic(err)
	}
	return string(data)
}

// WriteJSON atomically write a marshaled structure to disk.
func WriteJSON(filename string, val interface{}) error {
	data, err := json.MarshalIndent(val, "  ", "  ")
	if err != nil {
		return fmt.Errorf("WriteJSON failed: %v %v", filename, err)
	}
	return ioutil2.WriteFileAtomic(filename, data, 0660)
}

// ReadJSON reads and unmarshals a JSON file
func ReadJSON(filename string, val interface{}) error {
	data, err := ioutil.ReadFile(filename)
	if err != nil {
		return fmt.Errorf("ReadJSON failed: %T %v", val, err)
	}
	if err = json.Unmarshal(data, val); err != nil {
		return fmt.Errorf("ReadJSON failed: %T %v %v", val, filename, err)
	}
	return nil
}
