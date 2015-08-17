// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package jscfg implements a simple API for reading and writing JSON files.
package jscfg

import "encoding/json"

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
