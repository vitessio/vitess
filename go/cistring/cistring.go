// Copyright 2016, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package cistring implements a case-insensitive string type.
package cistring

import (
	"encoding/json"
	"strings"
)

// CIString is an immutable case-insensitive string.
type CIString struct {
	val, lowered string
	// nocompare prevents this struct from being compared
	// with itself.
	nocompare func()
}

// NewCIString creates a new CIString.
func NewCIString(str string) CIString {
	return CIString{
		val:     str,
		lowered: strings.ToLower(str),
	}
}

func (s CIString) String() string {
	return s.val
}

// Val returns the case-preserved value of the string.
func (s CIString) Val() string {
	return s.val
}

// Lowered returns the lower-case value of the string.
func (s CIString) Lowered() string {
	return s.lowered
}

// Equal returns true if the input is case-insensitive
// equal to the string. If the input is already lower-cased,
// it's more efficient to check if s.Lowered()==in.
func (s CIString) Equal(in string) bool {
	return s.lowered == strings.ToLower(in)
}

// MarshalJSON marshals into JSON.
func (s CIString) MarshalJSON() ([]byte, error) {
	return json.Marshal(s.val)
}
