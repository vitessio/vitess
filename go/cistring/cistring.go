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
// It precomputes and stores the lower case version of the string
// internally. This increases the initial memory cost of the object
// but saves the CPU (and memory) cost of lowercasing as needed.
// This should generally trade off favorably because there are many
// situations where comparisons are performed in a loop against
// the same object.
type CIString struct {
	// This artifact prevents this struct from being compared
	// with itself. It consumes no space as long as it's not the
	// last field in the struct.
	_            [0]struct{ notComparable []byte }
	val, lowered string
}

// New creates a new CIString.
func New(str string) CIString {
	return CIString{
		val:     str,
		lowered: strings.ToLower(str),
	}
}

func (s CIString) String() string {
	return s.val
}

// Original returns the case-preserved value of the string.
func (s CIString) Original() string {
	return s.val
}

// Lowered returns the lower-case value of the string.
// This function should generally be used only for optimizing
// comparisons.
func (s CIString) Lowered() string {
	return s.lowered
}

// Equal performs a case-insensitive compare. For comparing
// in a loop, it's beneficial to build a CIString outside
// the loop and using it to compare with other CIString
// variables inside the loop.
func (s CIString) Equal(in CIString) bool {
	return s.lowered == in.lowered
}

// EqualString performs a case-insensitive compare with str.
// If the input is already lower-cased, it's more efficient
// to check if s.Lowered()==in.
func (s CIString) EqualString(in string) bool {
	return s.lowered == strings.ToLower(in)
}

// MarshalJSON marshals into JSON.
func (s CIString) MarshalJSON() ([]byte, error) {
	return json.Marshal(s.val)
}

// UnmarshalJSON unmarshals from JSON.
func (s *CIString) UnmarshalJSON(b []byte) error {
	var result string
	err := json.Unmarshal(b, &result)
	if err != nil {
		return err
	}
	s.val = result
	s.lowered = strings.ToLower(result)
	return nil
}

// ToStrings converts a []CIString to a case-preserved
// []string.
func ToStrings(in []CIString) []string {
	s := make([]string, len(in))
	for i := 0; i < len(in); i++ {
		s[i] = in[i].Original()
	}
	return s
}
