// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package vterrors

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/youtube/vitess/go/vt/proto/vtrpc"
)

func TestToJSONError(t *testing.T) {
	var testcases = []struct {
		input    error
		expected error
	}{
		{
			input:    nil,
			expected: nil,
		},
		{
			// valid VtError
			input: &VitessError{
				Code: vtrpc.ErrorCode_BAD_INPUT,
				err:  fmt.Errorf("test vtgate error"),
			},
			expected: fmt.Errorf(`{"code":3,"message":"test vtgate error"}`),
		},
		{
			// error doesn't implement VtError
			input: fmt.Errorf("regular error"),
			// code UNKNOWN_ERROR
			expected: fmt.Errorf(`{"code":2,"message":"regular error"}`),
		},
		// TODO(aaijazi): add a test for something that causes json.Marshal to return an error.
	}
	for _, tc := range testcases {
		out := ToJSONError(tc.input)
		if !reflect.DeepEqual(out, tc.expected) {
			t.Errorf("ToJSONError(%+v) = %+v \nwant: %+v",
				tc.input, out, tc.expected)
		}
	}
}

func TestFromJSONError(t *testing.T) {
	invalidJSON := fmt.Errorf("error: invalid json")

	var testcases = []struct {
		input    error
		expected error
	}{
		{
			input:    nil,
			expected: nil,
		},
		{
			// valid JSON
			input: fmt.Errorf(`{"code":3,"message":"test vtgate error"}`),
			expected: &VitessError{
				Code: vtrpc.ErrorCode_BAD_INPUT,
				err:  fmt.Errorf("test vtgate error"),
			},
		},
		{
			// valid JSON, but doesn't match the JSON fields we expect
			input: fmt.Errorf(`{"ErrCode":3,"ErrorMessage":"test vtgate error"}`),
			expected: &VitessError{
				Code: vtrpc.ErrorCode_INTERNAL_ERROR,
				err:  fmt.Errorf(`unexpected fields in JSONError: {"ErrCode":3,"ErrorMessage":"test vtgate error"}`),
			},
		},
		{
			// invalid JSON
			input: invalidJSON,
			expected: &VitessError{
				Code:    vtrpc.ErrorCode_UNKNOWN_ERROR,
				Message: "can't unmarshal JSON: error: invalid json",
				err:     invalidJSON,
			},
		},
	}
	for _, tc := range testcases {
		out := FromJSONError(tc.input)
		if !reflect.DeepEqual(out, tc.expected) {
			t.Errorf("FromJSONError(%+v) = %+v \nwant: %+v",
				tc.input, out, tc.expected)
		}
	}
}
