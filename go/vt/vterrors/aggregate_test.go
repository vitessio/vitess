/*
Copyright 2017 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package vterrors

import (
	"errors"
	"fmt"
	"reflect"
	"testing"

	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

var errGeneric = "generic error"

func errFromCode(c vtrpcpb.Code) error {
	return New(c, errGeneric)
}

func TestAggregateVtGateErrorCodes(t *testing.T) {
	var testcases = []struct {
		input    []error
		expected vtrpcpb.Code
	}{
		{
			// aggregation of no errors is a success code
			input:    nil,
			expected: vtrpcpb.Code_OK,
		},
		{
			// aggregation of no errors is a success code
			input:    []error{nil, nil},
			expected: vtrpcpb.Code_OK,
		},
		{
			// single error code gets returned directly
			input:    []error{errFromCode(vtrpcpb.Code_INVALID_ARGUMENT)},
			expected: vtrpcpb.Code_INVALID_ARGUMENT,
		},
		{
			// aggregate two codes to the highest priority
			input: []error{
				errFromCode(vtrpcpb.Code_UNAVAILABLE),
				errFromCode(vtrpcpb.Code_INVALID_ARGUMENT),
			},
			expected: vtrpcpb.Code_INVALID_ARGUMENT,
		},
		{
			// aggregation handles nil positions
			input: []error{
				nil,
				errFromCode(vtrpcpb.Code_UNAVAILABLE),
				nil,
				nil,
				errFromCode(vtrpcpb.Code_INVALID_ARGUMENT),
				nil,
			},
			expected: vtrpcpb.Code_INVALID_ARGUMENT,
		},
		{
			// unknown errors map to the unknown code
			input: []error{
				fmt.Errorf("unknown error"),
			},
			expected: vtrpcpb.Code_UNKNOWN,
		},
	}
	for _, tc := range testcases {
		out := aggregateCodes(tc.input)
		if out != tc.expected {
			t.Errorf("AggregateVtGateErrorCodes(%v) = %v \nwant: %v",
				tc.input, out, tc.expected)
		}
	}
}

func TestAggregateVtGateErrors(t *testing.T) {
	var testcases = []struct {
		input    []error
		expected error
	}{
		{
			input:    nil,
			expected: nil,
		},
		{
			// aggregating an array of all nil errors is not an error
			input:    []error{nil, nil},
			expected: nil,
		},
		{
			input: []error{
				errFromCode(vtrpcpb.Code_UNAVAILABLE),
				errFromCode(vtrpcpb.Code_INVALID_ARGUMENT),
			},
			expected: New(
				vtrpcpb.Code_INVALID_ARGUMENT,
				aggregateErrors([]error{
					errors.New(errGeneric),
					errors.New(errGeneric),
				}),
			),
		},
		{
			// nil errors are ignored in the array
			input: []error{
				nil,
				errFromCode(vtrpcpb.Code_UNAVAILABLE),
				nil,
				nil,
				errFromCode(vtrpcpb.Code_INVALID_ARGUMENT),
				nil,
			},
			expected: New(
				vtrpcpb.Code_INVALID_ARGUMENT,
				aggregateErrors([]error{
					errors.New(errGeneric),
					errors.New(errGeneric),
				}),
			),
		},
	}
	for _, tc := range testcases {
		out := Aggregate(tc.input)
		if !reflect.DeepEqual(out, tc.expected) {
			t.Errorf("AggregateVtGateErrors(%+v) = %+v \nwant: %+v",
				tc.input, out, tc.expected)
		}
	}
}
