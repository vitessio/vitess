// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package vtgate

import (
	"errors"
	"fmt"
	"reflect"
	"testing"

	vtrpcpb "github.com/youtube/vitess/go/vt/proto/vtrpc"
	"github.com/youtube/vitess/go/vt/vterrors"
)

var errGeneric = errors.New("generic error")

func errFromCode(c vtrpcpb.ErrorCode) error {
	return vterrors.FromError(c, errGeneric)
}

func TestAggregateVtGateErrorCodes(t *testing.T) {
	var testcases = []struct {
		input    []error
		expected vtrpcpb.ErrorCode
	}{
		{
			// aggregation of no errors is a success code
			input:    nil,
			expected: vtrpcpb.ErrorCode_SUCCESS,
		},
		{
			// single error code gets returned directly
			input:    []error{errFromCode(vtrpcpb.ErrorCode_BAD_INPUT)},
			expected: vtrpcpb.ErrorCode_BAD_INPUT,
		},
		{
			// aggregate two codes to the highest priority
			input: []error{
				errFromCode(vtrpcpb.ErrorCode_SUCCESS),
				errFromCode(vtrpcpb.ErrorCode_TRANSIENT_ERROR),
			},
			expected: vtrpcpb.ErrorCode_TRANSIENT_ERROR,
		},
		{
			input: []error{
				errFromCode(vtrpcpb.ErrorCode_SUCCESS),
				errFromCode(vtrpcpb.ErrorCode_TRANSIENT_ERROR),
				errFromCode(vtrpcpb.ErrorCode_BAD_INPUT),
			},
			expected: vtrpcpb.ErrorCode_BAD_INPUT,
		},
		{
			// unknown errors map to the unknown code
			input: []error{
				errFromCode(vtrpcpb.ErrorCode_SUCCESS),
				fmt.Errorf("unknown error"),
			},
			expected: vtrpcpb.ErrorCode_UNKNOWN_ERROR,
		},
	}
	for _, tc := range testcases {
		out := aggregateVtGateErrorCodes(tc.input)
		if out != tc.expected {
			t.Errorf("aggregateVtGateErrorCodes(%v) = %v \nwant: %v",
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
			input: []error{
				errFromCode(vtrpcpb.ErrorCode_SUCCESS),
				errFromCode(vtrpcpb.ErrorCode_TRANSIENT_ERROR),
				errFromCode(vtrpcpb.ErrorCode_BAD_INPUT),
			},
			expected: vterrors.FromError(
				vtrpcpb.ErrorCode_BAD_INPUT,
				vterrors.ConcatenateErrors([]error{errGeneric, errGeneric, errGeneric}),
			),
		},
	}
	for _, tc := range testcases {
		out := AggregateVtGateErrors(tc.input)
		if !reflect.DeepEqual(out, tc.expected) {
			t.Errorf("aggregateVtGateErrors(%+v) = %+v \nwant: %+v",
				tc.input, out, tc.expected)
		}
	}
}
