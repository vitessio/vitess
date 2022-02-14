package vterrors

import (
	"errors"
	"fmt"
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
	}
	for _, tc := range testcases {
		out := Aggregate(tc.input)
		if !Equals(out, tc.expected) {
			t.Errorf("AggregateVtGateErrors(%+v) = %+v \nwant: %+v",
				tc.input, out, tc.expected)
		}
	}
}
