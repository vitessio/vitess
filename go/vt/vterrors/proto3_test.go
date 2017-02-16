// Copyright 2017, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package vterrors

import (
	"errors"
	"reflect"
	"testing"

	vtrpcpb "github.com/youtube/vitess/go/vt/proto/vtrpc"
)

func TestFromVtRPCError(t *testing.T) {
	testcases := []struct {
		in   *vtrpcpb.RPCError
		want error
	}{{
		in:   nil,
		want: nil,
	}, {
		in: &vtrpcpb.RPCError{
			LegacyCode: vtrpcpb.LegacyErrorCode_BAD_INPUT_LEGACY,
			Message:    "bad input",
		},
		want: &VitessError{
			Code: vtrpcpb.Code_INVALID_ARGUMENT,
			err:  errors.New("bad input"),
		},
	}, {
		in: &vtrpcpb.RPCError{
			LegacyCode: vtrpcpb.LegacyErrorCode_BAD_INPUT_LEGACY,
			Message:    "bad input",
			Code:       vtrpcpb.Code_INVALID_ARGUMENT,
		},
		want: &VitessError{
			Code: vtrpcpb.Code_INVALID_ARGUMENT,
			err:  errors.New("bad input"),
		},
	}, {
		in: &vtrpcpb.RPCError{
			Message: "bad input",
			Code:    vtrpcpb.Code_INVALID_ARGUMENT,
		},
		want: &VitessError{
			Code: vtrpcpb.Code_INVALID_ARGUMENT,
			err:  errors.New("bad input"),
		},
	}}
	for _, tcase := range testcases {
		got := FromVtRPCError(tcase.in)
		if !reflect.DeepEqual(got, tcase.want) {
			t.Errorf("FromVtRPCError(%v): %v, want %v", tcase.in, got, tcase.want)
		}
	}
}

func TestVtRPCErrorFromVtError(t *testing.T) {
	testcases := []struct {
		in   error
		want *vtrpcpb.RPCError
	}{{
		in:   nil,
		want: nil,
	}, {
		in: &VitessError{
			Code: vtrpcpb.Code_INVALID_ARGUMENT,
			err:  errors.New("bad input"),
		},
		want: &vtrpcpb.RPCError{
			LegacyCode: vtrpcpb.LegacyErrorCode_BAD_INPUT_LEGACY,
			Message:    "bad input",
			Code:       vtrpcpb.Code_INVALID_ARGUMENT,
		},
	}}
	for _, tcase := range testcases {
		got := VtRPCErrorFromVtError(tcase.in)
		if !reflect.DeepEqual(got, tcase.want) {
			t.Errorf("VtRPCErrorFromVtError(%v): %v, want %v", tcase.in, got, tcase.want)
		}
	}
}
