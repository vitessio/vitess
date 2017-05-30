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
	"reflect"
	"testing"

	"github.com/golang/protobuf/proto"

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
		want: New(vtrpcpb.Code_INVALID_ARGUMENT, "bad input"),
	}, {
		in: &vtrpcpb.RPCError{
			LegacyCode: vtrpcpb.LegacyErrorCode_BAD_INPUT_LEGACY,
			Message:    "bad input",
			Code:       vtrpcpb.Code_INVALID_ARGUMENT,
		},
		want: New(vtrpcpb.Code_INVALID_ARGUMENT, "bad input"),
	}, {
		in: &vtrpcpb.RPCError{
			Message: "bad input",
			Code:    vtrpcpb.Code_INVALID_ARGUMENT,
		},
		want: New(vtrpcpb.Code_INVALID_ARGUMENT, "bad input"),
	}}
	for _, tcase := range testcases {
		got := FromVTRPC(tcase.in)
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
		in: New(vtrpcpb.Code_INVALID_ARGUMENT, "bad input"),
		want: &vtrpcpb.RPCError{
			LegacyCode: vtrpcpb.LegacyErrorCode_BAD_INPUT_LEGACY,
			Message:    "bad input",
			Code:       vtrpcpb.Code_INVALID_ARGUMENT,
		},
	}}
	for _, tcase := range testcases {
		got := ToVTRPC(tcase.in)
		if !proto.Equal(got, tcase.want) {
			t.Errorf("VtRPCErrorFromVtError(%v): %v, want %v", tcase.in, got, tcase.want)
		}
	}
}
