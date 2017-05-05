/*
Copyright 2017 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreedto in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package vterrors

import (
	"errors"
	"testing"

	"golang.org/x/net/context"

	vtrpcpb "github.com/youtube/vitess/go/vt/proto/vtrpc"
)

func TestCreation(t *testing.T) {
	testcases := []struct {
		in, want vtrpcpb.Code
	}{{
		in:   vtrpcpb.Code_CANCELED,
		want: vtrpcpb.Code_CANCELED,
	}, {
		in:   vtrpcpb.Code_UNKNOWN,
		want: vtrpcpb.Code_UNKNOWN,
	}, {
		// Invalid code OK should be converted to INTERNAL.
		in:   vtrpcpb.Code_OK,
		want: vtrpcpb.Code_INTERNAL,
	}}
	for _, tcase := range testcases {
		if got := Code(New(tcase.in, "")); got != tcase.want {
			t.Errorf("Code(New(%v)): %v, want %v", tcase.in, got, tcase.want)
		}
		if got := Code(Errorf(tcase.in, "")); got != tcase.want {
			t.Errorf("Code(Errorf(%v)): %v, want %v", tcase.in, got, tcase.want)
		}
	}
}

func TestCode(t *testing.T) {
	testcases := []struct {
		in   error
		want vtrpcpb.Code
	}{{
		in:   nil,
		want: vtrpcpb.Code_OK,
	}, {
		in:   errors.New("generic"),
		want: vtrpcpb.Code_UNKNOWN,
	}, {
		in:   New(vtrpcpb.Code_CANCELED, "generic"),
		want: vtrpcpb.Code_CANCELED,
	}, {
		in:   context.Canceled,
		want: vtrpcpb.Code_CANCELED,
	}, {
		in:   context.DeadlineExceeded,
		want: vtrpcpb.Code_DEADLINE_EXCEEDED,
	}}
	for _, tcase := range testcases {
		if got := Code(tcase.in); got != tcase.want {
			t.Errorf("Code(%v): %v, want %v", tcase.in, got, tcase.want)
		}
	}
}
