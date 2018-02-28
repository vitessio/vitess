// Copyright 2016, Google Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// AUTO-GENERATED CODE. DO NOT EDIT.

package debugger_test

import (
	"cloud.google.com/go/debugger/apiv2"
	"golang.org/x/net/context"
	clouddebuggerpb "google.golang.org/genproto/googleapis/devtools/clouddebugger/v2"
)

func ExampleNewController2Client() {
	ctx := context.Background()
	c, err := debugger.NewController2Client(ctx)
	if err != nil {
		// TODO: Handle error.
	}
	// TODO: Use client.
	_ = c
}

func ExampleController2Client_RegisterDebuggee() {
	ctx := context.Background()
	c, err := debugger.NewController2Client(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &clouddebuggerpb.RegisterDebuggeeRequest{
	// TODO: Fill request struct fields.
	}
	resp, err := c.RegisterDebuggee(ctx, req)
	if err != nil {
		// TODO: Handle error.
	}
	// TODO: Use resp.
	_ = resp
}

func ExampleController2Client_ListActiveBreakpoints() {
	ctx := context.Background()
	c, err := debugger.NewController2Client(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &clouddebuggerpb.ListActiveBreakpointsRequest{
	// TODO: Fill request struct fields.
	}
	resp, err := c.ListActiveBreakpoints(ctx, req)
	if err != nil {
		// TODO: Handle error.
	}
	// TODO: Use resp.
	_ = resp
}

func ExampleController2Client_UpdateActiveBreakpoint() {
	ctx := context.Background()
	c, err := debugger.NewController2Client(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &clouddebuggerpb.UpdateActiveBreakpointRequest{
	// TODO: Fill request struct fields.
	}
	resp, err := c.UpdateActiveBreakpoint(ctx, req)
	if err != nil {
		// TODO: Handle error.
	}
	// TODO: Use resp.
	_ = resp
}
