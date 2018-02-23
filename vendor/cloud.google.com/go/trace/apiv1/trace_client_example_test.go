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

package trace_test

import (
	"cloud.google.com/go/trace/apiv1"
	"golang.org/x/net/context"
	cloudtracepb "google.golang.org/genproto/googleapis/devtools/cloudtrace/v1"
)

func ExampleNewClient() {
	ctx := context.Background()
	c, err := trace.NewClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}
	// TODO: Use client.
	_ = c
}

func ExampleClient_PatchTraces() {
	ctx := context.Background()
	c, err := trace.NewClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &cloudtracepb.PatchTracesRequest{
	// TODO: Fill request struct fields.
	}
	err = c.PatchTraces(ctx, req)
	if err != nil {
		// TODO: Handle error.
	}
}

func ExampleClient_GetTrace() {
	ctx := context.Background()
	c, err := trace.NewClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &cloudtracepb.GetTraceRequest{
	// TODO: Fill request struct fields.
	}
	resp, err := c.GetTrace(ctx, req)
	if err != nil {
		// TODO: Handle error.
	}
	// TODO: Use resp.
	_ = resp
}

func ExampleClient_ListTraces() {
	ctx := context.Background()
	c, err := trace.NewClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &cloudtracepb.ListTracesRequest{
	// TODO: Fill request struct fields.
	}
	it := c.ListTraces(ctx, req)
	for {
		resp, err := it.Next()
		if err != nil {
			// TODO: Handle error.
			break
		}
		// TODO: Use resp.
		_ = resp
	}
}
