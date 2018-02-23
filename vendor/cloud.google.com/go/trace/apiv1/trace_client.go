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

package trace

import (
	"fmt"
	"math"
	"runtime"
	"strings"
	"time"

	gax "github.com/googleapis/gax-go"
	"golang.org/x/net/context"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
	"google.golang.org/api/transport"
	cloudtracepb "google.golang.org/genproto/googleapis/devtools/cloudtrace/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
)

// CallOptions contains the retry settings for each method of Client.
type CallOptions struct {
	PatchTraces []gax.CallOption
	GetTrace    []gax.CallOption
	ListTraces  []gax.CallOption
}

func defaultClientOptions() []option.ClientOption {
	return []option.ClientOption{
		option.WithEndpoint("cloudtrace.googleapis.com:443"),
		option.WithScopes(
			"https://www.googleapis.com/auth/cloud-platform",
			"https://www.googleapis.com/auth/trace.append",
			"https://www.googleapis.com/auth/trace.readonly",
		),
	}
}

func defaultCallOptions() *CallOptions {
	retry := map[[2]string][]gax.CallOption{
		{"default", "idempotent"}: {
			gax.WithRetry(func() gax.Retryer {
				return gax.OnCodes([]codes.Code{
					codes.DeadlineExceeded,
					codes.Unavailable,
				}, gax.Backoff{
					Initial:    100 * time.Millisecond,
					Max:        1000 * time.Millisecond,
					Multiplier: 1.2,
				})
			}),
		},
	}
	return &CallOptions{
		PatchTraces: retry[[2]string{"default", "idempotent"}],
		GetTrace:    retry[[2]string{"default", "idempotent"}],
		ListTraces:  retry[[2]string{"default", "idempotent"}],
	}
}

// Client is a client for interacting with Stackdriver Trace API.
type Client struct {
	// The connection to the service.
	conn *grpc.ClientConn

	// The gRPC API client.
	client cloudtracepb.TraceServiceClient

	// The call options for this service.
	CallOptions *CallOptions

	// The metadata to be sent with each request.
	metadata metadata.MD
}

// NewClient creates a new trace service client.
//
// This file describes an API for collecting and viewing traces and spans
// within a trace.  A Trace is a collection of spans corresponding to a single
// operation or set of operations for an application. A span is an individual
// timed event which forms a node of the trace tree. Spans for a single trace
// may span multiple services.
func NewClient(ctx context.Context, opts ...option.ClientOption) (*Client, error) {
	conn, err := transport.DialGRPC(ctx, append(defaultClientOptions(), opts...)...)
	if err != nil {
		return nil, err
	}
	c := &Client{
		conn:        conn,
		CallOptions: defaultCallOptions(),

		client: cloudtracepb.NewTraceServiceClient(conn),
	}
	c.SetGoogleClientInfo("gax", gax.Version)
	return c, nil
}

// Connection returns the client's connection to the API service.
func (c *Client) Connection() *grpc.ClientConn {
	return c.conn
}

// Close closes the connection to the API service. The user should invoke this when
// the client is no longer required.
func (c *Client) Close() error {
	return c.conn.Close()
}

// SetGoogleClientInfo sets the name and version of the application in
// the `x-goog-api-client` header passed on each request. Intended for
// use by Google-written clients.
func (c *Client) SetGoogleClientInfo(name, version string) {
	goVersion := strings.Replace(runtime.Version(), " ", "_", -1)
	v := fmt.Sprintf("%s/%s %s gax/%s go/%s", name, version, gapicNameVersion, gax.Version, goVersion)
	c.metadata = metadata.Pairs("x-goog-api-client", v)
}

// PatchTraces sends new traces to Stackdriver Trace or updates existing traces. If the ID
// of a trace that you send matches that of an existing trace, any fields
// in the existing trace and its spans are overwritten by the provided values,
// and any new fields provided are merged with the existing trace data. If the
// ID does not match, a new trace is created.
func (c *Client) PatchTraces(ctx context.Context, req *cloudtracepb.PatchTracesRequest) error {
	md, _ := metadata.FromContext(ctx)
	ctx = metadata.NewContext(ctx, metadata.Join(md, c.metadata))
	err := gax.Invoke(ctx, func(ctx context.Context) error {
		var err error
		_, err = c.client.PatchTraces(ctx, req)
		return err
	}, c.CallOptions.PatchTraces...)
	return err
}

// GetTrace gets a single trace by its ID.
func (c *Client) GetTrace(ctx context.Context, req *cloudtracepb.GetTraceRequest) (*cloudtracepb.Trace, error) {
	md, _ := metadata.FromContext(ctx)
	ctx = metadata.NewContext(ctx, metadata.Join(md, c.metadata))
	var resp *cloudtracepb.Trace
	err := gax.Invoke(ctx, func(ctx context.Context) error {
		var err error
		resp, err = c.client.GetTrace(ctx, req)
		return err
	}, c.CallOptions.GetTrace...)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// ListTraces returns of a list of traces that match the specified filter conditions.
func (c *Client) ListTraces(ctx context.Context, req *cloudtracepb.ListTracesRequest) *TraceIterator {
	md, _ := metadata.FromContext(ctx)
	ctx = metadata.NewContext(ctx, metadata.Join(md, c.metadata))
	it := &TraceIterator{}
	it.InternalFetch = func(pageSize int, pageToken string) ([]*cloudtracepb.Trace, string, error) {
		var resp *cloudtracepb.ListTracesResponse
		req.PageToken = pageToken
		if pageSize > math.MaxInt32 {
			req.PageSize = math.MaxInt32
		} else {
			req.PageSize = int32(pageSize)
		}
		err := gax.Invoke(ctx, func(ctx context.Context) error {
			var err error
			resp, err = c.client.ListTraces(ctx, req)
			return err
		}, c.CallOptions.ListTraces...)
		if err != nil {
			return nil, "", err
		}
		return resp.Traces, resp.NextPageToken, nil
	}
	fetch := func(pageSize int, pageToken string) (string, error) {
		items, nextPageToken, err := it.InternalFetch(pageSize, pageToken)
		if err != nil {
			return "", err
		}
		it.items = append(it.items, items...)
		return nextPageToken, nil
	}
	it.pageInfo, it.nextFunc = iterator.NewPageInfo(fetch, it.bufLen, it.takeBuf)
	return it
}

// TraceIterator manages a stream of *cloudtracepb.Trace.
type TraceIterator struct {
	items    []*cloudtracepb.Trace
	pageInfo *iterator.PageInfo
	nextFunc func() error

	// InternalFetch is for use by the Google Cloud Libraries only.
	// It is not part of the stable interface of this package.
	//
	// InternalFetch returns results from a single call to the underlying RPC.
	// The number of results is no greater than pageSize.
	// If there are no more results, nextPageToken is empty and err is nil.
	InternalFetch func(pageSize int, pageToken string) (results []*cloudtracepb.Trace, nextPageToken string, err error)
}

// PageInfo supports pagination. See the google.golang.org/api/iterator package for details.
func (it *TraceIterator) PageInfo() *iterator.PageInfo {
	return it.pageInfo
}

// Next returns the next result. Its second return value is iterator.Done if there are no more
// results. Once Next returns Done, all subsequent calls will return Done.
func (it *TraceIterator) Next() (*cloudtracepb.Trace, error) {
	var item *cloudtracepb.Trace
	if err := it.nextFunc(); err != nil {
		return item, err
	}
	item = it.items[0]
	it.items = it.items[1:]
	return item, nil
}

func (it *TraceIterator) bufLen() int {
	return len(it.items)
}

func (it *TraceIterator) takeBuf() interface{} {
	b := it.items
	it.items = nil
	return b
}
