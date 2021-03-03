/*
Copyright 2019 The Vitess Authors.

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

package services

import (
	"strings"

	"context"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/vtgateservice"

	querypb "vitess.io/vitess/go/vt/proto/query"
	vtgatepb "vitess.io/vitess/go/vt/proto/vtgate"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

// errorClient implements vtgateservice.VTGateService
// and returns specific errors. It is meant to test all possible error cases,
// and make sure all clients handle the errors correctly.

const (
	// ErrorPrefix is the prefix to send with queries so they go through this service handler.
	ErrorPrefix = "error://"
	// PartialErrorPrefix is the prefix to send with queries so the RPC returns a partial error.
	// A partial error is when we return an error as part of the RPC response instead of via
	// the regular error channels. This occurs if an RPC partially succeeds, and therefore
	// requires some kind of response, but still needs to return an error.
	// VTGate Execute* calls do this: they always return a new session ID, but might also
	// return an error in the response.
	PartialErrorPrefix = "partialerror://"
)

type errorClient struct {
	fallbackClient
}

func newErrorClient(fallback vtgateservice.VTGateService) *errorClient {
	return &errorClient{
		fallbackClient: newFallbackClient(fallback),
	}
}

// requestToError returns an error for the given request, by looking at the
// request's prefix and requested error type. If the request doesn't match an
// error request, return nil.
func requestToError(request string) error {
	if !strings.HasPrefix(request, ErrorPrefix) {
		return nil
	}
	return trimmedRequestToError(strings.TrimPrefix(request, ErrorPrefix))
}

// requestToPartialError fills reply for a partial error if requested
// (that is, an error that may change the session).
// It returns true if a partial error was requested, false otherwise.
// This partial error should only be returned by Execute* calls.
func requestToPartialError(request string, session *vtgatepb.Session) error {
	if !strings.HasPrefix(request, PartialErrorPrefix) {
		return nil
	}
	request = strings.TrimPrefix(request, PartialErrorPrefix)
	parts := strings.Split(request, "/")
	if len(parts) > 1 && parts[1] == "close transaction" {
		session.InTransaction = false
	}
	return trimmedRequestToError(parts[0])
}

// trimmedRequestToError returns an error for a trimmed request by looking at the
// requested error type. It assumes that prefix checking has already been done.
// If the received string doesn't match a known error, returns an unknown error.
func trimmedRequestToError(received string) error {
	switch received {
	case "bad input":
		return vterrors.New(vtrpcpb.Code_INVALID_ARGUMENT, "vtgate test client forced error: bad input")
	case "deadline exceeded":
		return vterrors.New(vtrpcpb.Code_DEADLINE_EXCEEDED, "vtgate test client forced error: deadline exceeded")
	case "integrity error":
		return vterrors.New(vtrpcpb.Code_ALREADY_EXISTS, "vtgate test client forced error: integrity error (errno 1062) (sqlstate 23000)")
	// request backlog and general throttling type errors
	case "transient error":
		return vterrors.New(vtrpcpb.Code_UNAVAILABLE, "request_backlog: too many requests in flight: vtgate test client forced error: transient error")
	case "throttled error":
		return vterrors.New(vtrpcpb.Code_UNAVAILABLE, "request_backlog: exceeded XXX quota, rate limiting: vtgate test client forced error: transient error")
	case "unauthenticated":
		return vterrors.New(vtrpcpb.Code_UNAUTHENTICATED, "vtgate test client forced error: unauthenticated")
	case "aborted":
		return vterrors.New(vtrpcpb.Code_ABORTED, "vtgate test client forced error: aborted")
	case "query not served":
		return vterrors.New(vtrpcpb.Code_FAILED_PRECONDITION, "vtgate test client forced error: query not served")
	case "unknown error":
		return vterrors.New(vtrpcpb.Code_UNKNOWN, "vtgate test client forced error: unknown error")
	default:
		return vterrors.Errorf(vtrpcpb.Code_UNKNOWN, "vtgate test client error request unrecognized: %v", received)
	}
}

func (c *errorClient) Execute(ctx context.Context, session *vtgatepb.Session, sql string, bindVariables map[string]*querypb.BindVariable) (*vtgatepb.Session, *sqltypes.Result, error) {
	if err := requestToPartialError(sql, session); err != nil {
		return session, nil, err
	}
	if err := requestToError(sql); err != nil {
		return session, nil, err
	}
	return c.fallbackClient.Execute(ctx, session, sql, bindVariables)
}

func (c *errorClient) ExecuteBatch(ctx context.Context, session *vtgatepb.Session, sqlList []string, bindVariablesList []map[string]*querypb.BindVariable) (*vtgatepb.Session, []sqltypes.QueryResponse, error) {
	if len(sqlList) == 1 {
		if err := requestToPartialError(sqlList[0], session); err != nil {
			return session, nil, err
		}
		if err := requestToError(sqlList[0]); err != nil {
			return session, nil, err
		}
	}
	return c.fallbackClient.ExecuteBatch(ctx, session, sqlList, bindVariablesList)
}

func (c *errorClient) StreamExecute(ctx context.Context, session *vtgatepb.Session, sql string, bindVariables map[string]*querypb.BindVariable, callback func(*sqltypes.Result) error) error {
	if err := requestToError(sql); err != nil {
		return err
	}
	return c.fallbackClient.StreamExecute(ctx, session, sql, bindVariables, callback)
}
