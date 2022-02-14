package fakes

import (
	"fmt"

	"context"

	"vitess.io/vitess/go/vt/vttablet/queryservice"

	querypb "vitess.io/vitess/go/vt/proto/query"
)

// ErrorQueryService is an object that returns an error for all methods.
var ErrorQueryService = queryservice.Wrap(
	nil,
	func(ctx context.Context, target *querypb.Target, conn queryservice.QueryService, name string, inTransaction bool, inner func(context.Context, *querypb.Target, queryservice.QueryService) (bool, error)) error {
		return fmt.Errorf("ErrorQueryService does not implement any method")
	},
)
