package engine

import (
	"context"
	"time"

	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/vterrors"
)

// TimeoutHandler is a primitive that adds a timeout to the execution of a query.
type TimeoutHandler struct {
	Timeout int
	Input   Primitive
}

var _ Primitive = (*TimeoutHandler)(nil)

// NewTimeoutHandler creates a new timeout handler.
func NewTimeoutHandler(input Primitive, timeout int) *TimeoutHandler {
	return &TimeoutHandler{
		Timeout: timeout,
		Input:   input,
	}
}

// RouteType is part of the Primitive interface
func (t *TimeoutHandler) RouteType() string {
	return t.Input.RouteType()
}

// GetKeyspaceName is part of the Primitive interface
func (t *TimeoutHandler) GetKeyspaceName() string {
	return t.Input.GetKeyspaceName()
}

// GetTableName is part of the Primitive interface
func (t *TimeoutHandler) GetTableName() string {
	return t.Input.GetTableName()
}

// GetFields is part of the Primitive interface
func (t *TimeoutHandler) GetFields(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	return t.Input.GetFields(ctx, vcursor, bindVars)
}

// NeedsTransaction is part of the Primitive interface
func (t *TimeoutHandler) NeedsTransaction() bool {
	return t.Input.NeedsTransaction()
}

// TryExecute is part of the Primitive interface
func (t *TimeoutHandler) TryExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool) (res *sqltypes.Result, err error) {
	ctx, cancel := context.WithTimeout(ctx, time.Duration(t.Timeout)*time.Millisecond)
	defer cancel()

	complete := make(chan any)
	go func() {
		res, err = t.Input.TryExecute(ctx, vcursor, bindVars, wantfields)
		close(complete)
	}()

	select {
	case <-ctx.Done():
		return nil, vterrors.VT15001()
	case <-complete:
		return res, err
	}
}

// TryStreamExecute is part of the Primitive interface
func (t *TimeoutHandler) TryStreamExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool, callback func(*sqltypes.Result) error) (err error) {
	ctx, cancel := context.WithTimeout(ctx, time.Duration(t.Timeout)*time.Millisecond)
	defer cancel()

	complete := make(chan any)
	go func() {
		err = t.Input.TryStreamExecute(ctx, vcursor, bindVars, wantfields, callback)
		close(complete)
	}()

	select {
	case <-ctx.Done():
		return vterrors.VT15001()
	case <-complete:
		return err
	}
}

// Inputs is part of the Primitive interface
func (t *TimeoutHandler) Inputs() ([]Primitive, []map[string]any) {
	return []Primitive{t.Input}, nil
}

// description is part of the Primitive interface
func (t *TimeoutHandler) description() PrimitiveDescription {
	return PrimitiveDescription{
		OperatorType: "TimeoutHandler",
		Other: map[string]any{
			"Timeout": t.Timeout,
		},
	}
}
