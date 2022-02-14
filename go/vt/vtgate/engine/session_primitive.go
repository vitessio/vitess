package engine

import (
	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
)

// SessionPrimitive the session primitive is a very small primitive used
// when we have simple engine code that needs to interact with the Session
type SessionPrimitive struct {
	action func(sa SessionActions) (*sqltypes.Result, error)
	name   string

	noInputs
	noTxNeeded
}

var _ Primitive = (*SessionPrimitive)(nil)

// NewSessionPrimitive creates a SessionPrimitive
func NewSessionPrimitive(name string, action func(sa SessionActions) (*sqltypes.Result, error)) *SessionPrimitive {
	return &SessionPrimitive{
		action: action,
		name:   name,
	}
}

// RouteType implements the Primitive interface
func (s *SessionPrimitive) RouteType() string {
	return "SHOW"
}

// GetKeyspaceName implements the Primitive interface
func (s *SessionPrimitive) GetKeyspaceName() string {
	return ""
}

// GetTableName implements the Primitive interface
func (s *SessionPrimitive) GetTableName() string {
	return ""
}

// TryExecute implements the Primitive interface
func (s *SessionPrimitive) TryExecute(vcursor VCursor, _ map[string]*querypb.BindVariable, _ bool) (*sqltypes.Result, error) {
	return s.action(vcursor.Session())
}

// TryStreamExecute implements the Primitive interface
func (s *SessionPrimitive) TryStreamExecute(vcursor VCursor, _ map[string]*querypb.BindVariable, _ bool, callback func(*sqltypes.Result) error) error {
	qr, err := s.action(vcursor.Session())
	if err != nil {
		return err
	}
	return callback(qr)
}

// GetFields implements the Primitive interface
func (s *SessionPrimitive) GetFields(_ VCursor, _ map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	return nil, vterrors.New(vtrpcpb.Code_INTERNAL, "not supported for this primitive")
}

// description implements the Primitive interface
func (s *SessionPrimitive) description() PrimitiveDescription {
	return PrimitiveDescription{
		OperatorType: s.name,
	}
}
