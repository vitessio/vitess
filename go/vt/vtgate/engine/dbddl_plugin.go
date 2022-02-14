package engine

import (
	"context"

	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
)

type failDBDDL struct{}

// CreateDatabase implements the DropCreateDB interface
func (failDBDDL) CreateDatabase(context.Context, string) error {
	return vterrors.New(vtrpcpb.Code_UNIMPLEMENTED, "create database is not supported")
}

// DropDatabase implements the DropCreateDB interface
func (failDBDDL) DropDatabase(context.Context, string) error {
	return vterrors.New(vtrpcpb.Code_UNIMPLEMENTED, "drop database is not supported")
}

type noOp struct{}

// CreateDatabase implements the DropCreateDB interface
func (noOp) CreateDatabase(context.Context, string) error {
	return nil
}

// DropDatabase implements the DropCreateDB interface
func (noOp) DropDatabase(context.Context, string) error {
	return nil
}

const (
	faildbDDL          = "fail"
	noOpdbDDL          = "noop"
	defaultDBDDLPlugin = faildbDDL
)

func init() {
	DBDDLRegister(faildbDDL, failDBDDL{})
	DBDDLRegister(noOpdbDDL, noOp{})
}
