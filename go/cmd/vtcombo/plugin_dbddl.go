package main

import (
	"context"

	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/vtgate/engine"

	vttestpb "vitess.io/vitess/go/vt/proto/vttest"
)

var globalCreateDb func(ctx context.Context, ks *vttestpb.Keyspace) error
var globalDropDb func(ctx context.Context, ksName string) error

// DBDDL doesn't need to store any state - we use the global variables above instead
type DBDDL struct{}

// CreateDatabase implements the engine.DBDDLPlugin interface
func (plugin *DBDDL) CreateDatabase(ctx context.Context, name string) error {
	ks := &vttestpb.Keyspace{
		Name: name,
		Shards: []*vttestpb.Shard{{
			Name: "0",
		}},
	}
	return globalCreateDb(ctx, ks)
}

// DropDatabase implements the engine.DBDDLPlugin interface
func (plugin *DBDDL) DropDatabase(ctx context.Context, name string) error {
	return globalDropDb(ctx, name)
}

func init() {
	servenv.OnRun(func() {
		engine.DBDDLRegister("vttest", &DBDDL{})
	})
}
