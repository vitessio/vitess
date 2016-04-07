package topo

import (
	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/vt/vtgate/vindexes"
)

// SaveVSchema first validates the VSchema, then sends it to the underlying
// Impl.
func (ts Server) SaveVSchema(ctx context.Context, keyspace, vschema string) error {
	err := vindexes.ValidateVSchema([]byte(vschema))
	if err != nil {
		return err
	}

	return ts.Impl.SaveVSchema(ctx, keyspace, vschema)
}
