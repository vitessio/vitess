package engine

import (
	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/srvtopo"
	"vitess.io/vitess/go/vt/vterrors"
)

var _ StreamExecutor = (*shardRoute)(nil)

// shardRoute is an internal primitive used by Route
// for performing merge sorts.
type shardRoute struct {
	query string
	rs    *srvtopo.ResolvedShard
	bv    map[string]*querypb.BindVariable
}

// StreamExecute performs a streaming exec.
func (sr *shardRoute) StreamExecute(vcursor VCursor, _ map[string]*querypb.BindVariable, _ bool, callback func(*sqltypes.Result) error) error {
	// TODO rollback on error and autocommit should probably not be used like this
	errors := vcursor.StreamExecuteMulti(sr.query, []*srvtopo.ResolvedShard{sr.rs}, []map[string]*querypb.BindVariable{sr.bv}, false /* rollbackOnError */, false /* autocommit */, callback)
	return vterrors.Aggregate(errors)
}
