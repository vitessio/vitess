package engine

import (
	"sync"

	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/vt/vtgate/queryinfo"
)

// UnionAll specifies the parameters for a union primitive
type UnionAll struct {
	Left, Right Primitive `json:",omitempty"`
}

// Execute performs an execute
func (union *UnionAll) Execute(vcursor VCursor, queryConstruct *queryinfo.QueryConstruct, joinvars map[string]interface{}, wantfields bool) (*sqltypes.Result, error) {
	var lres, rres *sqltypes.Result
	var lerr, rerr error
	var wg sync.WaitGroup
	wg.Add(1)
	go func(vc *VCursor, qc *queryinfo.QueryConstruct, jv map[string]interface{}, wf bool) {
		defer wg.Done()
		lres, lerr = union.Left.Execute(*vc, qc, jv, wf)
	}(&vcursor, queryConstruct, joinvars, wantfields)
	wg.Add(1)
	go func(vc *VCursor, qc *queryinfo.QueryConstruct, jv map[string]interface{}, wf bool) {
		defer wg.Done()
		rres, rerr = union.Right.Execute(*vc, qc, jv, wf)
	}(&vcursor, queryConstruct, joinvars, wantfields)
	wg.Wait()
	// TODO(acharis): possibly better to aggregate errors as in ScatterConn
	if lerr != nil {
		return nil, lerr
	}
	if rerr != nil {
		return nil, rerr
	}
	// TODO(acharis): deal with column type mismatch
	lres.AppendResult(rres)
	return lres, nil
}

// StreamExecute performs a streaming execute
func (union *UnionAll) StreamExecute(vcursor VCursor, queryConstruct *queryinfo.QueryConstruct, joinvars map[string]interface{}, wantfields bool, callback func(*sqltypes.Result) error) error {
	var lerr, rerr error
	var wg sync.WaitGroup
	wg.Add(1)
	go func(vc *VCursor, qc *queryinfo.QueryConstruct, jv map[string]interface{}, wf bool, cb func(*sqltypes.Result) error) {
		defer wg.Done()
		lerr = union.Left.StreamExecute(*vc, qc, jv, wf, cb)
	}(&vcursor, queryConstruct, joinvars, wantfields, callback)
	wg.Add(1)
	go func(vc *VCursor, qc *queryinfo.QueryConstruct, jv map[string]interface{}, wf bool, cb func(*sqltypes.Result) error) {
		defer wg.Done()
		rerr = union.Right.StreamExecute(*vc, qc, jv, wf, cb)
	}(&vcursor, queryConstruct, joinvars, wantfields, callback)
	wg.Wait()
	// TODO(acharis): possibly better to aggregate errors as in ScatterConn
	if lerr != nil {
		return lerr
	}
	if rerr != nil {
		return rerr
	}
	return nil
}

// GetFields gets the field information
// the fields of a union are the fields of the left query
func (union *UnionAll) GetFields(vcursor VCursor, queryConstruct *queryinfo.QueryConstruct, joinvars map[string]interface{}) (*sqltypes.Result, error) {
	return union.Left.GetFields(vcursor, queryConstruct, joinvars)
}
