package engine

import (
	"sync"

	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/vt/concurrency"
	"github.com/youtube/vitess/go/vt/vterrors"
	"github.com/youtube/vitess/go/vt/vtgate/queryinfo"
)

// UnionAll specifies the parameters for a union primitive
type UnionAll struct {
	Left, Right Primitive `json:",omitempty"`
}

// Execute performs an execute
func (union *UnionAll) Execute(vcursor VCursor, queryConstruct *queryinfo.QueryConstruct, joinvars map[string]interface{}, wantfields bool) (*sqltypes.Result, error) {
	var lres, rres *sqltypes.Result
	allErrors := new(concurrency.AllErrorRecorder)
	var wg sync.WaitGroup
	wg.Add(1)
	go func(vc *VCursor, qc *queryinfo.QueryConstruct, jv map[string]interface{}, wf bool) {
		defer wg.Done()
		var err error
		lres, err = union.Left.Execute(*vc, qc, jv, wf)
		allErrors.RecordError(err)
	}(&vcursor, queryConstruct, joinvars, wantfields)
	wg.Add(1)
	go func(vc *VCursor, qc *queryinfo.QueryConstruct, jv map[string]interface{}, wf bool) {
		defer wg.Done()
		var err error
		rres, err = union.Right.Execute(*vc, qc, jv, wf)
		allErrors.RecordError(err)
	}(&vcursor, queryConstruct, joinvars, wantfields)
	wg.Wait()
	if allErrors.HasErrors() {
		return nil, allErrors.AggrError(vterrors.Aggregate)
	}
	// TODO(acharis): deal with column type mismatch
	lres.AppendResult(rres)
	return lres, nil
}

// StreamExecute performs a streaming execute
func (union *UnionAll) StreamExecute(vcursor VCursor, queryConstruct *queryinfo.QueryConstruct, joinvars map[string]interface{}, wantfields bool, callback func(*sqltypes.Result) error) error {
	allErrors := new(concurrency.AllErrorRecorder)
	var wg sync.WaitGroup
	wg.Add(1)
	go func(vc *VCursor, qc *queryinfo.QueryConstruct, jv map[string]interface{}, wf bool, cb func(*sqltypes.Result) error) {
		defer wg.Done()
		err := union.Left.StreamExecute(*vc, qc, jv, wf, cb)
		allErrors.RecordError(err)
	}(&vcursor, queryConstruct, joinvars, wantfields, callback)
	wg.Add(1)
	go func(vc *VCursor, qc *queryinfo.QueryConstruct, jv map[string]interface{}, wf bool, cb func(*sqltypes.Result) error) {
		defer wg.Done()
		err := union.Right.StreamExecute(*vc, qc, jv, wf, cb)
		allErrors.RecordError(err)
	}(&vcursor, queryConstruct, joinvars, wantfields, callback)
	wg.Wait()
	if allErrors.HasErrors() {
		return allErrors.AggrError(vterrors.Aggregate)
	}
	return nil
}

// GetFields gets the field information
// the fields of a union are the fields of the left query
func (union *UnionAll) GetFields(vcursor VCursor, queryConstruct *queryinfo.QueryConstruct, joinvars map[string]interface{}) (*sqltypes.Result, error) {
	return union.Left.GetFields(vcursor, queryConstruct, joinvars)
}
