// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package vindexes

import (
	"fmt"

	mproto "github.com/youtube/vitess/go/mysql/proto"
	"github.com/youtube/vitess/go/vt/key"
	tproto "github.com/youtube/vitess/go/vt/tabletserver/proto"
	"github.com/youtube/vitess/go/vt/vtgate/planbuilder"
)

var (
	_ planbuilder.Unique          = (*LookupHashUnique)(nil)
	_ planbuilder.LookupGenerator = (*LookupHashUnique)(nil)
)

// LookupHashUnique defines a vindex that uses a lookup table.
// The table is expected to define the id column as unique. It's
// Unique and a Lookup. It's also a LookupGenerator, because it
// can use the autoinc capabilities of the lookup table.
type LookupHashUnique struct {
	lookupHash
}

// NewLookupHashUnique creates a LookupHashUnique vindex.
func NewLookupHashUnique(m map[string]interface{}) (planbuilder.Vindex, error) {
	lhu := &LookupHashUnique{}
	lhu.init(m)
	return lhu, nil
}

// Cost returns the cost of this vindex as 10.
func (vind *LookupHashUnique) Cost() int {
	return 10
}

// Map returns the list of associated keyspace ids. It returns key.MinKey if there's no map.
func (vind *LookupHashUnique) Map(vcursor planbuilder.VCursor, ids []interface{}) ([]key.KeyspaceId, error) {
	out := make([]key.KeyspaceId, 0, len(ids))
	bq := &tproto.BoundQuery{
		Sql: vind.sel,
	}
	for _, id := range ids {
		bq.BindVariables = map[string]interface{}{
			vind.From: id,
		}
		result, err := vcursor.Execute(bq)
		if err != nil {
			return nil, err
		}
		if len(result.Rows) == 0 {
			out = append(out, "")
			continue
		}
		if len(result.Rows) != 1 {
			return nil, fmt.Errorf("unexpected multiple results from vindex %s: %v", vind.Table, id)
		}
		inum, err := mproto.Convert(result.Fields[0].Type, result.Rows[0][0])
		if err != nil {
			return nil, err
		}
		num, err := getNumber(inum)
		if err != nil {
			return nil, err
		}
		out = append(out, vhash(num))
	}
	return out, nil
}

// Generate generates an id and associates the ksid to the new id.
func (vind *LookupHashUnique) Generate(vcursor planbuilder.VCursor, ksid key.KeyspaceId) (id int64, err error) {
	bq := &tproto.BoundQuery{
		Sql: vind.ins,
		BindVariables: map[string]interface{}{
			vind.From: nil,
			vind.To:   vunhash(ksid),
		},
	}
	result, err := vcursor.Execute(bq)
	if err != nil {
		return 0, err
	}
	return int64(result.InsertId), err
}

func init() {
	planbuilder.Register("lookup_hash_unique", NewLookupHashUnique)
}
