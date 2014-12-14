// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package vindexes

import (
	mproto "github.com/youtube/vitess/go/mysql/proto"
	"github.com/youtube/vitess/go/vt/key"
	tproto "github.com/youtube/vitess/go/vt/tabletserver/proto"
	"github.com/youtube/vitess/go/vt/vtgate/planbuilder"
)

type LookupHashMulti struct {
	lookupHash
}

func NewLookupHashMulti(m map[string]interface{}) (planbuilder.Vindex, error) {
	lhm := &LookupHashMulti{}
	lhm.init(m)
	return lhm, nil
}

func (vind *LookupHashMulti) Cost() int {
	return 20
}

func (vind *LookupHashMulti) Map(vcursor planbuilder.VCursor, ids []interface{}) ([][]key.KeyspaceId, error) {
	out := make([][]key.KeyspaceId, 0, len(ids))
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
		var ksids []key.KeyspaceId
		for _, row := range result.Rows {
			inum, err := mproto.Convert(result.Fields[0].Type, row[0])
			if err != nil {
				return nil, err
			}
			num, err := getNumber(inum)
			if err != nil {
				return nil, err
			}
			ksids = append(ksids, vhash(num))
		}
		out = append(out, ksids)
	}
	return out, nil
}

func init() {
	planbuilder.Register("lookup_hash_multi", NewLookupHashMulti)
}
