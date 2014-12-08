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

type LookupHashUnique struct {
	Table, From, To       string
	sel, verify, ins, del string
}

func NewLookupHashUnique(m map[string]interface{}) (planbuilder.Vindex, error) {
	get := func(name string) string {
		v, _ := m[name].(string)
		return v
	}
	t := get("Table")
	from := get("From")
	to := get("To")
	return &LookupHashUnique{
		Table:  t,
		From:   from,
		To:     to,
		sel:    fmt.Sprintf("select %s from %s where %s = :%s", to, t, from, from),
		verify: fmt.Sprintf("select %s from %s where %s = :%s and %s = :%s", from, t, from, from, to, to),
		ins:    fmt.Sprintf("insert into %s(%s, %s) values(:%s, :%s)", t, from, to, from, to),
		del:    fmt.Sprintf("delete from %s where %s = :%s and %s = :%s", t, from, from, to, to),
	}, nil
}

func (vind *LookupHashUnique) Cost() int {
	return 10
}

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

func (vind *LookupHashUnique) Verify(vcursor planbuilder.VCursor, id interface{}, ksid key.KeyspaceId) (bool, error) {
	bq := &tproto.BoundQuery{
		Sql: vind.verify,
		BindVariables: map[string]interface{}{
			vind.From: id,
			vind.To:   vunhash(ksid),
		},
	}
	result, err := vcursor.Execute(bq)
	if err != nil {
		return false, err
	}
	if len(result.Rows) == 0 {
		return false, nil
	}
	return true, nil
}

func (vind *LookupHashUnique) Create(vcursor planbuilder.VCursor, id interface{}, ksid key.KeyspaceId) error {
	bq := &tproto.BoundQuery{
		Sql: vind.ins,
		BindVariables: map[string]interface{}{
			vind.From: id,
			vind.To:   vunhash(ksid),
		},
	}
	if _, err := vcursor.Execute(bq); err != nil {
		return err
	}
	return nil
}

func (vind *LookupHashUnique) Generate(vcursor planbuilder.VCursor, ksid key.KeyspaceId) (id interface{}, err error) {
	bq := &tproto.BoundQuery{
		Sql: vind.ins,
		BindVariables: map[string]interface{}{
			vind.From: nil,
			vind.To:   vunhash(ksid),
		},
	}
	result, err := vcursor.Execute(bq)
	if err != nil {
		return nil, err
	}
	return result.InsertId, err
}

func (vind *LookupHashUnique) Delete(vcursor planbuilder.VCursor, id interface{}, ksid key.KeyspaceId) error {
	bq := &tproto.BoundQuery{
		Sql: vind.del,
		BindVariables: map[string]interface{}{
			vind.From: id,
			vind.To:   vunhash(ksid),
		},
	}
	if _, err := vcursor.Execute(bq); err != nil {
		return err
	}
	return nil
}

func init() {
	planbuilder.Register("lookup_hash_unique", NewLookupHashUnique)
}
