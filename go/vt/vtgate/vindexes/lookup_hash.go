// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package vindexes

import (
	"fmt"

	"github.com/youtube/vitess/go/vt/key"
	tproto "github.com/youtube/vitess/go/vt/tabletserver/proto"
	"github.com/youtube/vitess/go/vt/vtgate/planbuilder"
)

// lookupHash implements the common functions between
// LookupHashUnique and LookupHashMulti.
type lookupHash struct {
	Table, From, To       string
	sel, verify, ins, del string
}

func (vind *lookupHash) init(m map[string]interface{}) {
	get := func(name string) string {
		v, _ := m[name].(string)
		return v
	}
	t := get("Table")
	from := get("From")
	to := get("To")

	vind.Table = t
	vind.From = from
	vind.To = to
	vind.sel = fmt.Sprintf("select %s from %s where %s = :%s", to, t, from, from)
	vind.verify = fmt.Sprintf("select %s from %s where %s = :%s and %s = :%s", from, t, from, from, to, to)
	vind.ins = fmt.Sprintf("insert into %s(%s, %s) values(:%s, :%s)", t, from, to, from, to)
	vind.del = fmt.Sprintf("delete from %s where %s in ::%s and %s = :%s", t, from, from, to, to)
}

// Verify returns true if id maps to ksid.
func (vind *lookupHash) Verify(vcursor planbuilder.VCursor, id interface{}, ksid key.KeyspaceId) (bool, error) {
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

// Create creates an association between id and ksid by inserting a row in the vindex table.
func (vind *lookupHash) Create(vcursor planbuilder.VCursor, id interface{}, ksid key.KeyspaceId) error {
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

// Delete deletes the association between ids and ksid.
func (vind *lookupHash) Delete(vcursor planbuilder.VCursor, ids []interface{}, ksid key.KeyspaceId) error {
	bq := &tproto.BoundQuery{
		Sql: vind.del,
		BindVariables: map[string]interface{}{
			vind.From: ids,
			vind.To:   vunhash(ksid),
		},
	}
	if _, err := vcursor.Execute(bq); err != nil {
		return err
	}
	return nil
}
