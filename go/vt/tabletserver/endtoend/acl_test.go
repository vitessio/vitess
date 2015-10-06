// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package endtoend

import (
	"strings"
	"testing"

	mproto "github.com/youtube/vitess/go/mysql/proto"
	"github.com/youtube/vitess/go/vt/tabletserver"
	"github.com/youtube/vitess/go/vt/tabletserver/endtoend/framework"
)

func TestTableACLNoAccess(t *testing.T) {
	client := framework.NewDefaultClient()

	aclErr := "error: table acl error"
	execCases := []struct {
		query string
		err   string
	}{{
		query: "select * from vtocc_acl_no_access where key1=1",
		err:   aclErr,
	}, {
		query: "delete from vtocc_acl_no_access where key1=1",
		err:   aclErr,
	}, {
		query: "alter table vtocc_acl_no_access comment 'comment'",
		err:   aclErr,
	}, {
		query: "select * from vtocc_acl_read_only where key1=1",
	}, {
		query: "delete from vtocc_acl_read_only where key1=1",
		err:   aclErr,
	}, {
		query: "alter table vtocc_acl_read_only comment 'comment'",
		err:   aclErr,
	}, {
		query: "select * from vtocc_acl_read_write where key1=1",
	}, {
		query: "delete from vtocc_acl_read_write where key1=1",
	}, {
		query: "alter table vtocc_acl_read_write comment 'comment'",
		err:   aclErr,
	}, {
		query: "select * from vtocc_acl_admin where key1=1",
	}, {
		query: "delete from vtocc_acl_admin where key1=1",
	}, {
		query: "alter table vtocc_acl_admin comment 'comment'",
	}, {
		query: "select * from vtocc_acl_unmatched where key1=1",
	}, {
		query: "delete from vtocc_acl_unmatched where key1=1",
	}, {
		query: "alter table vtocc_acl_unmatched comment 'comment'",
	}, {
		query: "select * from vtocc_acl_all_user_read_only where key1=1",
	}, {
		query: "delete from vtocc_acl_all_user_read_only where key1=1",
		err:   aclErr,
	}, {
		query: "alter table vtocc_acl_all_user_read_only comment 'comment'",
		err:   aclErr,
	}}

	for _, tcase := range execCases {
		_, err := client.Execute(tcase.query, nil)
		if tcase.err == "" {
			if err != nil {
				t.Error(err)
			}
			continue
		}
		if err == nil || !strings.HasPrefix(err.Error(), tcase.err) {
			t.Errorf("Error: %v, must start with %s", err, tcase.err)
		}
	}

	streamCases := []struct {
		query string
		err   string
	}{{
		query: "select * from vtocc_acl_no_access where key1=1",
		err:   aclErr,
	}, {
		query: "select * from vtocc_acl_read_only where key1=1",
	}, {
		query: "select * from vtocc_acl_read_write where key1=1",
	}, {
		query: "select * from vtocc_acl_admin where key1=1",
	}, {
		query: "select * from vtocc_acl_unmatched where key1=1",
	}, {
		query: "select * from vtocc_acl_all_user_read_only where key1=1",
	}}
	for _, tcase := range streamCases {
		err := client.StreamExecute(tcase.query, nil, func(*mproto.QueryResult) error { return nil })
		if tcase.err == "" {
			if err != nil {
				t.Error(err)
			}
			continue
		}
		if err == nil || !strings.HasPrefix(err.Error(), tcase.err) {
			t.Errorf("Error: %v, must start with %s", err, tcase.err)
		}
	}
}

var rulesJSON = []byte(`[{
	"Name": "r1",
	"Description": "disallow bindvar 'asdfg'",
	"BindVarConds":[{
		"Name": "asdfg",
		"OnAbsent": false,
		"Operator": "NOOP"
	}]
}]`)

func TestQueryRules(t *testing.T) {
	rules := tabletserver.NewQueryRules()
	err := rules.UnmarshalJSON(rulesJSON)
	if err != nil {
		t.Error(err)
		return
	}
	err = framework.DefaultServer.SetQueryRules("endtoend", rules)
	want := "Rule source identifier endtoend is not valid"
	if err == nil || err.Error() != want {
		t.Errorf("Error: %v, want %s", err, want)
	}

	framework.DefaultServer.RegisterQueryRuleSource("endtoend")
	defer framework.DefaultServer.UnRegisterQueryRuleSource("endtoend")
	err = framework.DefaultServer.SetQueryRules("endtoend", rules)
	if err != nil {
		t.Error(err)
		return
	}

	client := framework.NewDefaultClient()
	query := "select * from vtocc_test where intval=:asdfg"
	bv := map[string]interface{}{"asdfg": 1}
	_, err = client.Execute(query, bv)
	want = "error: Query disallowed due to rule: disallow bindvar 'asdfg'"
	if err == nil || err.Error() != want {
		t.Errorf("Error: %v, want %s", err, want)
	}

	err = framework.DefaultServer.SetQueryRules("endtoend", nil)
	if err != nil {
		t.Error(err)
		return
	}
	_, err = client.Execute(query, bv)
	if err != nil {
		t.Error(err)
		return
	}
}
