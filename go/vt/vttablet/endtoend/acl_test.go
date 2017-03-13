// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package endtoend

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	"github.com/youtube/vitess/go/vt/vttablet/endtoend/framework"
	"github.com/youtube/vitess/go/vt/vttablet/tabletserver/rules"
)

func TestTableACLNoAccess(t *testing.T) {
	client := framework.NewClient()

	aclErr := "table acl error"
	execCases := []struct {
		query string
		err   string
	}{{
		query: "select * from vitess_acl_no_access where key1=1",
		err:   aclErr,
	}, {
		query: "delete from vitess_acl_no_access where key1=1",
		err:   aclErr,
	}, {
		query: "alter table vitess_acl_no_access comment 'comment'",
		err:   aclErr,
	}, {
		query: "select * from vitess_acl_read_only where key1=1",
	}, {
		query: "delete from vitess_acl_read_only where key1=1",
		err:   aclErr,
	}, {
		query: "alter table vitess_acl_read_only comment 'comment'",
		err:   aclErr,
	}, {
		query: "select * from vitess_acl_read_write where key1=1",
	}, {
		query: "delete from vitess_acl_read_write where key1=1",
	}, {
		query: "alter table vitess_acl_read_write comment 'comment'",
		err:   aclErr,
	}, {
		query: "select * from vitess_acl_admin where key1=1",
	}, {
		query: "delete from vitess_acl_admin where key1=1",
	}, {
		query: "alter table vitess_acl_admin comment 'comment'",
	}, {
		query: "select * from vitess_acl_unmatched where key1=1",
	}, {
		query: "delete from vitess_acl_unmatched where key1=1",
	}, {
		query: "alter table vitess_acl_unmatched comment 'comment'",
	}, {
		query: "select * from vitess_acl_all_user_read_only where key1=1",
	}, {
		query: "delete from vitess_acl_all_user_read_only where key1=1",
		err:   aclErr,
	}, {
		query: "alter table vitess_acl_all_user_read_only comment 'comment'",
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
		query: "select * from vitess_acl_no_access where key1=1",
		err:   aclErr,
	}, {
		query: "select * from vitess_acl_read_only where key1=1",
	}, {
		query: "select * from vitess_acl_read_write where key1=1",
	}, {
		query: "select * from vitess_acl_admin where key1=1",
	}, {
		query: "select * from vitess_acl_unmatched where key1=1",
	}, {
		query: "select * from vitess_acl_all_user_read_only where key1=1",
	}}
	for _, tcase := range streamCases {
		_, err := client.StreamExecute(tcase.query, nil)
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
		"Operator": ""
	}]
}]`)

func TestQueryRules(t *testing.T) {
	rules := rules.New()
	err := rules.UnmarshalJSON(rulesJSON)
	if err != nil {
		t.Error(err)
		return
	}
	err = framework.Server.SetQueryRules("endtoend", rules)
	want := "Rule source identifier endtoend is not valid"
	if err == nil || err.Error() != want {
		t.Errorf("Error: %v, want %s", err, want)
	}

	framework.Server.RegisterQueryRuleSource("endtoend")
	defer framework.Server.UnRegisterQueryRuleSource("endtoend")
	err = framework.Server.SetQueryRules("endtoend", rules)
	if err != nil {
		t.Error(err)
		return
	}

	rulesJSON := compacted(framework.FetchURL("/debug/query_rules"))
	want = compacted(`{
		"endtoend":[{
			"Description": "disallow bindvar 'asdfg'",
			"Name": "r1",
			"BindVarConds":[{
				"Name": "asdfg",
				"OnAbsent": false,
				"Operator": ""
			}],
			"Action": "FAIL"
		}]
	}`)
	if rulesJSON != want {
		t.Errorf("/debug/query_rules:\n%v, want\n%s", rulesJSON, want)
	}

	client := framework.NewClient()
	query := "select * from vitess_test where intval=:asdfg"
	bv := map[string]interface{}{"asdfg": 1}
	_, err = client.Execute(query, bv)
	want = "disallowed due to rule: disallow bindvar 'asdfg'"
	if err == nil || err.Error() != want {
		t.Errorf("Error: %v, want %s", err, want)
	}
	_, err = client.StreamExecute(query, bv)
	want = "disallowed due to rule: disallow bindvar 'asdfg'"
	if err == nil || err.Error() != want {
		t.Errorf("Error: %v, want %s", err, want)
	}

	err = framework.Server.SetQueryRules("endtoend", nil)
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

func compacted(in string) string {
	dst := bytes.NewBuffer(nil)
	err := json.Compact(dst, []byte(in))
	if err != nil {
		panic(err)
	}
	return dst.String()
}
