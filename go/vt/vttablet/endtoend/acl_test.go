/*
Copyright 2019 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package endtoend

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/vttablet/endtoend/framework"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/rules"

	querypb "vitess.io/vitess/go/vt/proto/query"
)

func TestTableACL(t *testing.T) {
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
	}, {
		query: "select * from vitess_acl_read_only, vitess_acl_no_access",
		err:   aclErr,
	}, {
		query: "delete from vitess_acl_read_write where key1=(select key1 from vitess_acl_no_access)",
		err:   aclErr,
	}, {
		query: "delete from vitess_acl_read_write where key1=(select key1 from vitess_acl_read_only)",
	}, {
		query: "update vitess_acl_read_write join vitess_acl_read_only on 1!=1 set key1=1",
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
			t.Errorf("Execute(%s): Error: %v, must start with %s", tcase.query, err, tcase.err)
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
	bv := map[string]*querypb.BindVariable{"asdfg": sqltypes.Int64BindVariable(1)}
	_, err = client.Execute(query, bv)
	want = "disallowed due to rule: disallow bindvar 'asdfg' (CallerID: dev)"
	if err == nil || err.Error() != want {
		t.Errorf("Error: %v, want %s", err, want)
	}
	_, err = client.StreamExecute(query, bv)
	want = "disallowed due to rule: disallow bindvar 'asdfg' (CallerID: dev)"
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
