/*
Copyright 2017 Google Inc.

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

package tmutils

import (
	"testing"

	"github.com/youtube/vitess/go/sqltypes"
	querypb "github.com/youtube/vitess/go/vt/proto/query"

	tabletmanagerdatapb "github.com/youtube/vitess/go/vt/proto/tabletmanagerdata"
)

func mapToSQLResults(row map[string]string) ([]*querypb.Field, []sqltypes.Value) {
	fields := make([]*querypb.Field, len(row))
	values := make([]sqltypes.Value, len(row))
	index := 0
	for key, value := range row {
		fields[index] = &querypb.Field{Name: key}
		values[index] = sqltypes.NewVarBinary(value)
		index++
	}
	return fields, values
}

func testPermissionsDiff(t *testing.T, left, right *tabletmanagerdatapb.Permissions, leftName, rightName string, expected []string) {
	t.Helper()

	actual := DiffPermissionsToArray(leftName, left, rightName, right)

	equal := false
	if len(actual) == len(expected) {
		equal = true
		for i, val := range actual {
			if val != expected[i] {
				equal = false
				break
			}
		}
	}

	if !equal {
		t.Logf("Expected: %v", expected)
		t.Logf("Actual  : %v", actual)
		t.Fail()
	}
}

func TestPermissionsDiff(t *testing.T) {

	p1 := &tabletmanagerdatapb.Permissions{}
	p1.UserPermissions = append(p1.UserPermissions, NewUserPermission(mapToSQLResults(map[string]string{
		"Host":        "%",
		"User":        "vt",
		"Password":    "p1",
		"Select_priv": "Y",
		"Insert_priv": "N",
		// Test the next field is skipped (to avoid date drifts).
		"password_last_changed": "2016-11-08 02:56:23",
	})))
	p1.DbPermissions = append(p1.DbPermissions, NewDbPermission(mapToSQLResults(map[string]string{
		"Host":        "%",
		"Db":          "vt_live",
		"User":        "vt",
		"Select_priv": "N",
		"Insert_priv": "Y",
	})))

	if PermissionsString(p1) !=
		"User Permissions:\n"+
			"  %:vt: UserPermission PasswordChecksum(4831957779889520640) Insert_priv(N) Select_priv(Y)\n"+
			"Db Permissions:\n"+
			"  %:vt_live:vt: DbPermission Insert_priv(Y) Select_priv(N)\n" {
		t.Logf("Actual: %v", p1.String())
		t.Fail()
	}

	testPermissionsDiff(t, p1, p1, "p1-1", "p1-2", []string{})

	p2 := &tabletmanagerdatapb.Permissions{}
	testPermissionsDiff(t, p1, p2, "p1", "p2", []string{
		"p1 has an extra user %:vt",
		"p1 has an extra db %:vt_live:vt",
	})

	p2.DbPermissions = p1.DbPermissions
	p1.DbPermissions = nil
	testPermissionsDiff(t, p1, p2, "p1", "p2", []string{
		"p1 has an extra user %:vt",
		"p2 has an extra db %:vt_live:vt",
	})

	p2.UserPermissions = append(p2.UserPermissions, NewUserPermission(mapToSQLResults(map[string]string{
		"Host":        "%",
		"User":        "vt",
		"Password":    "p1",
		"Select_priv": "Y",
		"Insert_priv": "Y",
	})))
	p1.DbPermissions = append(p1.DbPermissions, NewDbPermission(mapToSQLResults(map[string]string{
		"Host":        "%",
		"Db":          "vt_live",
		"User":        "vt",
		"Select_priv": "Y",
		"Insert_priv": "N",
	})))
	testPermissionsDiff(t, p1, p2, "p1", "p2", []string{
		"permissions differ on user %:vt:\n" +
			"p1: UserPermission PasswordChecksum(4831957779889520640) Insert_priv(N) Select_priv(Y)\n" +
			" differs from:\n" +
			"p2: UserPermission PasswordChecksum(4831957779889520640) Insert_priv(Y) Select_priv(Y)",
		"permissions differ on db %:vt_live:vt:\n" +
			"p1: DbPermission Insert_priv(N) Select_priv(Y)\n" +
			" differs from:\n" +
			"p2: DbPermission Insert_priv(Y) Select_priv(N)",
	})

	p2.UserPermissions[0].Privileges["Insert_priv"] = "N"
	p2.DbPermissions[0].Privileges["Insert_priv"] = "N"
	p2.DbPermissions[0].Privileges["Select_priv"] = "Y"
	testPermissionsDiff(t, p1, p2, "p1", "p2", []string{})
}
