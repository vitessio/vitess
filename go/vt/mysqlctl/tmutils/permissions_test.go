// Copyright 2013, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

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
		values[index] = sqltypes.MakeString(([]byte)(value))
		index++
	}
	return fields, values
}

func testPermissionsDiff(t *testing.T, left, right *tabletmanagerdatapb.Permissions, leftName, rightName string, expected []string) {

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
		"p1 and p2 disagree on user %:vt:\n" +
			"UserPermission PasswordChecksum(4831957779889520640) Insert_priv(N) Select_priv(Y)\n" +
			" differs from:\n" +
			"UserPermission PasswordChecksum(4831957779889520640) Insert_priv(Y) Select_priv(Y)",
		"p1 and p2 disagree on db %:vt_live:vt:\n" +
			"DbPermission Insert_priv(N) Select_priv(Y)\n" +
			" differs from:\n" +
			"DbPermission Insert_priv(Y) Select_priv(N)",
	})

	p2.UserPermissions[0].Privileges["Insert_priv"] = "N"
	p2.DbPermissions[0].Privileges["Insert_priv"] = "N"
	p2.DbPermissions[0].Privileges["Select_priv"] = "Y"
	testPermissionsDiff(t, p1, p2, "p1", "p2", []string{})
}
