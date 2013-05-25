// Copyright 2013, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package mysqlctl

import (
	"testing"

	"code.google.com/p/vitess/go/mysql/proto"
	"code.google.com/p/vitess/go/sqltypes"
)

func mapToSqlResults(row map[string]string) ([]proto.Field, []sqltypes.Value) {
	fields := make([]proto.Field, len(row))
	values := make([]sqltypes.Value, len(row))
	index := 0
	for key, value := range row {
		fields[index] = proto.Field{Name: key}
		values[index] = sqltypes.MakeString(([]byte)(value))
		index++
	}
	return fields, values
}

func testPermissionsDiff(t *testing.T, left, right *Permissions, leftName, rightName string, expected []string) {

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

	p1 := &Permissions{}
	p1.UserPermissions = append(p1.UserPermissions, newUserPermission(mapToSqlResults(map[string]string{"Host": "%", "User": "vt", "Password": "p1", "Select_priv": "Y", "Insert_priv": "N"})))
	p1.DbPermissions = append(p1.DbPermissions, newDbPermission(mapToSqlResults(map[string]string{"Host": "%", "Db": "vt_live", "User": "vt", "Select_priv": "N", "Insert_priv": "Y"})))
	p1.HostPermissions = append(p1.HostPermissions, newHostPermission(mapToSqlResults(map[string]string{"Host": "localhost", "Db": "mysql", "Select_priv": "N", "Insert_priv": "N"})))

	if p1.String() !=
		"User Permissions:\n"+
			"  %:vt: UserPermission PasswordChecksum(4831957779889520640) Insert_priv(N) Select_priv(Y)\n"+
			"Db Permissions:\n"+
			"  %:vt_live:vt: DbPermission Insert_priv(Y) Select_priv(N)\n"+
			"Host Permissions:\n"+
			"  localhost:mysql: HostPermission Insert_priv(N) Select_priv(N)\n" {
		t.Logf("Actual: %v", p1.String())
		t.Fail()
	}

	testPermissionsDiff(t, p1, p1, "p1-1", "p1-2", []string{})

	p2 := &Permissions{}
	testPermissionsDiff(t, p1, p2, "p1", "p2", []string{
		"p1 has an extra user %:vt",
		"p1 has an extra db %:vt_live:vt",
		"p1 has an extra host localhost:mysql",
	})

	p2.DbPermissions = p1.DbPermissions
	p1.DbPermissions = nil
	testPermissionsDiff(t, p1, p2, "p1", "p2", []string{
		"p1 has an extra user %:vt",
		"p2 has an extra db %:vt_live:vt",
		"p1 has an extra host localhost:mysql",
	})

	p2.UserPermissions = append(p2.UserPermissions, newUserPermission(mapToSqlResults(map[string]string{"Host": "%", "User": "vt", "Password": "p1", "Select_priv": "Y", "Insert_priv": "Y"})))
	p1.DbPermissions = append(p1.DbPermissions, newDbPermission(mapToSqlResults(map[string]string{"Host": "%", "Db": "vt_live", "User": "vt", "Select_priv": "Y", "Insert_priv": "N"})))
	p2.HostPermissions = append(p2.HostPermissions, newHostPermission(mapToSqlResults(map[string]string{"Host": "localhost", "Db": "mysql", "Select_priv": "Y", "Insert_priv": "N"})))
	testPermissionsDiff(t, p1, p2, "p1", "p2", []string{
		"p1 and p2 disagree on user %:vt:\n" +
			"UserPermission PasswordChecksum(4831957779889520640) Insert_priv(N) Select_priv(Y)\n" +
			" differs from:\n" +
			"UserPermission PasswordChecksum(4831957779889520640) Insert_priv(Y) Select_priv(Y)",
		"p1 and p2 disagree on db %:vt_live:vt:\n" +
			"DbPermission Insert_priv(N) Select_priv(Y)\n" +
			" differs from:\n" +
			"DbPermission Insert_priv(Y) Select_priv(N)",
		"p1 and p2 disagree on host localhost:mysql:\n" +
			"HostPermission Insert_priv(N) Select_priv(N)\n" +
			" differs from:\n" +
			"HostPermission Insert_priv(N) Select_priv(Y)",
	})

	p2.UserPermissions[0].Privileges["Insert_priv"] = "N"
	p2.DbPermissions[0].Privileges["Insert_priv"] = "N"
	p2.DbPermissions[0].Privileges["Select_priv"] = "Y"
	p2.HostPermissions[0].Privileges["Select_priv"] = "N"
	testPermissionsDiff(t, p1, p2, "p1", "p2", []string{})
}
