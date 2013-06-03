// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package mysqlctl

import (
	"fmt"
	"hash/crc64"
	"sort"

	"code.google.com/p/vitess/go/mysql/proto"
	"code.google.com/p/vitess/go/sqltypes"
	"code.google.com/p/vitess/go/vt/concurrency"
)

var (
	hashTable = crc64.MakeTable(crc64.ISO)
)

type Permission interface {
	PrimaryKey() string
	String() string
}

type PermissionList interface {
	Get(int) Permission
	Len() int
}

func printPrivileges(priv map[string]string) string {
	si := make([]string, 0, len(priv))
	for k := range priv {
		si = append(si, k)
	}
	sort.Strings(si)
	result := ""
	for _, k := range si {
		result += " " + k + "(" + priv[k] + ")"
	}
	return result
}

// UserPermission describes a single row in the mysql.user table
// Primary key is Host+User
// PasswordChecksum is the crc64 of the password, for security reasons
type UserPermission struct {
	Host             string
	User             string
	PasswordChecksum uint64
	Privileges       map[string]string
}

func newUserPermission(fields []proto.Field, values []sqltypes.Value) *UserPermission {
	up := &UserPermission{Privileges: make(map[string]string)}
	for i, field := range fields {
		switch field.Name {
		case "Host":
			up.Host = values[i].String()
		case "User":
			up.User = values[i].String()
		case "Password":
			up.PasswordChecksum = crc64.Checksum(([]byte)(values[i].String()), hashTable)
		default:
			up.Privileges[field.Name] = values[i].String()
		}
	}
	return up
}

func (up *UserPermission) PrimaryKey() string {
	return up.Host + ":" + up.User
}

func (up *UserPermission) String() string {
	var passwd string
	if up.PasswordChecksum == 0 {
		passwd = "NoPassword"
	} else {
		passwd = fmt.Sprintf("PasswordChecksum(%v)", up.PasswordChecksum)
	}
	return "UserPermission " + passwd + printPrivileges(up.Privileges)
}

type UserPermissionList []*UserPermission

func (upl UserPermissionList) Get(i int) Permission {
	return upl[i]
}

func (upl UserPermissionList) Len() int {
	return len(upl)
}

// DbPermission describes a single row in the mysql.db table
// Primary key is Host+Db+User
type DbPermission struct {
	Host       string
	Db         string
	User       string
	Privileges map[string]string
}

func newDbPermission(fields []proto.Field, values []sqltypes.Value) *DbPermission {
	up := &DbPermission{Privileges: make(map[string]string)}
	for i, field := range fields {
		switch field.Name {
		case "Host":
			up.Host = values[i].String()
		case "Db":
			up.Db = values[i].String()
		case "User":
			up.User = values[i].String()
		default:
			up.Privileges[field.Name] = values[i].String()
		}
	}
	return up
}

func (dp *DbPermission) PrimaryKey() string {
	return dp.Host + ":" + dp.Db + ":" + dp.User
}

func (dp *DbPermission) String() string {
	return "DbPermission" + printPrivileges(dp.Privileges)
}

type DbPermissionList []*DbPermission

func (upl DbPermissionList) Get(i int) Permission {
	return upl[i]
}

func (upl DbPermissionList) Len() int {
	return len(upl)
}

// HostPermission describes a single row in the mysql.host table
// Primary key is Host+Db
type HostPermission struct {
	Host       string
	Db         string
	Privileges map[string]string
}

func newHostPermission(fields []proto.Field, values []sqltypes.Value) *HostPermission {
	hp := &HostPermission{Privileges: make(map[string]string)}
	for i, field := range fields {
		switch field.Name {
		case "Host":
			hp.Host = values[i].String()
		case "Db":
			hp.Db = values[i].String()
		default:
			hp.Privileges[field.Name] = values[i].String()
		}
	}
	return hp
}

func (hp *HostPermission) PrimaryKey() string {
	return hp.Host + ":" + hp.Db
}

func (hp *HostPermission) String() string {
	return "HostPermission" + printPrivileges(hp.Privileges)
}

type HostPermissionList []*HostPermission

func (upl HostPermissionList) Get(i int) Permission {
	return upl[i]
}

func (upl HostPermissionList) Len() int {
	return len(upl)
}

// Permissions have all the rows in mysql.{user,db,host} tables,
// (all rows are sorted by primary key)
type Permissions struct {
	UserPermissions UserPermissionList
	DbPermissions   DbPermissionList
	HostPermissions HostPermissionList
}

func (mysqld *Mysqld) GetPermissions() (*Permissions, error) {
	permissions := &Permissions{}

	// get Users
	qr, err := mysqld.fetchSuperQuery("SELECT * FROM mysql.user")
	if err != nil {
		return nil, err
	}
	for _, row := range qr.Rows {
		permissions.UserPermissions = append(permissions.UserPermissions, newUserPermission(qr.Fields, row))
	}

	// get Dbs
	qr, err = mysqld.fetchSuperQuery("SELECT * FROM mysql.db")
	if err != nil {
		return nil, err
	}
	for _, row := range qr.Rows {
		permissions.DbPermissions = append(permissions.DbPermissions, newDbPermission(qr.Fields, row))
	}

	// get Hosts
	qr, err = mysqld.fetchSuperQuery("SELECT * FROM mysql.host")
	if err != nil {
		return nil, err
	}
	for _, row := range qr.Rows {
		permissions.HostPermissions = append(permissions.HostPermissions, newHostPermission(qr.Fields, row))
	}

	return permissions, nil
}

func printPermissions(name string, permissions PermissionList) string {
	result := name + " Permissions:\n"
	for i := 0; i < permissions.Len(); i++ {
		perm := permissions.Get(i)
		result += "  " + perm.PrimaryKey() + ": " + perm.String() + "\n"
	}
	return result
}

func (permissions *Permissions) String() string {
	return printPermissions("User", permissions.UserPermissions) +
		printPermissions("Db", permissions.DbPermissions) +
		printPermissions("Host", permissions.HostPermissions)
}

func diffPermissions(name, leftName string, left PermissionList, rightName string, right PermissionList, er concurrency.ErrorRecorder) {

	leftIndex := 0
	rightIndex := 0
	for leftIndex < left.Len() && rightIndex < right.Len() {
		l := left.Get(leftIndex)
		r := right.Get(rightIndex)

		// extra value on the left side
		if l.PrimaryKey() < r.PrimaryKey() {
			er.RecordError(fmt.Errorf("%v has an extra %v %v", leftName, name, l.PrimaryKey()))
			leftIndex++
			continue
		}

		// extra value on the right side
		if l.PrimaryKey() > r.PrimaryKey() {
			er.RecordError(fmt.Errorf("%v has an extra %v %v", rightName, name, r.PrimaryKey()))
			rightIndex++
			continue
		}

		// same name, let's see content
		if l.String() != r.String() {
			er.RecordError(fmt.Errorf("%v and %v disagree on %v %v:\n%v\n differs from:\n%v", leftName, rightName, name, l.PrimaryKey(), l.String(), r.String()))
		}
		leftIndex++
		rightIndex++
	}
	for leftIndex < left.Len() {
		er.RecordError(fmt.Errorf("%v has an extra %v %v", leftName, name, left.Get(leftIndex).PrimaryKey()))
		leftIndex++
	}
	for rightIndex < right.Len() {
		er.RecordError(fmt.Errorf("%v has an extra %v %v", rightName, name, right.Get(rightIndex).PrimaryKey()))
		rightIndex++
	}
}

func DiffPermissions(leftName string, left *Permissions, rightName string, right *Permissions, er concurrency.ErrorRecorder) {
	diffPermissions("user", leftName, left.UserPermissions, rightName, right.UserPermissions, er)
	diffPermissions("db", leftName, left.DbPermissions, rightName, right.DbPermissions, er)
	diffPermissions("host", leftName, left.HostPermissions, rightName, right.HostPermissions, er)
}

func DiffPermissionsToArray(leftName string, left *Permissions, rightName string, right *Permissions) (result []string) {
	er := concurrency.AllErrorRecorder{}
	DiffPermissions(leftName, left, rightName, right, &er)
	if er.HasErrors() {
		return er.Errors
	} else {
		return nil
	}
}
