// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package tmutils contains helper methods to deal with the tabletmanagerdata
// proto3 structures.
package tmutils

import (
	"fmt"
	"hash/crc64"
	"sort"
	"strings"

	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/vt/concurrency"
	querypb "github.com/youtube/vitess/go/vt/proto/query"

	tabletmanagerdatapb "github.com/youtube/vitess/go/vt/proto/tabletmanagerdata"
)

// This file contains helper methods to deal with Permissions.

var (
	hashTable = crc64.MakeTable(crc64.ISO)
)

// permissionList is an internal type to facilitate common code between the 3 permission types
type permissionList interface {
	Get(int) (primayKey string, value string)
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

// NewUserPermission is a helper method to create a tabletmanagerdatapb.UserPermission
func NewUserPermission(fields []*querypb.Field, values []sqltypes.Value) *tabletmanagerdatapb.UserPermission {
	up := &tabletmanagerdatapb.UserPermission{
		Privileges: make(map[string]string),
	}
	for i, field := range fields {
		switch strings.ToLower(field.Name) {
		case "host":
			up.Host = values[i].String()
		case "user":
			up.User = values[i].String()
		case "password":
			up.PasswordChecksum = crc64.Checksum(([]byte)(values[i].String()), hashTable)
		case "password_last_changed":
			// we skip this one, as the value may be
			// different on master and slaves.
		default:
			up.Privileges[field.Name] = values[i].String()
		}
	}
	return up
}

// UserPermissionPrimaryKey returns the sorting key for a UserPermission
func UserPermissionPrimaryKey(up *tabletmanagerdatapb.UserPermission) string {
	return up.Host + ":" + up.User
}

// UserPermissionString pretty-prints a UserPermission
func UserPermissionString(up *tabletmanagerdatapb.UserPermission) string {
	var passwd string
	if up.PasswordChecksum == 0 {
		passwd = "NoPassword"
	} else {
		passwd = fmt.Sprintf("PasswordChecksum(%v)", up.PasswordChecksum)
	}
	return "UserPermission " + passwd + printPrivileges(up.Privileges)
}

type userPermissionList []*tabletmanagerdatapb.UserPermission

func (upl userPermissionList) Get(i int) (string, string) {
	return UserPermissionPrimaryKey(upl[i]), UserPermissionString(upl[i])
}

func (upl userPermissionList) Len() int {
	return len(upl)
}

// NewDbPermission is a helper method to create a tabletmanagerdatapb.DbPermission
func NewDbPermission(fields []*querypb.Field, values []sqltypes.Value) *tabletmanagerdatapb.DbPermission {
	up := &tabletmanagerdatapb.DbPermission{
		Privileges: make(map[string]string),
	}
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

// DbPermissionPrimaryKey returns the sorting key for a DbPermission
func DbPermissionPrimaryKey(dp *tabletmanagerdatapb.DbPermission) string {
	return dp.Host + ":" + dp.Db + ":" + dp.User
}

// DbPermissionString pretty-prints a DbPermission
func DbPermissionString(dp *tabletmanagerdatapb.DbPermission) string {
	return "DbPermission" + printPrivileges(dp.Privileges)
}

type dbPermissionList []*tabletmanagerdatapb.DbPermission

func (upl dbPermissionList) Get(i int) (string, string) {
	return DbPermissionPrimaryKey(upl[i]), DbPermissionString(upl[i])
}

func (upl dbPermissionList) Len() int {
	return len(upl)
}

func printPermissions(name string, permissions permissionList) string {
	result := name + " Permissions:\n"
	for i := 0; i < permissions.Len(); i++ {
		pk, val := permissions.Get(i)
		result += "  " + pk + ": " + val + "\n"
	}
	return result
}

// PermissionsString pretty-prints Permissions
func PermissionsString(permissions *tabletmanagerdatapb.Permissions) string {
	return printPermissions("User", userPermissionList(permissions.UserPermissions)) +
		printPermissions("Db", dbPermissionList(permissions.DbPermissions))
}

func diffPermissions(name, leftName string, left permissionList, rightName string, right permissionList, er concurrency.ErrorRecorder) {

	leftIndex := 0
	rightIndex := 0
	for leftIndex < left.Len() && rightIndex < right.Len() {
		lpk, lval := left.Get(leftIndex)
		rpk, rval := right.Get(rightIndex)

		// extra value on the left side
		if lpk < rpk {
			er.RecordError(fmt.Errorf("%v has an extra %v %v", leftName, name, lpk))
			leftIndex++
			continue
		}

		// extra value on the right side
		if lpk > rpk {
			er.RecordError(fmt.Errorf("%v has an extra %v %v", rightName, name, rpk))
			rightIndex++
			continue
		}

		// same name, let's see content
		if lval != rval {
			er.RecordError(fmt.Errorf("%v and %v disagree on %v %v:\n%v\n differs from:\n%v", leftName, rightName, name, lpk, lval, rval))
		}
		leftIndex++
		rightIndex++
	}
	for leftIndex < left.Len() {
		lpk, _ := left.Get(leftIndex)
		er.RecordError(fmt.Errorf("%v has an extra %v %v", leftName, name, lpk))
		leftIndex++
	}
	for rightIndex < right.Len() {
		rpk, _ := right.Get(rightIndex)
		er.RecordError(fmt.Errorf("%v has an extra %v %v", rightName, name, rpk))
		rightIndex++
	}
}

// DiffPermissions records the errors between two permission sets
func DiffPermissions(leftName string, left *tabletmanagerdatapb.Permissions, rightName string, right *tabletmanagerdatapb.Permissions, er concurrency.ErrorRecorder) {
	diffPermissions("user", leftName, userPermissionList(left.UserPermissions), rightName, userPermissionList(right.UserPermissions), er)
	diffPermissions("db", leftName, dbPermissionList(left.DbPermissions), rightName, dbPermissionList(right.DbPermissions), er)
}

// DiffPermissionsToArray difs two sets of permissions, and returns the difference
func DiffPermissionsToArray(leftName string, left *tabletmanagerdatapb.Permissions, rightName string, right *tabletmanagerdatapb.Permissions) (result []string) {
	er := concurrency.AllErrorRecorder{}
	DiffPermissions(leftName, left, rightName, right, &er)
	if er.HasErrors() {
		return er.ErrorStrings()
	}
	return nil
}
