// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package mysqlctl

import (
	"github.com/youtube/vitess/go/vt/mysqlctl/tmutils"
	tabletmanagerdatapb "github.com/youtube/vitess/go/vt/proto/tabletmanagerdata"
	"golang.org/x/net/context"
)

// GetPermissions lists the permissions on the mysqld
func GetPermissions(mysqld MysqlDaemon) (*tabletmanagerdatapb.Permissions, error) {
	ctx := context.TODO()
	permissions := &tabletmanagerdatapb.Permissions{}

	// get Users
	qr, err := mysqld.FetchSuperQuery(ctx, "SELECT * FROM mysql.user")
	if err != nil {
		return nil, err
	}
	for _, row := range qr.Rows {
		permissions.UserPermissions = append(permissions.UserPermissions, tmutils.NewUserPermission(qr.Fields, row))
	}

	// get Dbs
	qr, err = mysqld.FetchSuperQuery(ctx, "SELECT * FROM mysql.db")
	if err != nil {
		return nil, err
	}
	for _, row := range qr.Rows {
		permissions.DbPermissions = append(permissions.DbPermissions, tmutils.NewDbPermission(qr.Fields, row))
	}

	return permissions, nil
}
