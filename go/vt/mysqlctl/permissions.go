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
