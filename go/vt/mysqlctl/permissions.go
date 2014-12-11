// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package mysqlctl

import (
	"github.com/henryanand/vitess/go/vt/mysqlctl/proto"
)

func (mysqld *Mysqld) GetPermissions() (*proto.Permissions, error) {
	permissions := &proto.Permissions{}

	// get Users
	qr, err := mysqld.fetchSuperQuery("SELECT * FROM mysql.user")
	if err != nil {
		return nil, err
	}
	for _, row := range qr.Rows {
		permissions.UserPermissions = append(permissions.UserPermissions, proto.NewUserPermission(qr.Fields, row))
	}

	// get Dbs
	qr, err = mysqld.fetchSuperQuery("SELECT * FROM mysql.db")
	if err != nil {
		return nil, err
	}
	for _, row := range qr.Rows {
		permissions.DbPermissions = append(permissions.DbPermissions, proto.NewDbPermission(qr.Fields, row))
	}

	// get Hosts
	qr, err = mysqld.fetchSuperQuery("SELECT * FROM mysql.host")
	if err != nil {
		return nil, err
	}
	for _, row := range qr.Rows {
		permissions.HostPermissions = append(permissions.HostPermissions, proto.NewHostPermission(qr.Fields, row))
	}

	return permissions, nil
}
