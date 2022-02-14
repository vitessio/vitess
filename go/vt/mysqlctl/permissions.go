package mysqlctl

import (
	"context"

	"vitess.io/vitess/go/vt/mysqlctl/tmutils"
	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
)

// GetPermissions lists the permissions on the mysqld.
// The rows are sorted in primary key order to help with comparing
// permissions between tablets.
func GetPermissions(mysqld MysqlDaemon) (*tabletmanagerdatapb.Permissions, error) {
	ctx := context.TODO()
	permissions := &tabletmanagerdatapb.Permissions{}

	// get Users
	qr, err := mysqld.FetchSuperQuery(ctx, "SELECT * FROM mysql.user ORDER BY host, user")
	if err != nil {
		return nil, err
	}
	for _, row := range qr.Rows {
		permissions.UserPermissions = append(permissions.UserPermissions, tmutils.NewUserPermission(qr.Fields, row))
	}

	// get Dbs
	qr, err = mysqld.FetchSuperQuery(ctx, "SELECT * FROM mysql.db ORDER BY host, db, user")
	if err != nil {
		return nil, err
	}
	for _, row := range qr.Rows {
		permissions.DbPermissions = append(permissions.DbPermissions, tmutils.NewDbPermission(qr.Fields, row))
	}

	return permissions, nil
}
