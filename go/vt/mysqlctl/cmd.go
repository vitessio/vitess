/*
This file contains common functions for cmd/mysqlctl and cmd/mysqlctld.
*/

package mysqlctl

import (
	"fmt"

	"vitess.io/vitess/go/vt/dbconfigs"
)

// CreateMysqldAndMycnf returns a Mysqld and a Mycnf object to use for working with a MySQL
// installation that hasn't been set up yet.
func CreateMysqldAndMycnf(tabletUID uint32, mysqlSocket string, mysqlPort int32) (*Mysqld, *Mycnf, error) {
	mycnf := NewMycnf(tabletUID, mysqlPort)
	// Choose a random MySQL server-id, since this is a fresh data dir.
	// We don't want to use the tablet UID as the MySQL server-id,
	// because reusing server-ids is not safe.
	//
	// For example, if a tablet comes back with an empty data dir, it will restore
	// from backup and then connect to the primary. But if this tablet has the same
	// server-id as before, and if this tablet was recently a primary, then it can
	// lose data by skipping binlog events due to replicate-same-server-id=FALSE,
	// which is the default setting.
	if err := mycnf.RandomizeMysqlServerID(); err != nil {
		return nil, nil, fmt.Errorf("couldn't generate random MySQL server_id: %v", err)
	}
	if mysqlSocket != "" {
		mycnf.SocketFile = mysqlSocket
	}

	dbconfigs.GlobalDBConfigs.InitWithSocket(mycnf.SocketFile)
	return NewMysqld(&dbconfigs.GlobalDBConfigs), mycnf, nil
}

// OpenMysqldAndMycnf returns a Mysqld and a Mycnf object to use for working with a MySQL
// installation that already exists. The Mycnf will be built based on the my.cnf file
// of the MySQL instance.
func OpenMysqldAndMycnf(tabletUID uint32) (*Mysqld, *Mycnf, error) {
	// We pass a port of 0, this will be read and overwritten from the path on disk
	mycnf, err := ReadMycnf(NewMycnf(tabletUID, 0))
	if err != nil {
		return nil, nil, fmt.Errorf("couldn't read my.cnf file: %v", err)
	}

	dbconfigs.GlobalDBConfigs.InitWithSocket(mycnf.SocketFile)
	return NewMysqld(&dbconfigs.GlobalDBConfigs), mycnf, nil
}
