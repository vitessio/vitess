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
	// from backup and then connect to the master. But if this tablet has the same
	// server-id as before, and if this tablet was recently a master, then it can
	// lose data by skipping binlog events due to replicate-same-server-id=FALSE,
	// which is the default setting.
	if err := mycnf.RandomizeMysqlServerID(); err != nil {
		return nil, nil, fmt.Errorf("couldn't generate random MySQL server_id: %v", err)
	}
	if mysqlSocket != "" {
		mycnf.SocketFile = mysqlSocket
	}

	dbcfgs, err := dbconfigs.Init(mycnf.SocketFile)
	if err != nil {
		return nil, nil, fmt.Errorf("couldn't Init dbconfigs: %v", err)
	}

	mysqld, err := NewMysqld(dbcfgs)
	if err != nil {
		return nil, nil, err
	}
	return mysqld, mycnf, nil
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

	dbcfgs, err := dbconfigs.Init(mycnf.SocketFile)
	if err != nil {
		return nil, nil, fmt.Errorf("couldn't Init dbconfigs: %v", err)
	}

	mysqld, err := NewMysqld(dbcfgs)
	if err != nil {
		return nil, nil, err
	}
	return mysqld, mycnf, nil
}
