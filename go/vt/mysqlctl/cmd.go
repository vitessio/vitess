// Copyright 2016, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

/*
This file contains common functions for cmd/mysqlctl and cmd/mysqlctld.
*/

package mysqlctl

import (
	"crypto/rand"
	"fmt"
	"math/big"

	"github.com/youtube/vitess/go/vt/dbconfigs"
)

// CreateMysqld returns a Mysqld object to use for working with a MySQL
// installation that hasn't been set up yet. This will generate a new my.cnf
// from scratch and use that to call NewMysqld().
func CreateMysqld(tabletUID uint32, mysqlSocket string, mysqlPort int32, dbconfigFlags dbconfigs.DBConfigFlag) (*Mysqld, error) {
	// Choose a random MySQL server-id, since this is a fresh data dir.
	// We don't want to use the tablet UID as the MySQL server-id,
	// because reusing server-ids is not safe.
	//
	// For example, if a tablet comes back with an empty data dir, it will restore
	// from backup and then connect to the master. But if this tablet has the same
	// server-id as before, and if this tablet was recently a master, then it can
	// lose data by skipping binlog events due to replicate-same-server-id=FALSE,
	// which is the default setting.
	mysqlServerID, err := randomServerID()
	if err != nil {
		return nil, fmt.Errorf("couldn't generate random MySQL server_id: %v", err)
	}
	mycnf := NewMycnf(tabletUID, mysqlServerID, mysqlPort)
	if mysqlSocket != "" {
		mycnf.SocketFile = mysqlSocket
	}

	dbcfgs, err := dbconfigs.Init(mycnf.SocketFile, dbconfigFlags)
	if err != nil {
		return nil, fmt.Errorf("couldn't Init dbconfigs: %v", err)
	}

	return NewMysqld("Dba", "App", mycnf, &dbcfgs.Dba, &dbcfgs.App.ConnParams, &dbcfgs.Repl), nil
}

// OpenMysqld returns a Mysqld object to use for working with a MySQL
// installation that already exists. This will look for an existing my.cnf file
// and use that to call NewMysqld().
func OpenMysqld(tabletUID uint32, dbconfigFlags dbconfigs.DBConfigFlag) (*Mysqld, error) {
	mycnf, err := ReadMycnf(mycnfFile(tabletUID))
	if err != nil {
		return nil, fmt.Errorf("couldn't read my.cnf file: %v", err)
	}

	dbcfgs, err := dbconfigs.Init(mycnf.SocketFile, dbconfigFlags)
	if err != nil {
		return nil, fmt.Errorf("couldn't Init dbconfigs: %v", err)
	}

	return NewMysqld("Dba", "App", mycnf, &dbcfgs.Dba, &dbcfgs.App.ConnParams, &dbcfgs.Repl), nil
}

// randomServerID generates a random MySQL server_id.
//
// The returned ID will be in the range [100, 2^32).
// It avoids 0 because that's reserved for mysqlbinlog dumps.
// It also avoids 1-99 because low numbers are used for fake slave connections.
// See NewSlaveConnection() in slave_connection.go for more on that.
func randomServerID() (uint32, error) {
	// rand.Int(_, max) returns a value in the range [0, max).
	bigN, err := rand.Int(rand.Reader, big.NewInt(1<<32-100))
	if err != nil {
		return 0, err
	}
	n := bigN.Uint64()
	// n is in the range [0, 2^32 - 100).
	// Add back 100 to put it in the range [100, 2^32).
	return uint32(n + 100), nil
}
