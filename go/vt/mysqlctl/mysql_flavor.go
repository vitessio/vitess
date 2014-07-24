// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package mysqlctl

import (
	"os"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/vt/mysqlctl/proto"
)

/*
This file handles the differences between flavors of mysql.
*/

// MysqlFlavor is the abstract interface for a flavor.
type MysqlFlavor interface {
	// MasterStatus fills in the ReplicationPosition structure.
	// It has two components: the result of a standard 'SHOW MASTER STATUS'
	// and the corresponding transaction group id.
	MasterStatus(mysqld *Mysqld) (*proto.ReplicationPosition, error)

	// PromoteSlaveCommands returns the commands to run to change
	// a slave into a master
	PromoteSlaveCommands() []string

	// ParseGTID converts a string containing a GTID in the canonical format of
	// this MySQL flavor into a proto.GTID interface value.
	ParseGTID(string) (proto.GTID, error)
}

var mysqlFlavors map[string]MysqlFlavor = make(map[string]MysqlFlavor)

func mysqlFlavor() MysqlFlavor {
	f := os.Getenv("MYSQL_FLAVOR")
	if f == "" {
		if len(mysqlFlavors) == 1 {
			for k, v := range mysqlFlavors {
				log.Infof("Only one MySQL flavor declared, using %v", k)
				return v
			}
		}
		if v, ok := mysqlFlavors["GoogleMysql"]; ok {
			log.Info("MYSQL_FLAVOR is not set, using GoogleMysql flavor by default")
			return v
		}
		log.Fatal("MYSQL_FLAVOR is not set, and no GoogleMysql flavor registered")
	}
	if v, ok := mysqlFlavors[f]; ok {
		log.Infof("Using MySQL flavor %v", f)
		return v
	}
	log.Fatalf("MYSQL_FLAVOR is set to unknown value %v", f)
	panic("")
}
