// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package mysqlctl

import (
	"fmt"
	"os"
	"time"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/mysql"
	blproto "github.com/youtube/vitess/go/vt/binlog/proto"
	"github.com/youtube/vitess/go/vt/mysqlctl/proto"
)

/*
This file handles the differences between flavors of mysql.
*/

// MysqlFlavor is the abstract interface for a flavor.
type MysqlFlavor interface {
	// VersionMatch returns true if the version string (from SELECT VERSION())
	// represents a server that this flavor knows how to talk to.
	VersionMatch(version string) bool

	// MasterPosition returns the ReplicationPosition of a master.
	MasterPosition(mysqld *Mysqld) (proto.ReplicationPosition, error)

	// SlaveStatus returns the ReplicationStatus of a slave.
	SlaveStatus(mysqld *Mysqld) (*proto.ReplicationStatus, error)

	// PromoteSlaveCommands returns the commands to run to change
	// a slave into a master.
	PromoteSlaveCommands() []string

	// StartReplicationCommands returns the commands to start replicating from
	// a given master and position as specified in a ReplicationStatus.
	StartReplicationCommands(params *mysql.ConnectionParams, status *proto.ReplicationStatus) ([]string, error)

	// ParseGTID parses a GTID in the canonical format of this MySQL flavor into
	// a proto.GTID interface value.
	ParseGTID(string) (proto.GTID, error)

	// ParseReplicationPosition parses a replication position in the canonical
	// format of this MySQL flavor into a proto.ReplicationPosition struct.
	ParseReplicationPosition(string) (proto.ReplicationPosition, error)

	// SendBinlogDumpCommand sends the flavor-specific version of the
	// COM_BINLOG_DUMP command to start dumping raw binlog events over a slave
	// connection, starting at a given GTID.
	SendBinlogDumpCommand(mysqld *Mysqld, conn *SlaveConnection, startPos proto.ReplicationPosition) error

	// MakeBinlogEvent takes a raw packet from the MySQL binlog stream connection
	// and returns a BinlogEvent through which the packet can be examined.
	MakeBinlogEvent(buf []byte) blproto.BinlogEvent

	// WaitMasterPos waits until slave replication reaches at least targetPos.
	WaitMasterPos(mysqld *Mysqld, targetPos proto.ReplicationPosition, waitTimeout time.Duration) error

	// EnableBinlogPlayback prepares the server to play back events from a binlog stream.
	// Whatever it does for a given flavor, it must be idempotent.
	EnableBinlogPlayback(mysqld *Mysqld) error

	// DisableBinlogPlayback returns the server to the normal state after playback is done.
	// Whatever it does for a given flavor, it must be idempotent.
	DisableBinlogPlayback(mysqld *Mysqld) error
}

var mysqlFlavors map[string]MysqlFlavor = make(map[string]MysqlFlavor)

// registerFlavorBuiltin adds a flavor to the map only if the name is unused.
// The flavor implementation passed to this function will only be used if there
// are no calls to registerFlavorOverride with the same flavor name.
// This should only be called from the init() goroutine.
func registerFlavorBuiltin(name string, flavor MysqlFlavor) {
	if _, ok := mysqlFlavors[name]; !ok {
		mysqlFlavors[name] = flavor
	}
}

// registerFlavorOverride adds a flavor to the map, overriding any built-in
// implementations registered with registerFlavorBuiltin. There should only
// be one override per name, or else there's no guarantee which will win.
// This should only be called from the init() goroutine.
func registerFlavorOverride(name string, flavor MysqlFlavor) {
	mysqlFlavors[name] = flavor
}

// detectFlavor decides which flavor to assume, based on the MYSQL_FLAVOR
// environment variable. If that variable is empty or unset, we will try to
// auto-detect the flavor.
func (mysqld *Mysqld) detectFlavor() (MysqlFlavor, error) {
	// Check environment variable, which overrides auto-detect.
	if env := os.Getenv("MYSQL_FLAVOR"); env != "" {
		if flavor, ok := mysqlFlavors[env]; ok {
			log.Infof("Using MySQL flavor %v (set by MYSQL_FLAVOR)", env)
			return flavor, nil
		}
		return nil, fmt.Errorf("Unknown flavor (MYSQL_FLAVOR=%v)", env)
	}

	// If no environment variable set, fall back to auto-detect.
	log.Infof("MYSQL_FLAVOR empty or unset, attempting to auto-detect...")
	qr, err := mysqld.fetchSuperQuery("SELECT VERSION()")
	if err != nil {
		return nil, fmt.Errorf("couldn't SELECT VERSION(): %v", err)
	}
	if len(qr.Rows) != 1 || len(qr.Rows[0]) != 1 {
		return nil, fmt.Errorf("unexpected result for SELECT VERSION(): %#v", qr)
	}
	version := qr.Rows[0][0].String()
	log.Infof("SELECT VERSION() = %s", version)

	for name, flavor := range mysqlFlavors {
		if flavor.VersionMatch(version) {
			log.Infof("Using MySQL flavor %v (auto-detect match)", name)
			return flavor, nil
		}
	}

	return nil, fmt.Errorf("MYSQL_FLAVOR empty or unset, no auto-detect match found for VERSION() = %v", version)
}

func (mysqld *Mysqld) flavor() (MysqlFlavor, error) {
	mysqld.mysqlFlavorMutex.Lock()
	defer mysqld.mysqlFlavorMutex.Unlock()

	if mysqld.mysqlFlavor == nil {
		flavor, err := mysqld.detectFlavor()
		if err != nil {
			return nil, fmt.Errorf("couldn't detect MySQL flavor: %v", err)
		}
		mysqld.mysqlFlavor = flavor
	}
	return mysqld.mysqlFlavor, nil
}
