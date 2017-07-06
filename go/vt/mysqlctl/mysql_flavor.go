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
	"fmt"
	"os"

	log "github.com/golang/glog"
	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/mysql"
)

/*
This file handles the differences between flavors of mysql.
*/

// MysqlFlavor is the abstract interface for a flavor.
type MysqlFlavor interface {
	// VersionMatch returns true if the version string (from
	// SELECT VERSION()) represents a server that this flavor
	// knows how to talk to.
	VersionMatch(version string) bool

	// SlaveStatus returns the ReplicationStatus of a slave.
	SlaveStatus(mysqld *Mysqld) (Status, error)

	// SetMasterCommands returns the commands to use the provided master
	// as the new master (without changing any GTID position).
	// It is guaranteed to be called with replication stopped.
	// It should not start or stop replication.
	SetMasterCommands(params *mysql.ConnParams, masterHost string, masterPort int, masterConnectRetry int) ([]string, error)

	// ParseGTID parses a GTID in the canonical format of this
	// MySQL flavor into a replication.GTID interface value.
	ParseGTID(string) (mysql.GTID, error)

	// ParseReplicationPosition parses a replication position in
	// the canonical format of this MySQL flavor into a
	// mysql.Position struct.
	ParseReplicationPosition(string) (mysql.Position, error)

	// MakeBinlogEvent takes a raw packet from the MySQL binlog
	// stream connection and returns a BinlogEvent through which
	// the packet can be examined.
	MakeBinlogEvent(buf []byte) mysql.BinlogEvent

	// WaitMasterPos waits until slave replication reaches at
	// least targetPos.
	WaitMasterPos(ctx context.Context, mysqld *Mysqld, targetPos mysql.Position) error

	// EnableBinlogPlayback prepares the server to play back
	// events from a binlog stream.  Whatever it does for a given
	// flavor, it must be idempotent.
	EnableBinlogPlayback(mysqld *Mysqld) error

	// DisableBinlogPlayback returns the server to the normal
	// state after playback is done.  Whatever it does for a given
	// flavor, it must be idempotent.
	DisableBinlogPlayback(mysqld *Mysqld) error
}

var mysqlFlavors = make(map[string]MysqlFlavor)

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
	qr, err := mysqld.FetchSuperQuery(context.TODO(), "SELECT VERSION()")
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
	mysqld.mutex.Lock()
	defer mysqld.mutex.Unlock()

	if mysqld.mysqlFlavor == nil {
		flavor, err := mysqld.detectFlavor()
		if err != nil {
			return nil, fmt.Errorf("couldn't detect MySQL flavor: %v", err)
		}
		mysqld.mysqlFlavor = flavor
	}
	return mysqld.mysqlFlavor, nil
}
