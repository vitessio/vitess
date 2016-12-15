// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

//  Generate my.cnf files from templates.

package mysqlctl

import (
	"bytes"
	"crypto/rand"
	"fmt"
	"io/ioutil"
	"math/big"
	"path"
	"text/template"

	"github.com/youtube/vitess/go/vt/env"
)

// This files handles the creation of Mycnf objects for the default 'vt'
// file structure. These path are used by the mysqlctl commands.
//
// The default 'vt' file structure is as follows:
// - the root is specified by the environment variable VTDATAROOT,
//   and defaults to /vt
// - each tablet with uid NNNNNNNNNN is located in <root>/vt_NNNNNNNNNN
// - in that tablet directory, there is a my.cnf file for the mysql instance,
//   and 'data', 'innodb', 'relay-logs', 'bin-logs' directories.
// - these sub-directories might be symlinks to other places,
//   see comment for createTopDir, to allow some data types to be on
//   different disk partitions.

const (
	dataDir          = "data"
	innodbDir        = "innodb"
	relayLogDir      = "relay-logs"
	binLogDir        = "bin-logs"
	innodbDataSubdir = "innodb/data"
	innodbLogSubdir  = "innodb/logs"
)

// NewMycnf fills the Mycnf structure with vt root paths and derived values.
// This is used to fill out the cnfTemplate values and generate my.cnf.
// uid is a unique id for a particular tablet - it must be unique within the
// tabletservers deployed within a keyspace, lest there be collisions on disk.
// mysqldPort needs to be unique per instance per machine.
func NewMycnf(tabletUID uint32, mysqlPort int32) *Mycnf {
	cnf := new(Mycnf)
	cnf.path = mycnfFile(tabletUID)
	tabletDir := TabletDir(tabletUID)
	cnf.ServerID = tabletUID
	cnf.MysqlPort = mysqlPort
	cnf.DataDir = path.Join(tabletDir, dataDir)
	cnf.InnodbDataHomeDir = path.Join(tabletDir, innodbDataSubdir)
	cnf.InnodbLogGroupHomeDir = path.Join(tabletDir, innodbLogSubdir)
	cnf.SocketFile = path.Join(tabletDir, "mysql.sock")
	cnf.ErrorLogPath = path.Join(tabletDir, "error.log")
	cnf.SlowLogPath = path.Join(tabletDir, "slow-query.log")
	cnf.RelayLogPath = path.Join(tabletDir, relayLogDir,
		fmt.Sprintf("vt-%010d-relay-bin", tabletUID))
	cnf.RelayLogIndexPath = cnf.RelayLogPath + ".index"
	cnf.RelayLogInfoPath = path.Join(tabletDir, relayLogDir, "relay-log.info")
	cnf.BinLogPath = path.Join(tabletDir, binLogDir,
		fmt.Sprintf("vt-%010d-bin", tabletUID))
	cnf.MasterInfoFile = path.Join(tabletDir, "master.info")
	cnf.PidFile = path.Join(tabletDir, "mysql.pid")
	cnf.TmpDir = path.Join(tabletDir, "tmp")
	cnf.SlaveLoadTmpDir = cnf.TmpDir
	return cnf
}

// TabletDir returns the default directory for a tablet
func TabletDir(uid uint32) string {
	return fmt.Sprintf("%s/vt_%010d", env.VtDataRoot(), uid)
}

// mycnfFile returns the default location of the my.cnf file.
func mycnfFile(uid uint32) string {
	return path.Join(TabletDir(uid), "my.cnf")
}

// TopLevelDirs returns the list of directories in the toplevel tablet directory
// that might be located in a different place.
func TopLevelDirs() []string {
	return []string{dataDir, innodbDir, relayLogDir, binLogDir}
}

// directoryList returns the list of directories to create in an empty
// mysql instance.
func (cnf *Mycnf) directoryList() []string {
	return []string{
		cnf.DataDir,
		cnf.InnodbDataHomeDir,
		cnf.InnodbLogGroupHomeDir,
		cnf.TmpDir,
		path.Dir(cnf.RelayLogPath),
		path.Dir(cnf.BinLogPath),
	}
}

// makeMycnf will join cnf files cnfPaths and substitute in the right values.
func (cnf *Mycnf) makeMycnf(cnfFiles []string) (string, error) {
	myTemplateSource := new(bytes.Buffer)
	myTemplateSource.WriteString("[mysqld]\n")
	for _, path := range cnfFiles {
		data, dataErr := ioutil.ReadFile(path)
		if dataErr != nil {
			return "", dataErr
		}
		myTemplateSource.WriteString("## " + path + "\n")
		myTemplateSource.Write(data)
	}
	return cnf.fillMycnfTemplate(myTemplateSource.String())
}

// fillMycnfTemplate will fill in the passed in template with the values
// from Mycnf
func (cnf *Mycnf) fillMycnfTemplate(tmplSrc string) (string, error) {
	myTemplate, err := template.New("").Parse(tmplSrc)
	if err != nil {
		return "", err
	}
	mycnfData := new(bytes.Buffer)
	err = myTemplate.Execute(mycnfData, cnf)
	if err != nil {
		return "", err
	}
	return mycnfData.String(), nil
}

// RandomizeMysqlServerID generates a random MySQL server_id.
//
// The value assigned to ServerID will be in the range [100, 2^31):
// - It avoids 0 because that's reserved for mysqlbinlog dumps.
// - It also avoids 1-99 because low numbers are used for fake slave
// connections.  See NewSlaveConnection() in slave_connection.go for
// more on that.
// - It avoids the 2^31 - 2^32-1 range, as there seems to be some
// confusion there. The main MySQL documentation at:
// http://dev.mysql.com/doc/refman/5.7/en/replication-options.html
// implies serverID is a full 32 bits number. The semi-sync log line
// at startup '[Note] Start semi-sync binlog_dump to slave ...'
// interprets the server_id as signed 32-bit (shows negative numbers
// for that range).
// Such an ID may also be responsible for a mysqld crash in semi-sync code,
// although we haven't been able to verify that yet. The issue for that is:
// https://github.com/youtube/vitess/issues/2280
func (cnf *Mycnf) RandomizeMysqlServerID() error {
	// rand.Int(_, max) returns a value in the range [0, max).
	bigN, err := rand.Int(rand.Reader, big.NewInt(1<<31-100))
	if err != nil {
		return err
	}
	n := bigN.Uint64()
	// n is in the range [0, 2^31 - 100).
	// Add back 100 to put it in the range [100, 2^31).
	cnf.ServerID = uint32(n + 100)
	return nil
}
