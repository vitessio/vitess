// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

//  Generate my.cnf files from templates.

package mysqlctl

import (
	"bytes"
	"fmt"
	"io/ioutil"
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
	snapshotDir      = "snapshot"
)

// NewMycnf fills the Mycnf structure with vt root paths and derived values.
// This is used to fill out the cnfTemplate values and generate my.cnf.
// uid is a unique id for a particular tablet - it must be unique within the
// tabletservers deployed within a keyspace, lest there be collisions on disk.
// mysqldPort needs to be unique per instance per machine.
func NewMycnf(uid uint32, mysqlPort int32) *Mycnf {
	cnf := new(Mycnf)
	cnf.path = mycnfFile(uid)
	tabletDir := TabletDir(uid)
	cnf.ServerID = uid
	cnf.MysqlPort = mysqlPort
	cnf.DataDir = path.Join(tabletDir, dataDir)
	cnf.InnodbDataHomeDir = path.Join(tabletDir, innodbDataSubdir)
	cnf.InnodbLogGroupHomeDir = path.Join(tabletDir, innodbLogSubdir)
	cnf.SocketFile = path.Join(tabletDir, "mysql.sock")
	cnf.ErrorLogPath = path.Join(tabletDir, "error.log")
	cnf.SlowLogPath = path.Join(tabletDir, "slow-query.log")
	cnf.RelayLogPath = path.Join(tabletDir, relayLogDir,
		fmt.Sprintf("vt-%010d-relay-bin", cnf.ServerID))
	cnf.RelayLogIndexPath = cnf.RelayLogPath + ".index"
	cnf.RelayLogInfoPath = path.Join(tabletDir, relayLogDir, "relay-log.info")
	cnf.BinLogPath = path.Join(tabletDir, binLogDir,
		fmt.Sprintf("vt-%010d-bin", cnf.ServerID))
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

// SnapshotDir returns the default directory for a tablet's snapshots
func SnapshotDir(uid uint32) string {
	return fmt.Sprintf("%s/%s/vt_%010d", env.VtDataRoot(), snapshotDir, uid)
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
		path.Join(TabletDir(cnf.ServerID), relayLogDir),
		path.Join(TabletDir(cnf.ServerID), binLogDir),
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
