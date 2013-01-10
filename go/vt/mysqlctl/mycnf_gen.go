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

	"code.google.com/p/vitess/go/vt/env"
)

type VtReplParams struct {
	StartKey string
	EndKey   string
}

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
func NewMycnf(uid uint32, mysqlPort int, vtRepl VtReplParams) *Mycnf {
	cnf := new(Mycnf)
	cnf.path = MycnfFile(uid)
	tabletDir := TabletDir(uid)
	cnf.ServerId = uid
	cnf.MysqlPort = mysqlPort
	cnf.DataDir = path.Join(tabletDir, dataDir)
	cnf.InnodbDataHomeDir = path.Join(tabletDir, innodbDataSubdir)
	cnf.InnodbLogGroupHomeDir = path.Join(tabletDir, innodbLogSubdir)
	cnf.SocketFile = path.Join(tabletDir, "mysql.sock")
	cnf.StartKey = vtRepl.StartKey
	cnf.EndKey = vtRepl.EndKey
	cnf.ErrorLogPath = path.Join(tabletDir, "error.log")
	cnf.SlowLogPath = path.Join(tabletDir, "slow-query.log")
	cnf.RelayLogPath = path.Join(tabletDir, relayLogDir,
		fmt.Sprintf("vt-%010d-relay-bin", cnf.ServerId))
	cnf.RelayLogIndexPath = cnf.RelayLogPath + ".index"
	cnf.RelayLogInfoPath = path.Join(tabletDir, relayLogDir, "relay-log.info")
	cnf.BinLogPath = path.Join(tabletDir, binLogDir,
		fmt.Sprintf("vt-%010d-bin", cnf.ServerId))
	cnf.BinLogIndexPath = cnf.BinLogPath + ".index"
	cnf.MasterInfoFile = path.Join(tabletDir, "master.info")
	cnf.PidFile = path.Join(tabletDir, "mysql.pid")
	cnf.TmpDir = path.Join(tabletDir, "tmp")
	cnf.SlaveLoadTmpDir = cnf.TmpDir
	return cnf
}

func TabletDir(uid uint32) string {
	return fmt.Sprintf("%s/vt_%010d", env.VtDataRoot(), uid)
}

func SnapshotDir(uid uint32) string {
	return fmt.Sprintf("%s/%s/vt_%010d", env.VtDataRoot(), snapshotDir, uid)
}

func MycnfFile(uid uint32) string {
	return path.Join(TabletDir(uid), "my.cnf")
}

func TopLevelDirs() []string {
	return []string{dataDir, innodbDir, relayLogDir, binLogDir}
}

func DirectoryList(cnf *Mycnf) []string {
	return []string{
		cnf.DataDir,
		cnf.InnodbDataHomeDir,
		cnf.InnodbLogGroupHomeDir,
		cnf.TmpDir,
		path.Join(TabletDir(cnf.ServerId), relayLogDir),
		path.Join(TabletDir(cnf.ServerId), binLogDir),
	}
}

// Join cnf files cnfPaths and subsitute in the right values.
func MakeMycnf(mycnf *Mycnf, cnfFiles []string) (string, error) {
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
	return fillMycnfTemplate(mycnf, myTemplateSource.String())
}

func fillMycnfTemplate(mycnf *Mycnf, tmplSrc string) (string, error) {
	myTemplate, err := template.New("").Parse(tmplSrc)
	if err != nil {
		return "", err
	}
	mycnfData := new(bytes.Buffer)
	err = myTemplate.Execute(mycnfData, mycnf)
	if err != nil {
		return "", err
	}
	return mycnfData.String(), nil
}
