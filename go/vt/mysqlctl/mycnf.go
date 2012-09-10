// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

/*
  Generate my.cnf files from templates.
*/

package mysqlctl

import (
	"bufio"
	"bytes"

	"fmt"
	"io"
	"io/ioutil"
	"net"
	"os"
	"path"
	"strconv"
	"strings"
	"text/template"
)

type VtReplParams struct {
	TabletHost string
	TabletPort int
	StartKey   string
	EndKey     string
}

func (vtrp VtReplParams) TabletAddr() string {
	return fmt.Sprintf("%v:%v", vtrp.TabletHost, vtrp.TabletPort)
}

type Mycnf struct {
	ServerId              uint
	TabletDir             string
	SnapshotDir           string
	DataDir               string
	MycnfFile             string
	InnodbDataHomeDir     string
	InnodbLogGroupHomeDir string
	DatabaseName          string // for replication FIXME(msolomon) should not be needed
	SocketFile            string
	MysqlPort             int
	VtHost                string
	VtPort                int
	StartKey              string
	EndKey                string
}

const (
	VtDataRoot       = "/vt"
	snapshotDir      = "snapshot"
	dataDir          = "data"
	innodbDir        = "innodb"
	relayLogDir      = "relay-logs"
	binLogDir        = "bin-logs"
	innodbDataSubdir = "innodb/data"
	innodbLogSubdir  = "innodb/log"
)

/* uid is a unique id for a particular tablet - it must be unique within the
tabletservers deployed within a keyspace, lest there be collisions on disk.
 mysqldPort needs to be unique per instance per machine (shocking) but choosing
 this sensibly has nothing to do with the config, so I'll punt.
*/
func NewMycnf(uid uint, mysqlPort int, vtRepl VtReplParams) *Mycnf {
	cnf := new(Mycnf)
	cnf.ServerId = uid
	cnf.MysqlPort = mysqlPort
	cnf.TabletDir = fmt.Sprintf("%s/vt_%010d", VtDataRoot, uid)
	cnf.SnapshotDir = fmt.Sprintf("%s/%s/vt_%010d", VtDataRoot, snapshotDir, uid)
	cnf.DataDir = path.Join(cnf.TabletDir, dataDir)
	cnf.MycnfFile = path.Join(cnf.TabletDir, "my.cnf")
	cnf.InnodbDataHomeDir = path.Join(cnf.TabletDir, innodbDataSubdir)
	cnf.InnodbLogGroupHomeDir = path.Join(cnf.TabletDir, innodbLogSubdir)
	cnf.SocketFile = path.Join(cnf.TabletDir, "mysql.sock")
	cnf.VtHost = vtRepl.TabletHost
	cnf.VtPort = vtRepl.TabletPort
	cnf.StartKey = vtRepl.StartKey
	cnf.EndKey = vtRepl.EndKey
	return cnf
}

func (cnf *Mycnf) TopLevelDirs() []string {
	return []string{dataDir, innodbDir, relayLogDir, binLogDir}
}

func (cnf *Mycnf) DirectoryList() []string {
	return []string{
		cnf.DataDir,
		cnf.InnodbDataHomeDir,
		cnf.InnodbLogGroupHomeDir,
		cnf.relayLogDir(),
		cnf.binLogDir(),
	}
}

func (cnf *Mycnf) ErrorLogPath() string {
	return path.Join(cnf.TabletDir, "error.log")
}

func (cnf *Mycnf) SlowLogPath() string {
	return path.Join(cnf.TabletDir, "slow-query.log")
}

func (cnf *Mycnf) relayLogDir() string {
	return path.Join(cnf.TabletDir, relayLogDir)
}

func (cnf *Mycnf) RelayLogPath() string {
	return path.Join(cnf.relayLogDir(),
		fmt.Sprintf("vt-%010d-relay-bin", cnf.ServerId))
}

func (cnf *Mycnf) RelayLogIndexPath() string {
	return cnf.RelayLogPath() + ".index"
}

func (cnf *Mycnf) RelayLogInfoPath() string {
	return path.Join(cnf.TabletDir, "relay-logs", "relay.info")
}

func (cnf *Mycnf) binLogDir() string {
	return path.Join(cnf.TabletDir, binLogDir)
}

func (cnf *Mycnf) BinLogPath() string {
	return path.Join(cnf.binLogDir(),
		fmt.Sprintf("vt-%010d-bin", cnf.ServerId))
}

func (cnf *Mycnf) BinLogPathForId(fileid int) string {
	return path.Join(cnf.binLogDir(),
		fmt.Sprintf("vt-%010d-bin.%06d", cnf.ServerId, fileid))
}

func (cnf *Mycnf) BinLogIndexPath() string {
	return cnf.BinLogPath() + ".index"
}

func (cnf *Mycnf) MasterInfoPath() string {
	return path.Join(cnf.TabletDir, "master.info")
}

func (cnf *Mycnf) PidFile() string {
	return path.Join(cnf.TabletDir, "mysql.pid")
}

func (cnf *Mycnf) MysqlAddr() string {
	return fmt.Sprintf("%v:%v", fqdn(), cnf.MysqlPort)
}

func fqdn() string {
	hostname, err := os.Hostname()
	if err != nil {
		panic(err)
	}

	cname, err := net.LookupCNAME(hostname)
	if err != nil {
		panic(err)
	}
	return strings.TrimRight(cname, ".")
}

/*
  Join cnf files cnfPaths and subsitute in the right values.
*/
func MakeMycnf(cnfFiles []string, mycnf *Mycnf, header string) (string, error) {
	myTemplateSource := new(bytes.Buffer)
	for _, line := range strings.Split(header, "\n") {
		fmt.Fprintf(myTemplateSource, "## %v\n", strings.TrimSpace(line))
	}
	myTemplateSource.WriteString("[mysqld]\n")
	for _, path := range cnfFiles {
		data, dataErr := ioutil.ReadFile(path)
		if dataErr != nil {
			return "", dataErr
		}
		myTemplateSource.WriteString("## " + path + "\n")
		myTemplateSource.Write(data)
	}

	myTemplate, err := template.New("").Parse(myTemplateSource.String())
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

/* Create a config for this instance. Search cnfFiles for the appropriate
cnf template files.
*/
func MakeMycnfForMysqld(mysqld *Mysqld, cnfFiles, header string) (string, error) {
	// FIXME(msolomon) determine config list from mysqld struct
	cnfs := []string{"default", "master", "replica"}
	paths := make([]string, len(cnfs))
	for i, name := range cnfs {
		paths[i] = fmt.Sprintf("%v/%v.cnf", cnfFiles, name)
	}
	return MakeMycnf(paths, mysqld.config, header)
}

func ReadMycnf(cnfFile string) (*Mycnf, error) {
	f, err := os.Open(cnfFile)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	buf := bufio.NewReader(f)
	mycnf := new(Mycnf)

	for {
		line, _, err := buf.ReadLine()
		if err == io.EOF {
			break
		}
		line = bytes.TrimSpace(line)
		if bytes.HasPrefix(line, []byte("server-id")) {
			serverId, err := strconv.Atoi(string(bytes.TrimSpace(bytes.Split(line, []byte("="))[1])))
			if err != nil {
				return nil, fmt.Errorf("mycnf: failed to convert server-id %v", err)
			}
			mycnf.ServerId = uint(serverId)
		} else if bytes.HasPrefix(line, []byte("port")) {
			port, err := strconv.Atoi(string(bytes.TrimSpace(bytes.Split(line, []byte("="))[1])))
			if err != nil {
				return nil, fmt.Errorf("mycnf: failed to convert port %v", err)
			}
			mycnf.MysqlPort = port
		} else if bytes.HasPrefix(line, []byte("datadir")) {
			mycnf.DataDir = string(bytes.TrimSpace(bytes.Split(line, []byte("="))[1]))
		} else if bytes.HasPrefix(line, []byte("innodb_log_group_home_dir")) {
			mycnf.InnodbLogGroupHomeDir = string(bytes.TrimSpace(bytes.Split(line, []byte("="))[1]))
		} else if bytes.HasPrefix(line, []byte("innodb_data_home_dir")) {
			mycnf.InnodbDataHomeDir = string(bytes.TrimSpace(bytes.Split(line, []byte("="))[1]))
		} else if bytes.HasPrefix(line, []byte("socket")) {
			mycnf.SocketFile = string(bytes.TrimSpace(bytes.Split(line, []byte("="))[1]))
		}
	}

	// Make sure we run the correct initialization.
	vtMycnf := NewMycnf(mycnf.ServerId, mycnf.MysqlPort, VtReplParams{})

	// Apply overrides.
	vtMycnf.DataDir = mycnf.DataDir
	vtMycnf.InnodbDataHomeDir = mycnf.InnodbDataHomeDir
	vtMycnf.InnodbLogGroupHomeDir = mycnf.InnodbLogGroupHomeDir
	vtMycnf.SocketFile = mycnf.SocketFile

	return vtMycnf, nil
}
