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
  Generate my.cnf files from templates.
*/

package mysqlctl

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"os"
	"strconv"
)

// Mycnf is a memory structure that contains a bunch of interesting
// parameters to start mysqld. It can be used to generate standard
// my.cnf files from a server id and mysql port. It can also be
// populated from an existing my.cnf, or by command line parameters.
type Mycnf struct {
	// ServerID is the unique id for this server.
	// Used to create a bunch of named directories.
	ServerID uint32

	// MysqlPort is the port for the MySQL server running on this machine.
	// It is mainly used to communicate with topology server.
	MysqlPort int32

	// DataDir is where the table files are
	// (used by vt software for Clone)
	DataDir string

	// InnodbDataHomeDir is the data directory for innodb.
	// (used by vt software for Clone)
	InnodbDataHomeDir string

	// InnodbLogGroupHomeDir is the logs directory for innodb.
	// (used by vt software for Clone)
	InnodbLogGroupHomeDir string

	// SocketFile is the path to the local mysql.sock file.
	// (used by vt software to check server is running)
	SocketFile string

	// GeneralLogPath is the path to store general logs at,
	// if general-log is enabled.
	// (unused by vt software for now)
	GeneralLogPath string

	// ErrorLogPath is the path to store error logs at.
	// (unused by vt software for now)
	ErrorLogPath string

	// SlowLogPath is the slow query log path
	// (unused by vt software for now)
	SlowLogPath string

	// RelayLogPath is the path of the relay logs
	// (unused by vt software for now)
	RelayLogPath string

	// RelayLogIndexPath is the file name for the relay log index
	// (unused by vt software for now)
	RelayLogIndexPath string

	// RelayLogInfoPath is the file name for the relay log info file
	// (unused by vt software for now)
	RelayLogInfoPath string

	// BinLogPath is the base path for binlogs
	// (used by vt software for binlog streaming)
	BinLogPath string

	// MasterInfoFile is the master.info file location.
	// (unused by vt software for now)
	MasterInfoFile string

	// PidFile is the mysql.pid file location
	// (used by vt software to check server is running)
	PidFile string

	// TmpDir is where to create temporary tables
	// (unused by vt software for now)
	TmpDir string

	// SlaveLoadTmpDir is where to create tmp files for replication
	// (unused by vt software for now)
	SlaveLoadTmpDir string

	mycnfMap map[string]string
	path     string // the actual path that represents this mycnf
}

func (cnf *Mycnf) lookup(key string) string {
	key = normKey([]byte(key))
	return cnf.mycnfMap[key]
}

func (cnf *Mycnf) lookupWithDefault(key, defaultVal string) string {
	val := cnf.lookup(key)
	if val == "" {
		if defaultVal == "" {
			panic(fmt.Errorf("Value for key '%v' not set and no default value set", key))
		}
		return defaultVal
	}
	return val
}

func (cnf *Mycnf) lookupAndCheck(key string) string {
	return cnf.lookupWithDefault(key, "")
}

func normKey(bkey []byte) string {
	// FIXME(msolomon) People are careless about hyphen vs underscore - we should normalize.
	// But you have to normalize to hyphen, or mysqld_safe can fail.
	return string(bytes.Replace(bytes.TrimSpace(bkey), []byte("_"), []byte("-"), -1))
}

// ReadMycnf will read an existing my.cnf from disk, and update the passed in Mycnf object
// with values from the my.cnf on disk.
func ReadMycnf(mycnf *Mycnf) (*Mycnf, error) {
	var err error
	defer func(err *error) {
		if x := recover(); x != nil {
			*err = x.(error)
		}
	}(&err)

	f, err := os.Open(mycnf.path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	buf := bufio.NewReader(f)
	if err != nil {
		return nil, err
	}
	mycnf.mycnfMap = make(map[string]string)
	var lval, rval string
	var parts [][]byte

	for {
		line, _, err := buf.ReadLine()
		if err == io.EOF {
			break
		}
		line = bytes.TrimSpace(line)

		parts = bytes.Split(line, []byte("="))
		if len(parts) < 2 {
			continue
		}
		lval = normKey(parts[0])
		rval = string(bytes.TrimSpace(parts[1]))
		mycnf.mycnfMap[lval] = rval
	}

	serverIDStr := mycnf.lookupAndCheck("server-id")
	serverID, err := strconv.Atoi(serverIDStr)
	if err != nil {
		return nil, fmt.Errorf("Failed to convert server-id %v", err)
	}
	mycnf.ServerID = uint32(serverID)

	portStr := mycnf.lookupAndCheck("port")
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return nil, fmt.Errorf("Failed: failed to convert port %v", err)
	}

	mycnf.MysqlPort = int32(port)
	mycnf.DataDir = mycnf.lookupWithDefault("datadir", mycnf.DataDir)
	mycnf.InnodbDataHomeDir = mycnf.lookupWithDefault("innodb_data_home_dir", mycnf.InnodbDataHomeDir)
	mycnf.InnodbLogGroupHomeDir = mycnf.lookupWithDefault("innodb_log_group_home_dir", mycnf.InnodbLogGroupHomeDir)
	mycnf.SocketFile = mycnf.lookupWithDefault("socket", mycnf.SocketFile)
	mycnf.GeneralLogPath = mycnf.lookupWithDefault("general_log_file", mycnf.GeneralLogPath)
	mycnf.ErrorLogPath = mycnf.lookupWithDefault("log-error", mycnf.ErrorLogPath)
	mycnf.SlowLogPath = mycnf.lookupWithDefault("slow-query-log-file", mycnf.SlowLogPath)
	mycnf.RelayLogPath = mycnf.lookupWithDefault("relay-log", mycnf.RelayLogPath)
	mycnf.RelayLogIndexPath = mycnf.lookupWithDefault("relay-log-index", mycnf.RelayLogIndexPath)
	mycnf.RelayLogInfoPath = mycnf.lookupWithDefault("relay-log-info-file", mycnf.RelayLogInfoPath)
	mycnf.BinLogPath = mycnf.lookupWithDefault("log-bin", mycnf.BinLogPath)
	mycnf.MasterInfoFile = mycnf.lookupWithDefault("master-info-file", mycnf.MasterInfoFile)
	mycnf.PidFile = mycnf.lookupWithDefault("pid-file", mycnf.PidFile)
	mycnf.TmpDir = mycnf.lookupWithDefault("tmpdir", mycnf.TmpDir)
	mycnf.SlaveLoadTmpDir = mycnf.lookupWithDefault("slave_load_tmpdir", mycnf.SlaveLoadTmpDir)

	return mycnf, nil
}
