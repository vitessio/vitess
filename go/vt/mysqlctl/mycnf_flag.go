/*
Copyright 2019 The Vitess Authors.

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
	"errors"
	"flag"
	"os"

	"vitess.io/vitess/go/vt/log"
)

// This file handles using command line flags to create a Mycnf object.
// Since whoever links with this module doesn't necessarely need the flags,
// RegisterFlags needs to be called explicitly to set the flags up.

var (
	// the individual command line parameters
	flagServerID              *int
	flagMysqlPort             *int
	flagDataDir               *string
	flagInnodbDataHomeDir     *string
	flagInnodbLogGroupHomeDir *string
	flagSocketFile            *string
	flagGeneralLogPath        *string
	flagErrorLogPath          *string
	flagSlowLogPath           *string
	flagRelayLogPath          *string
	flagRelayLogIndexPath     *string
	flagRelayLogInfoPath      *string
	flagBinLogPath            *string
	flagMasterInfoFile        *string
	flagPidFile               *string
	flagTmpDir                *string
	flagSecureFilePriv        *string

	// the file to use to specify them all
	flagMycnfFile *string
)

// RegisterFlags registers the command line flags for
// specifying the values of a mycnf config file. See NewMycnfFromFlags
// to get the supported modes.
func RegisterFlags() {
	flagServerID = flag.Int("mycnf_server_id", 0, "mysql server id of the server (if specified, mycnf-file will be ignored)")
	flagMysqlPort = flag.Int("mycnf_mysql_port", 0, "port mysql is listening on")
	flagDataDir = flag.String("mycnf_data_dir", "", "data directory for mysql")
	flagInnodbDataHomeDir = flag.String("mycnf_innodb_data_home_dir", "", "Innodb data home directory")
	flagInnodbLogGroupHomeDir = flag.String("mycnf_innodb_log_group_home_dir", "", "Innodb log group home directory")
	flagSocketFile = flag.String("mycnf_socket_file", "", "mysql socket file")
	flagGeneralLogPath = flag.String("mycnf_general_log_path", "", "mysql general log path")
	flagErrorLogPath = flag.String("mycnf_error_log_path", "", "mysql error log path")
	flagSlowLogPath = flag.String("mycnf_slow_log_path", "", "mysql slow query log path")
	flagRelayLogPath = flag.String("mycnf_relay_log_path", "", "mysql relay log path")
	flagRelayLogIndexPath = flag.String("mycnf_relay_log_index_path", "", "mysql relay log index path")
	flagRelayLogInfoPath = flag.String("mycnf_relay_log_info_path", "", "mysql relay log info path")
	flagBinLogPath = flag.String("mycnf_bin_log_path", "", "mysql binlog path")
	flagMasterInfoFile = flag.String("mycnf_master_info_file", "", "mysql master.info file")
	flagPidFile = flag.String("mycnf_pid_file", "", "mysql pid file")
	flagTmpDir = flag.String("mycnf_tmp_dir", "", "mysql tmp directory")
	flagSecureFilePriv = flag.String("mycnf_secure_file_priv", "", "mysql path for loading secure files")

	flagMycnfFile = flag.String("mycnf-file", "", "path to my.cnf, if reading all config params from there")
}

// NewMycnfFromFlags creates a Mycnf object from command-line flags.
//
// 1. First, create a Mycnf object from NewMycnf.
// 2. Then, merge in any values from mycnf files.
//   - If --mycnf_file is specified, used that. Return an error if the
//     file does not exist or cannot be read.
//   - Otherwise, use a default mycnf file directived from the tablet UID.
//     Return an error if the file exists but cannot be read.
//
// 3. Then, merge in any provided command-line flags.
//
// RegisterFlags should have been called before calling
// this, otherwise we'll panic.
func NewMycnfFromFlags(tabletUID uint32, mysqlPort int32) (*Mycnf, error) {
	var cnfs []*Mycnf

	cnfs = append(cnfs, NewMycnf(tabletUID, mysqlPort))

	// Get a Mycnf file.
	file := NewMycnf(tabletUID, 0)
	var path string
	if *flagMycnfFile == "" {
		path = MycnfFile(tabletUID)
		log.Infof("No mycnf_server_id, no mycnf-file specified, using default config for server id %v: %v", tabletUID, path)
	} else {
		path = *flagMycnfFile
		log.Infof("No mycnf_server_id specified, using mycnf-file file: %v", path)
	}
	file.Path = path
	file, err := ReadMycnf(file)
	if err != nil {
		if *flagMycnfFile != "" || !errors.Is(err, os.ErrNotExist) {
			log.Errorf("Failed to read mycnf file: %v", err)
			return nil, err
		}
		log.Errorf("File does not exist: %s", path)
	} else {
		cnfs = append(cnfs, file)
	}

	cnfs = append(cnfs, &Mycnf{
		ServerID:              uint32(*flagServerID),
		MysqlPort:             int32(*flagMysqlPort),
		DataDir:               *flagDataDir,
		InnodbDataHomeDir:     *flagInnodbDataHomeDir,
		InnodbLogGroupHomeDir: *flagInnodbLogGroupHomeDir,
		SocketFile:            *flagSocketFile,
		GeneralLogPath:        *flagGeneralLogPath,
		ErrorLogPath:          *flagErrorLogPath,
		SlowLogPath:           *flagSlowLogPath,
		RelayLogPath:          *flagRelayLogPath,
		RelayLogIndexPath:     *flagRelayLogIndexPath,
		RelayLogInfoPath:      *flagRelayLogInfoPath,
		BinLogPath:            *flagBinLogPath,
		MasterInfoFile:        *flagMasterInfoFile,
		PidFile:               *flagPidFile,
		TmpDir:                *flagTmpDir,
		SecureFilePriv:        *flagSecureFilePriv,
	})

	return merge(cnfs...), nil
}
