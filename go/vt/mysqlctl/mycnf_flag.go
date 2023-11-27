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
	"github.com/spf13/pflag"

	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/servenv"
)

// This file handles using command line flags to create a Mycnf object.
// Since whoever links with this module doesn't necessarely need the flags,
// RegisterFlags needs to be called explicitly to set the flags up.

var (
	// the individual command line parameters
	flagServerID              int
	flagMysqlPort             int
	flagDataDir               string
	flagInnodbDataHomeDir     string
	flagInnodbLogGroupHomeDir string
	flagSocketFile            string
	flagGeneralLogPath        string
	flagErrorLogPath          string
	flagSlowLogPath           string
	flagRelayLogPath          string
	flagRelayLogIndexPath     string
	flagRelayLogInfoPath      string
	flagBinLogPath            string
	flagMasterInfoFile        string
	flagPidFile               string
	flagTmpDir                string
	flagSecureFilePriv        string

	// the file to use to specify them all
	flagMycnfFile string
)

// RegisterFlags registers the command line flags for
// specifying the values of a mycnf config file. See NewMycnfFromFlags
// to get the supported modes.
func RegisterFlags() {
	servenv.OnParse(func(fs *pflag.FlagSet) {
		fs.IntVar(&flagServerID, "mycnf_server_id", flagServerID, "mysql server id of the server (if specified, mycnf-file will be ignored)")
		fs.IntVar(&flagMysqlPort, "mycnf_mysql_port", flagMysqlPort, "port mysql is listening on")
		fs.StringVar(&flagDataDir, "mycnf_data_dir", flagDataDir, "data directory for mysql")
		fs.StringVar(&flagInnodbDataHomeDir, "mycnf_innodb_data_home_dir", flagInnodbDataHomeDir, "Innodb data home directory")
		fs.StringVar(&flagInnodbLogGroupHomeDir, "mycnf_innodb_log_group_home_dir", flagInnodbLogGroupHomeDir, "Innodb log group home directory")
		fs.StringVar(&flagSocketFile, "mycnf_socket_file", flagSocketFile, "mysql socket file")
		fs.StringVar(&flagGeneralLogPath, "mycnf_general_log_path", flagGeneralLogPath, "mysql general log path")
		fs.StringVar(&flagErrorLogPath, "mycnf_error_log_path", flagErrorLogPath, "mysql error log path")
		fs.StringVar(&flagSlowLogPath, "mycnf_slow_log_path", flagSlowLogPath, "mysql slow query log path")
		fs.StringVar(&flagRelayLogPath, "mycnf_relay_log_path", flagRelayLogPath, "mysql relay log path")
		fs.StringVar(&flagRelayLogIndexPath, "mycnf_relay_log_index_path", flagRelayLogIndexPath, "mysql relay log index path")
		fs.StringVar(&flagRelayLogInfoPath, "mycnf_relay_log_info_path", flagRelayLogInfoPath, "mysql relay log info path")
		fs.StringVar(&flagBinLogPath, "mycnf_bin_log_path", flagBinLogPath, "mysql binlog path")
		fs.StringVar(&flagMasterInfoFile, "mycnf_master_info_file", flagMasterInfoFile, "mysql master.info file")
		fs.StringVar(&flagPidFile, "mycnf_pid_file", flagPidFile, "mysql pid file")
		fs.StringVar(&flagTmpDir, "mycnf_tmp_dir", flagTmpDir, "mysql tmp directory")
		fs.StringVar(&flagSecureFilePriv, "mycnf_secure_file_priv", flagSecureFilePriv, "mysql path for loading secure files")

		fs.StringVar(&flagMycnfFile, "mycnf-file", flagMycnfFile, "path to my.cnf, if reading all config params from there")
	})
}

// NewMycnfFromFlags creates a Mycnf object from the command line flags.
//
// Multiple modes are supported:
//   - at least mycnf_server_id is set on the command line
//     --> then we read all parameters from the command line, and not from
//     any my.cnf file.
//   - mycnf_server_id is not passed in, but mycnf-file is passed in
//     --> then we read that mycnf file
//   - mycnf_server_id and mycnf-file are not passed in:
//     --> then we use the default location of the my.cnf file for the
//     provided uid and read that my.cnf file.
//
// RegisterCommandLineFlags should have been called before calling
// this, otherwise we'll panic.
func NewMycnfFromFlags(uid uint32) (mycnf *Mycnf, err error) {
	if flagServerID != 0 {
		log.Info("mycnf_server_id is specified, using command line parameters for mysql config")
		return &Mycnf{
			ServerID:              uint32(flagServerID),
			MysqlPort:             flagMysqlPort,
			DataDir:               flagDataDir,
			InnodbDataHomeDir:     flagInnodbDataHomeDir,
			InnodbLogGroupHomeDir: flagInnodbLogGroupHomeDir,
			SocketFile:            flagSocketFile,
			GeneralLogPath:        flagGeneralLogPath,
			ErrorLogPath:          flagErrorLogPath,
			SlowLogPath:           flagSlowLogPath,
			RelayLogPath:          flagRelayLogPath,
			RelayLogIndexPath:     flagRelayLogIndexPath,
			RelayLogInfoPath:      flagRelayLogInfoPath,
			BinLogPath:            flagBinLogPath,
			MasterInfoFile:        flagMasterInfoFile,
			PidFile:               flagPidFile,
			TmpDir:                flagTmpDir,
			SecureFilePriv:        flagSecureFilePriv,

			// This is probably not going to be used by anybody,
			// but fill in a default value. (Note it's used by
			// mysqld.Start, in which case it is correct).
			Path: MycnfFile(uint32(flagServerID)),
		}, nil
	}

	if flagMycnfFile == "" {
		flagMycnfFile = MycnfFile(uid)
		log.Infof("No mycnf_server_id, no mycnf-file specified, using default config for server id %v: %v", uid, flagMycnfFile)
	} else {
		log.Infof("No mycnf_server_id specified, using mycnf-file file %v", flagMycnfFile)
	}
	mycnf = NewMycnf(uid, 0)
	mycnf.Path = flagMycnfFile
	return ReadMycnf(mycnf)
}
