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
	"time"

	"github.com/spf13/pflag"

	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/utils"
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

const (
	waitForMyCnf = 10 * time.Second
)

// RegisterFlags registers the command line flags for
// specifying the values of a mycnf config file. See NewMycnfFromFlags
// to get the supported modes.
func RegisterFlags() {
	servenv.OnParse(func(fs *pflag.FlagSet) {
		utils.SetFlagIntVar(fs, &flagServerID, "mycnf-server-id", flagServerID, "mysql server id of the server (if specified, mycnf-file will be ignored)")
		utils.SetFlagIntVar(fs, &flagMysqlPort, "mycnf-mysql-port", flagMysqlPort, "port mysql is listening on")
		utils.SetFlagStringVar(fs, &flagDataDir, "mycnf-data-dir", flagDataDir, "data directory for mysql")
		utils.SetFlagStringVar(fs, &flagInnodbDataHomeDir, "mycnf-innodb-data-home-dir", flagInnodbDataHomeDir, "Innodb data home directory")
		utils.SetFlagStringVar(fs, &flagInnodbLogGroupHomeDir, "mycnf-innodb-log-group-home-dir", flagInnodbLogGroupHomeDir, "Innodb log group home directory")
		utils.SetFlagStringVar(fs, &flagSocketFile, "mycnf-socket-file", flagSocketFile, "mysql socket file")
		utils.SetFlagStringVar(fs, &flagGeneralLogPath, "mycnf-general-log-path", flagGeneralLogPath, "mysql general log path")
		utils.SetFlagStringVar(fs, &flagErrorLogPath, "mycnf-error-log-path", flagErrorLogPath, "mysql error log path")
		utils.SetFlagStringVar(fs, &flagSlowLogPath, "mycnf-slow-log-path", flagSlowLogPath, "mysql slow query log path")
		utils.SetFlagStringVar(fs, &flagRelayLogPath, "mycnf-relay-log-path", flagRelayLogPath, "mysql relay log path")
		utils.SetFlagStringVar(fs, &flagRelayLogIndexPath, "mycnf-relay-log-index-path", flagRelayLogIndexPath, "mysql relay log index path")
		utils.SetFlagStringVar(fs, &flagRelayLogInfoPath, "mycnf-relay-log-info-path", flagRelayLogInfoPath, "mysql relay log info path")
		utils.SetFlagStringVar(fs, &flagBinLogPath, "mycnf-bin-log-path", flagBinLogPath, "mysql binlog path")
		utils.SetFlagStringVar(fs, &flagMasterInfoFile, "mycnf-master-info-file", flagMasterInfoFile, "mysql master.info file")
		utils.SetFlagStringVar(fs, &flagPidFile, "mycnf-pid-file", flagPidFile, "mysql pid file")
		utils.SetFlagStringVar(fs, &flagTmpDir, "mycnf-tmp-dir", flagTmpDir, "mysql tmp directory")
		utils.SetFlagStringVar(fs, &flagSecureFilePriv, "mycnf-secure-file-priv", flagSecureFilePriv, "mysql path for loading secure files")

		fs.StringVar(&flagMycnfFile, "mycnf-file", flagMycnfFile, "path to my.cnf, if reading all config params from there")
	})
}

// NewMycnfFromFlags creates a Mycnf object from the command line flags.
//
// Multiple modes are supported:
//   - at least mycnf-server-id is set on the command line
//     --> then we read all parameters from the command line, and not from
//     any my.cnf file.
//   - mycnf-server-id is not passed in, but mycnf-file is passed in
//     --> then we read that mycnf file
//   - mycnf-server-id and mycnf-file are not passed in:
//     --> then we use the default location of the my.cnf file for the
//     provided uid and read that my.cnf file.
//
// RegisterCommandLineFlags should have been called before calling
// this, otherwise we'll panic.
func NewMycnfFromFlags(uid uint32) (mycnf *Mycnf, err error) {
	if flagServerID != 0 {
		log.Info("mycnf-server-id is specified, using command line parameters for mysql config")
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
		log.Infof("No mycnf-server-id, no mycnf-file specified, using default config for server id %v: %v", uid, flagMycnfFile)
	} else {
		log.Infof("No mycnf-server-id specified, using mycnf-file file %v", flagMycnfFile)
	}
	mycnf = NewMycnf(uid, 0)
	mycnf.Path = flagMycnfFile
	return ReadMycnf(mycnf, waitForMyCnf)
}
