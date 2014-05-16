// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package mysqlctl

import (
	"flag"
	"fmt"

	log "github.com/golang/glog"
)

// This file handles using command line flags to create a Mycnf object.
// Since whoever links with this module doesn't necessarely need the flags,
// RegisterFlags needs to be called explicitely to set the flags up.

var (
	// the individual command line parameters
	flagServerId              *int
	flagMysqlPort             *int
	flagDataDir               *string
	flagInnodbDataHomeDir     *string
	flagInnodbLogGroupHomeDir *string
	flagSocketFile            *string
	flagErrorLogPath          *string
	flagSlowLogPath           *string
	flagRelayLogPath          *string
	flagRelayLogIndexPath     *string
	flagRelayLogInfoPath      *string
	flagBinLogPath            *string
	flagMasterInfoFile        *string
	flagPidFile               *string
	flagTmpDir                *string
	flagSlaveLoadTmpDir       *string

	// the file to use to specify them all
	flagMycnfFile *string
)

// RegisterFlags registers the command line flags for
// specifying the values of a mycnf config file. See NewMycnfFromFlags
// to get the supported modes.
func RegisterFlags() {
	flagServerId = flag.Int("mycnf_server_id", 0, "mysql server id of the server (if specified, mycnf-file will be ignored)")
	flagMysqlPort = flag.Int("mycnf_mysql_port", 0, "port mysql is listening on")
	flagDataDir = flag.String("mycnf_data_dir", "", "data directory for mysql")
	flagInnodbDataHomeDir = flag.String("mycnf_innodb_data_home_dir", "", "Innodb data home directory")
	flagInnodbLogGroupHomeDir = flag.String("mycnf_innodb_log_group_home_dir", "", "Innodb log group home directory")
	flagSocketFile = flag.String("mycnf_socket_file", "", "mysql socket file")
	flagErrorLogPath = flag.String("mycnf_error_log_path", "", "mysql error log path")
	flagSlowLogPath = flag.String("mycnf_slow_log_path", "", "mysql slow query log path")
	flagRelayLogPath = flag.String("mycnf_relay_log_path", "", "mysql relay log path")
	flagRelayLogIndexPath = flag.String("mycnf_relay_log_index_path", "", "mysql relay log index path")
	flagRelayLogInfoPath = flag.String("mycnf_relay_log_info_path", "", "mysql relay log info path")
	flagBinLogPath = flag.String("mycnf_bin_log_path", "", "mysql binlog path")
	flagMasterInfoFile = flag.String("mycnf_master_info_file", "", "mysql master.info file")
	flagPidFile = flag.String("mycnf_pid_file", "", "mysql pid file")
	flagTmpDir = flag.String("mycnf_tmp_dir", "", "mysql tmp directory")
	flagSlaveLoadTmpDir = flag.String("mycnf_slave_load_tmp_dir", "", "slave load tmp directory")

	flagMycnfFile = flag.String("mycnf-file", "", "path to my.cnf, if reading all config params from there")
}

// NewMycnfFromFlags creates a Mycnf object from the command line flags.
//
// Multiple modes are supported:
// - at least mycnf_server_id is set on the command line
//   --> then we read all parameters from the command line, and not from
//       any my.cnf file.
// - mycnf_server_id is not passed in, but mycnf-file is passed in
//   --> then we read that mycnf file
// - mycnf_server_id and mycnf-file are not passed in:
//   --> then we use the default location of the my.cnf file for the
//       provided uid and read that my.cnf file.
//
// RegisterCommandLineFlags should have been called before calling
// this, otherwise we'll panic.
func NewMycnfFromFlags(uid uint32) (mycnf *Mycnf, err error) {
	if *flagServerId != 0 {
		log.Info("mycnf_server_id is specified, using command line parameters for mysql config")
		panic("NYI")
		return &Mycnf{
			ServerId:              uint32(*flagServerId),
			MysqlPort:             *flagMysqlPort,
			DataDir:               *flagDataDir,
			InnodbDataHomeDir:     *flagInnodbDataHomeDir,
			InnodbLogGroupHomeDir: *flagInnodbLogGroupHomeDir,
			SocketFile:            *flagSocketFile,
			ErrorLogPath:          *flagErrorLogPath,
			SlowLogPath:           *flagSlowLogPath,
			RelayLogPath:          *flagRelayLogPath,
			RelayLogIndexPath:     *flagRelayLogIndexPath,
			RelayLogInfoPath:      *flagRelayLogInfoPath,
			BinLogPath:            *flagBinLogPath,
			MasterInfoFile:        *flagMasterInfoFile,
			PidFile:               *flagPidFile,
			TmpDir:                *flagTmpDir,
			SlaveLoadTmpDir:       *flagSlaveLoadTmpDir,

			// This is probably not going to be used by anybody,
			// but fill in a default value. (Note it's used by
			// mysqld.Start, in which case it is correct).
			path: mycnfFile(uint32(*flagServerId)),
		}, nil
	} else {
		if *flagMycnfFile == "" {
			if uid == 0 {
				log.Fatalf("No mycnf_server_id, no mycnf-file, and no backup server id to use")
			}
			*flagMycnfFile = mycnfFile(uid)
			log.Infof("No mycnf_server_id, no mycnf-file specified, using default config for server id %v: %v", uid, *flagMycnfFile)
		} else {
			log.Infof("No mycnf_server_id specified, using mycnf-file file %v", *flagMycnfFile)
		}
		return ReadMycnf(*flagMycnfFile)
	}
}

// GetSubprocessFlags returns the flags to pass to a subprocess to
// have the exact same mycnf config as us.
//
// RegisterCommandLineFlags and NewMycnfFromFlags should have been
// called before this.
func GetSubprocessFlags() []string {
	if *flagServerId != 0 {
		// all from command line
		return []string{
			"-mycnf_server_id", fmt.Sprintf("%v", *flagServerId),
			"-mycnf_mysql_port", fmt.Sprintf("%v", *flagMysqlPort),
			"-mycnf_data_dir", *flagDataDir,
			"-mycnf_innodb_data_home_dir", *flagInnodbDataHomeDir,
			"-mycnf_innodb_log_group_home_dir", *flagInnodbLogGroupHomeDir,
			"-mycnf_socket_file", *flagSocketFile,
			"-mycnf_error_log_path", *flagErrorLogPath,
			"-mycnf_slow_log_path", *flagSlowLogPath,
			"-mycnf_relay_log_path", *flagRelayLogPath,
			"-mycnf_relay_log_index_path", *flagRelayLogIndexPath,
			"-mycnf_relay_log_info_path", *flagRelayLogInfoPath,
			"-mycnf_bin_log_path", *flagBinLogPath,
			"-mycnf_master_info_file", *flagMasterInfoFile,
			"-mycnf_pid_file", *flagPidFile,
			"-mycnf_tmp_dir", *flagTmpDir,
			"-mycnf_slave_load_tmp_dir", *flagSlaveLoadTmpDir,
		}
	}

	// Just pass through the mycnf-file param, it has been altered
	// if we didn't get it but guessed it from uid.
	return []string{"-mycnf-file", *flagMycnfFile}
}
