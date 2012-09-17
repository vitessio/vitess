// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package mysqlctl

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"path"
	"strconv"
	"strings"

	"code.google.com/p/vitess/go/ioutil2"
	"code.google.com/p/vitess/go/relog"
)

// These methods deal with cloning a running instance of mysql.

const (
	maxLagSeconds = 5
)

const (
	replicaSourceFile = "replica_source.json"
)

// Validate that this instance is a reasonable source of data.
// FIXME(msolomon) Provide a hook to call out to a program to decide.
// Is this an option to vtaction? Or look for $VTROOT/bin/vthook_validate_clone_source?
// What environment variables do we have to provide? dbname, host, socket?
func (mysqld *Mysqld) ValidateCloneSource() error {
	slaveStatus, err := mysqld.slaveStatus()
	if err != nil {
		if err != ERR_NOT_SLAVE {
			return fmt.Errorf("mysqlctl: ValidateCloneSource failed, %v", err)
		}
	} else {
		lagSeconds, _ := strconv.Atoi(slaveStatus["seconds_behind_master"])
		if lagSeconds > maxLagSeconds {
			return fmt.Errorf("mysqlctl: ValidateCloneSource failed, lag_seconds exceed maximum tolerance (%v)", lagSeconds)
		}
	}
	// FIXME(msolomon) check free space based on an estimate of the current
	// size of the db files.
	// Also, check that we aren't already cloning/compressing or acting as a
	// source. Mysqld being down isn't enough, presumably that will be
	// restarted as soon as the snapshot is taken.
	return nil
}

func (mysqld *Mysqld) ValidateCloneTarget() error {
	rows, err := mysqld.fetchSuperQuery("SHOW DATABASES")
	if err != nil {
		return fmt.Errorf("mysqlctl: ValidateCloneTarget failed, %v", err)
	}

	for _, row := range rows {
		if strings.HasPrefix(row[0].(string), "vt_") {
			dbName := row[0].(string)
			tableRows, err := mysqld.fetchSuperQuery("SHOW TABLES FROM " + dbName)
			if err != nil {
				return fmt.Errorf("mysqlctl: ValidateCloneTarget failed, %v", err)
			} else if len(tableRows) == 0 {
				// no tables == empty db, all is well
				continue
			}
			return fmt.Errorf("mysqlctl: ValidateCloneTarget failed, found active db %v", dbName)
		}
	}

	return nil
}

func (mysqld *Mysqld) ValidateSplitReplicaTarget() error {
	return errors.New("unimplemented")
	rows, err := mysqld.fetchSuperQuery("SHOW PROCESSLIST")
	if err != nil {
		return err
	}
	if len(rows) > 4 {
		return errors.New("too many active db processes")
	}

	rows, err = mysqld.fetchSuperQuery("SHOW DATABASES")
	if err != nil {
		return err
	}

	// NOTE: we expect that database was already created during tablet
	// assignment.
	return nil
}

func compressFiles(srcDir, dstDir string) ([]DataFile, error) {
	dataFiles := make([]DataFile, 0, 128)
	fiList, err := ioutil.ReadDir(srcDir)
	if err != nil {
		return nil, err
	}
	for _, fi := range fiList {
		if !fi.IsDir() {
			srcPath := path.Join(srcDir, fi.Name())
			dstPath := path.Join(dstDir, fi.Name()+".gz")
			if err := compressFile(srcPath, dstPath); err != nil {
				return nil, err
			}
			hash, err := md5File(dstPath)
			if err != nil {
				return nil, err
			}
			dataFiles = append(dataFiles, DataFile{dstPath, hash})
			relog.Info("clone data ready %v:%v", dstPath, hash)
		}
	}
	return dataFiles, nil
}

func (mysqld *Mysqld) FindVtDatabases() ([]string, error) {
	fiList, err := ioutil.ReadDir(mysqld.config.DataDir)
	if err != nil {
		return nil, err
	}

	dbNames := make([]string, 0, 16)
	for _, fi := range fiList {
		if strings.HasSuffix(fi.Name(), "vt_") {
			dbNames = append(dbNames, fi.Name())
		}
	}
	return dbNames, nil
}

func (mysqld *Mysqld) createSnapshot(dbName, snapshotPath string) ([]DataFile, error) {
	// wrapErr := func(err error) error {
	// 	return fmt.Errorf("mysqlctl: createSnapshot failed: %v", err)
	// }

	// FIXME(msolomon) Must match patterns in mycnf - probably belongs
	// in there as derived paths.
	snapshotDataSrcPath := path.Join(snapshotPath, dataDir, dbName)
	snapshotInnodbDataSrcPath := path.Join(snapshotPath, innodbDataSubdir)
	snapshotInnodbLogSrcPath := path.Join(snapshotPath, innodbLogSubdir)
	// clean out and start fresh
	for _, _path := range []string{snapshotDataSrcPath, snapshotInnodbDataSrcPath, snapshotInnodbLogSrcPath} {
		if err := os.RemoveAll(_path); err != nil {
			return nil, err
		}
		if err := os.MkdirAll(_path, 0775); err != nil {
			return nil, err
		}
	}

	allDataFiles := make([]DataFile, 0, 128)

	dbDataDir := path.Join(mysqld.config.DataDir, dbName)
	dataFiles, err := compressFiles(dbDataDir, snapshotDataSrcPath)
	if err != nil {
		return nil, err
	}
	allDataFiles = append(allDataFiles, dataFiles...)

	dataFiles, err = compressFiles(mysqld.config.InnodbDataHomeDir, snapshotInnodbDataSrcPath)
	if err != nil {
		return nil, err
	}
	allDataFiles = append(allDataFiles, dataFiles...)

	dataFiles, err = compressFiles(mysqld.config.InnodbLogGroupHomeDir, snapshotInnodbLogSrcPath)
	if err != nil {
		return nil, err
	}
	allDataFiles = append(allDataFiles, dataFiles...)

	return allDataFiles, nil
}

// This function runs on the machine acting as the source for the clone.
//
// Check master/slave status and determine restore needs.
// If this instance is a slave, stop replication, otherwise place in read-only mode.
// Record replication position.
// Shutdown mysql
// Check paths for storing data
// Compress /vt/vt_[0-9a-f]+/data/vt_.+
// Compute md5() sums
// Place in /vt/clone_src where they will be served by http server (not rpc)
// Restart mysql
func (mysqld *Mysqld) CreateSnapshot(dbName, sourceAddr string, allowHierarchicalReplication bool) (replicaSource *ReplicaSource, err error) {
	if dbName == "" {
		return nil, errors.New("CreateSnapshot failed: no database name provided")
	}

	if err = mysqld.ValidateCloneSource(); err != nil {
		return nil, err
	}

	// save initial state so we can restore on Start()
	slaveStartRequired := false
	sourceIsMaster := false
	readOnly := true

	slaveStatus, slaveErr := mysqld.slaveStatus()
	if slaveErr == nil {
		slaveStartRequired = (slaveStatus["Slave_IO_Running"] == "Yes" && slaveStatus["Slave_SQL_Running"] == "Yes")
	} else if slaveErr == ERR_NOT_SLAVE {
		sourceIsMaster = true
	} else {
		// If we can't get any data, just fail.
		return nil, err
	}

	readOnly, err = mysqld.IsReadOnly()
	if err != nil {
		return nil, err
	}

	// Stop sources of writes so we can get a consistent replication position.
	// If the source is a slave use the master replication position
	// unless we are allowing hierachical replicas.
	masterAddr := ""
	var replicationPosition *ReplicationPosition
	if sourceIsMaster {
		if err = mysqld.SetReadOnly(true); err != nil {
			return nil, err
		}
		replicationPosition, err = mysqld.MasterStatus()
		if err != nil {
			return nil, err
		}
		masterAddr = mysqld.Addr()
	} else {
		if err = mysqld.StopSlave(); err != nil {
			return nil, err
		}
		replicationPosition, err = mysqld.SlaveStatus()
		if err != nil {
			return nil, err
		}
		// We are a slave, check our replication strategy before choosing
		// the master address.
		if allowHierarchicalReplication {
			masterAddr = mysqld.Addr()
		} else {
			masterAddr, err = mysqld.GetMasterAddr()
			if err != nil {
				return nil, err
			}
		}
	}

	if err = Shutdown(mysqld, true); err != nil {
		return nil, err
	}

	var rs *ReplicaSource
	dataFiles, snapshotErr := mysqld.createSnapshot(dbName, mysqld.config.SnapshotDir)
	if snapshotErr != nil {
		relog.Error("CreateSnapshot failed: %v", snapshotErr)
	} else {
		rs = NewReplicaSource(sourceAddr, masterAddr, mysqld.replParams.Uname, mysqld.replParams.Pass,
			dbName, dataFiles, replicationPosition)
		rsFile := path.Join(mysqld.config.SnapshotDir, replicaSourceFile)
		if snapshotErr = writeJson(rsFile, rs); snapshotErr != nil {
			relog.Error("CreateSnapshot failed: %v", snapshotErr)
		}
	}

	// Try to restart mysqld regardless of snapshot success.
	if err = Start(mysqld); err != nil {
		return nil, err
	}

	// Restore original mysqld state that we saved above.
	if slaveStartRequired {
		if err = mysqld.StartSlave(); err != nil {
			return nil, err
		}
		// this should be quick, but we might as well just wait
		if err = mysqld.WaitForSlaveStart(SlaveStartDeadline); err != nil {
			return nil, err
		}
	}

	if err = mysqld.SetReadOnly(readOnly); err != nil {
		return nil, err
	}

	if snapshotErr != nil {
		return nil, snapshotErr
	}

	return rs, nil
}

func writeJson(filename string, x interface{}) error {
	data, err := json.MarshalIndent(x, "  ", "  ")
	if err != nil {
		return err
	}
	return ioutil2.WriteFileAtomic(filename, data, 0660)
}

func ReadReplicaSource(filename string) (*ReplicaSource, error) {
	data, err := ioutil.ReadFile(filename)
	if err != nil {
		return nil, err
	}
	rs := new(ReplicaSource)
	if err = json.Unmarshal(data, rs); err != nil {
		return nil, fmt.Errorf("ReadReplicaSource failed: %v %v", filename, err)
	}
	return rs, nil
}

// This piece runs on the presumably empty machine acting as the target in the
// create replica action.
//
// validate target (self)
// shutdown_mysql()
// create temp data directory /vt/target/vt_<keyspace>
// copy compressed data files via HTTP
// verify md5sum of compressed files
// uncompress into /vt/vt_<target-uid>/data/vt_<keyspace>
// start_mysql()
// clean up compressed files
func (mysqld *Mysqld) RestoreFromSnapshot(replicaSource *ReplicaSource) error {
	if replicaSource == nil {
		return errors.New("RestoreFromSnapshot: nil replicaSource")
	}

	relog.Debug("ValidateCloneTarget")
	if err := mysqld.ValidateCloneTarget(); err != nil {
		return err
	}

	relog.Debug("Shutdown mysqld")
	if err := Shutdown(mysqld, true); err != nil {
		return err
	}

	relog.Debug("Fetch snapshot")
	if err := mysqld.fetchSnapshot(replicaSource); err != nil {
		return err
	}

	relog.Debug("Restart mysqld")
	if err := Start(mysqld); err != nil {
		return err
	}

	cmdList := StartReplicationCommands(replicaSource.ReplicationState)
	relog.Info("StartReplicationCommands %#v", cmdList)
	if err := mysqld.executeSuperQueryList(cmdList); err != nil {
		return err
	}

	return mysqld.WaitForSlaveStart(SlaveStartDeadline)
}

func (mysqld *Mysqld) fetchSnapshot(replicaSource *ReplicaSource) error {
	replicaDbPath := path.Join(mysqld.config.DataDir, replicaSource.DbName)

	cleanDirs := []string{mysqld.config.SnapshotDir, replicaDbPath,
		mysqld.config.InnodbDataHomeDir, mysqld.config.InnodbLogGroupHomeDir}

	// clean out and start fresh
	// FIXME(msolomon) this might be changed to allow partial recovery, but at that point
	// we are starting to reimplement rsync.
	for _, dir := range cleanDirs {
		if err := os.RemoveAll(dir); err != nil {
			return err
		}
		if err := os.MkdirAll(dir, 0775); err != nil {
			return err
		}
	}

	// FIXME(msolomon) parallelize
	// FIXME(msolomon) automatically retry a file transfer at least once
	// FIXME(msolomon) deadlines?
	for _, fi := range replicaSource.Files {
		relativePath := strings.SplitN(fi.Path, "/", 5)[4]
		gzFilename := path.Join(mysqld.config.SnapshotDir, relativePath)
		filename := path.Join(mysqld.config.TabletDir, relativePath)
		// trim .gz
		filename = filename[:len(filename)-3]

		// Ensure directory for final destination.
		dir, _ := path.Split(gzFilename)
		if err := os.MkdirAll(dir, 0775); err != nil {
			return err
		}

		furl := "http://" + replicaSource.Addr + fi.Path
		if err := fetchSnapshotUrl(furl, fi.Hash, gzFilename, filename); err != nil {
			return err
		}

		relog.Info("fetched snapshot file: %v", filename)
	}
	return nil
}

func fetchSnapshotUrl(srcUrl, srcHash, tmpFilename, dstFilename string) error {
	resp, err := http.Get(srcUrl)
	if resp.StatusCode != 200 {
		return errors.New("failed fetching " + srcUrl + ": " + resp.Status)
	}
	defer resp.Body.Close()

	// FIXME(msolomon) buffer output?
	file, err := os.OpenFile(tmpFilename, os.O_CREATE|os.O_WRONLY, 0660)
	if err != nil {
		return err
	}
	defer file.Close()

	if _, err = io.Copy(file, resp.Body); err != nil {
		return err
	}

	file.Close()
	hash, err := md5File(tmpFilename)
	if err != nil {
		return err
	}

	if srcHash != hash {
		return errors.New("hash mismatch for " + tmpFilename + ", " + srcHash + " != " + hash)
	}

	if err := uncompressFile(tmpFilename, dstFilename); err != nil {
		return err
	}

	if err := os.Remove(tmpFilename); err != nil {
		// don't stop the process for this error
		relog.Warning("failed to remove temp file: %v", err)
	}

	return nil
}
