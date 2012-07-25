// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package mysqlctl

// FIXME(msolomon) this actions were copy/pasted from replication.go because
// they were conceptually quite similar. They should be reconciled at some
// point.

/*
Given a single shard, split into 2 subshards, each addressing some subset of the total key ranges.


T is the tablet server controlling M
R is the entity_id key range that T handles
M is the master mysql db
 S is the stemcell mysql slave, which takes no normal traffic (does this have a tablet server?)

M', M" are new master db's, each of which will have some subset of the key range of M
S', S" are new stemcell db's, each of which will have some number of slaves
T', T" are the corresponding tablet servers for M'/M"

 Assume masters take a significant amount of read traffic (unlike EMD).

Resharding may be implemented as a subprocess from the tablet server that communicates back over a netchan. This will make it easier to patch without taking down the tablet server.
 Acquire machine resources (M'/M", S'/S", ...)
 2*M + 2*S + min((N+X), 2*min # of replicas) + (2 * Lag)
N is replica count local to M
X is replicas outside of M's datacenter
 Laggards are optional (but probably good)
The global minimum for replicas per shard is ~3 for durability and the ability to clone while you are online serving queries.
Install/init tablet server processes T'/T"
Install/init mysql on M'/M"
 SET GLOBAL read_only = 1;
does this allow replication to proceed?
what about commands issued by SUPER?
Arrange replication layout amongst new instances
If there are latency/geographic considerations, this is where they manifest themselves. In general, the stemcells will be the source of the replication streams. Each geographic area should have a stemcell which acts as the parent for all other slaves in that area. The local stemcell should slave from the master's stemcell. It should look like a shrub more than a tree.
Alternatively, this layout can be used for an initial copy of the table dumps. After the initial data load, the replication streams can be set up. This might be faster, but is likely to be more complex to manage.
Apply baseline schema
turn off indexes to increase throughput? can't do this on InnoDB
Stop replication on stemcell S
Record replication position on S for M' and M"
Given two key ranges, R' and R" set the replication key range on M' and M"
this requires modifications to mysql replication which I have made in the past to be redone
This should be fixable to row-based replication as well.
 For each table on S, export subranges to M' and M":
 SELECT * FROM table WHERE R'.start <= id AND id < R'.end
 SELECT * FROM table WHERE R".start <= id AND id < R".end
Feed dump query streams in M' and M" respectively
use some sort of SELECT INTO..., LOAD FROM... to optimize?
use some sort of COMMIT buffering to optimize?
disable AUTOCOMMIT
 SET UNIQUE_CHECKS=0; do some stuff; SET UNIQUE_CHECKS=1;
use the tablet server to compress or do data-only instead of sending full SQL commands
will single replication threads handle the inserts fast enough downstream of S' and S"?
Once the bulk export is complete, restart replication on S.
 Once the bulk import is complete, rebuild tables? (might not be necessary since data is sequential)
Reparent M' and M" to S
set the key range that replication will accept
Start splitting replication on M' and M"
 Wait for M'/M" to catch up to S (implying caught up to M)
 Wait for S'x and S"x hosts (mysql instances slaved from the new stemcells) to catch up to M'/M".
 S'Lag and S"Lag (24 hour lag hosts) will not be 24 hrs behind for 23+ hrs
Writes can now be shunted from M to M'/M"
writes are likely to be warm from replication
reads will be cold since there is no traffic going to the T'/T" - the row cache is empty
row cache could be warmed, but the invalidation is tricky if you are allowing writes
8GB of cache will take 120 seconds to transfer, even if you can nearly max out the 1Gb port to an adjacent machine
if shards are small, this might not be a big deal
Start failing writes on T, report that T split to smart clients.
 SET GLOBAL read_only = 1 on M to prevent ghost writes.
 Set T to refuse new connections (read or write)
Disconnect replication on M'/M" from S.
 SET GLOBAL read_only = 0 on M'/M" to allow new writes.
Update table wrangler and reassign R'/R" to T'/T".
T disconnects reading clients and shutsdown mysql.
How aggressively can we do this? The faster the better.
Garbage collect the hosts.
leave the 24 lag for 1 day
*/

import (
	"encoding/base64"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"path"
	"strconv"
	"strings"

	"code.google.com/p/vitess/go/relog"
)

type SplitReplicaSource struct {
	*ReplicaSource
	StartKey string
	EndKey   string
	Schema   string
}

func NewSplitReplicaSource(addr, mysqlAddr string) *SplitReplicaSource {
	return &SplitReplicaSource{ReplicaSource: NewReplicaSource(addr, mysqlAddr)}
}

// FIXME(msolomon) use query format/bind vars
var selectIntoOutfile = `SELECT * INTO OUTFILE :tableoutputpath
  CHARACTER SET binary
  FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' ESCAPED BY '\\'
  LINES TERMINATED BY '\n'
  FROM {{.TableName}} WHERE {{.KeyspaceIdColumnName}} >= :startkey AND 
	{{.KeyspaceIdColumnName}} < :endkey`

var loadDataInfile = `LOAD DATA INFILE '{{.TableInputPath}}' INTO TABLE {{.TableName}}
  CHARACTER SET binary
  FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' ESCAPED BY '\\'
  LINES TERMINATED BY '\n'`

func b64ForFilename(s string) string {
	return strings.Replace(base64.URLEncoding.EncodeToString([]byte(s)), "=", "", -1)
}

/*
copied from replication.
create a series of raw dump files the contain rows to be reinserted

dbName - mysql db name
keyName - name of the mysql column that is the leading edge of all primary keys
startKey, endKey - the row range to prepare
replicaSourcePath - where to copy the output data
sourceAddr - the ip addr of the machine running the export
*/
func (mysqld *Mysqld) CreateSplitReplicaSource(dbName, keyName, startKey, endKey, replicaSourcePath, sourceAddr string) (_replicaSource *SplitReplicaSource, err error) {
	if dbName == "" {
		err = errors.New("no database name provided")
		return
	}
	// same logic applies here
	relog.Info("ValidateReplicaSource")
	if err = mysqld.ValidateReplicaSource(); err != nil {
		return
	}

	// FIXME(msolomon) bleh, must match patterns in mycnf - probably belongs
	// in there as derived paths.
	cloneSourcePath := path.Join(replicaSourcePath, "data", dbName+"-"+b64ForFilename(startKey)+","+b64ForFilename(endKey))
	// clean out and start fresh	
	for _, _path := range []string{cloneSourcePath} {
		if err = os.RemoveAll(_path); err != nil {
			return
		}
		if err = os.MkdirAll(_path, 0775); err != nil {
			return
		}
	}

	// get a list of tables to process
	rows, fetchErr := mysqld.fetchSuperQuery("SHOW TABLES")
	if fetchErr != nil {
		return nil, fetchErr
	}
	if len(rows) == 0 {
		return nil, errors.New("empty table list")
	}
	tableNames := make([]string, len(rows))
	for i, row := range rows {
		tableNames[i] = row[0].(string)
	}
	relog.Info("Fetch Tables: %#v %#v", rows, tableNames)

	/*
	   mysql> show master status\G
	   **************************** 1. row ***************************
	   File: vt-000001c6-bin.000003
	   Position: 106
	   Binlog_Do_DB: 
	   Binlog_Ignore_DB: 
	*/
	// FIXME(msolomon) handle both master and slave situtation
	rows, fetchErr = mysqld.fetchSuperQuery("SHOW MASTER STATUS")
	if fetchErr != nil {
		return nil, fetchErr
	}
	if len(rows) != 1 {
		return nil, errors.New("unexpected result for show master status")
	}
	relog.Info("Save Master Status")

	replicaSource := NewSplitReplicaSource(sourceAddr, mysqld.Addr())
	replicaSource.DbName = dbName
	replicaSource.ReplicationPosition.MasterLogFile = rows[0][0].(string)
	temp, _ := strconv.ParseUint(rows[0][1].(string), 10, 0)
	replicaSource.ReplicationPosition.MasterLogPosition = uint(temp)

	// save initial state so we can restore on Start()
	slaveStartRequired := false
	if slaveStatus, slaveErr := mysqld.slaveStatus(); slaveErr == nil {
		slaveStartRequired = (slaveStatus["Slave_IO_Running"] == "Yes" && slaveStatus["Slave_SQL_Running"] == "Yes")
	}
	readOnly := true
	if readOnly, err = mysqld.IsReadOnly(); err != nil {
		return
	}

	relog.Info("Set Read Only")
	if !readOnly {
		mysqld.SetReadOnly(true)
	}
	relog.Info("Stop Slave")
	if err = mysqld.StopSlave(); err != nil {
		return
	}
	relog.Info("Flush tables")
	if err = mysqld.executeSuperQuery("FLUSH TABLES WITH READ LOCK"); err != nil {
		return
	}

	// export each table to a CSV-like file, compress the results
	tableFiles := make([]string, len(tableNames))
	// FIXME(msolomon) parallelize
	for i, tableName := range tableNames {
		relog.Info("Dump table %v...", tableName)
		filename := path.Join(cloneSourcePath, tableName+".csv")
		tableFiles[i] = filename

		queryParams := map[string]string{
			"TableName":            tableName,
			"KeyspaceIdColumnName": keyName,
		}
		// FIXME(sougou/msolomon): no bindparams for the new mysql module
		/*bindParams := map[string]interface{}{
		  "tableoutputpath": filename,
		  "startkey":        startKey,
		  "endkey":          endKey,
		}*/

		query := mustFillStringTemplate(selectIntoOutfile, queryParams)
		relog.Info("  %v", query)
		if err = mysqld.executeSuperQuery(query); err != nil {
			// FIXME(msolomon) on abort, should everything go back the way it was?
			// alternatively, we could just leave it and wait for the wrangler to
			// notice and start cleaning up
			return
		}
	}

	// FIXME(msolomon) should mysqld just restart on any failure?
	compressFiles := func(filenames []string) error {
		for _, srcPath := range filenames {
			dstPath := srcPath + ".gz"
			if err := compressFile(srcPath, dstPath); err != nil {
				return err
			}
			// prune files to free up disk space, if it errors, we'll figure out
			// later
			os.Remove(srcPath)

			hash, hashErr := md5File(dstPath)
			if hashErr != nil {
				return hashErr
			}

			replicaSource.FileList = append(replicaSource.FileList, dstPath)
			replicaSource.HashList = append(replicaSource.HashList, hash)
			relog.Info("%v:%v ready", dstPath, hash)
		}
		return nil
	}

	// FIXME(msolomon) at some point, you could pipeline requests for speed
	if err = compressFiles(tableFiles); err != nil {
		return
	}

	if err = mysqld.executeSuperQuery("UNLOCK TABLES"); err != nil {
		return
	}

	// restore original mysqld state that we saved above
	if slaveStartRequired {
		if err = mysqld.StartSlave(); err != nil {
			return
		}
		// this should be quick, but we might as well just wait
		if err = mysqld.WaitForSlaveStart(5); err != nil {
			return
		}
	}
	if err = mysqld.SetReadOnly(readOnly); err != nil {
		return
	}

	// ok, copy over the pointer on success
	_replicaSource = replicaSource
	relog.Info("mysqld replicaSource %#v", replicaSource)
	return
}

/*
 This piece runs on the presumably empty machine acting as the target in the
 create replica action.

 validate target (self)
 shutdown_mysql()
 create temp data directory /vt/target/vt_<keyspace>
 copy compressed data files via HTTP
 verify md5sum of compressed files
 uncompress into /vt/vt_<target-uid>/data/vt_<keyspace>
 start_mysql()
 clean up compressed files
*/
func (mysqld *Mysqld) CreateSplitReplicaTarget(replicaSource *SplitReplicaSource, tempStoragePath string) (err error) {
	if err = mysqld.ValidateSplitReplicaTarget(); err != nil {
		return
	}

	cleanDirs := []string{tempStoragePath}

	// clean out and start fresh
	// FIXME(msolomon) this might be changed to allow partial recovery
	for _, dir := range cleanDirs {
		if err = os.RemoveAll(dir); err != nil {
			return
		}
		if err = os.MkdirAll(dir, 0775); err != nil {
			return
		}
	}

	if err = mysqld.SetReadOnly(true); err != nil {
		return
	}
	// we could conditionally create the database, but this helps us
	// verify that other parts of the process are working
	createDbCmds := []string{
		// "CREATE DATABASE " + replicaSource.DbName + " IF NOT EXISTS",
		"USE " + replicaSource.DbName}
	for _, cmd := range strings.Split(replicaSource.Schema, ";") {
		cmd = strings.TrimSpace(cmd)
		if cmd == "" {
			continue
		}
		createDbCmds = append(createDbCmds, cmd)
	}
	// FIXME(msolomon) make sure this works with multiple tables
	if err = mysqld.executeSuperQueryList(createDbCmds); err != nil {
		return
	}

	httpConn, connErr := net.Dial("tcp", replicaSource.Addr)
	if connErr != nil {
		return connErr
	}
	defer httpConn.Close()

	fileClient := httputil.NewClientConn(httpConn, nil)
	defer fileClient.Close()
	// FIXME(msolomon) parallelize
	// FIXME(msolomon) pull out simple URL fetch?
	// FIXME(msolomon) automatically retry a file transfer at least once
	// FIXME(msolomon) deadlines?
	// FIXME(msolomon) work into replication.go
	for i, srcPath := range replicaSource.FileList {
		srcHash := replicaSource.HashList[i]
		urlstr := "http://" + replicaSource.Addr + srcPath
		urlobj, parseErr := url.Parse(urlstr)
		if parseErr != nil {
			return errors.New("failed to create url " + urlstr)
		}
		req := &http.Request{Method: "GET",
			Host: replicaSource.Addr,
			URL:  urlobj}
		err = fileClient.Write(req)
		if err != nil {
			return errors.New("failed requesting " + urlstr)
		}
		var response *http.Response
		response, err = fileClient.Read(req)
		if err != nil {
			return errors.New("failed fetching " + urlstr)
		}
		if response.StatusCode != 200 {
			return errors.New("failed fetching " + urlstr + ": " + response.Status)
		}
		relativePath := strings.SplitN(srcPath, "/", 5)[4]
		gzFilename := path.Join(tempStoragePath, relativePath)
		// trim .gz
		filename := gzFilename[:len(gzFilename)-3]

		dir, _ := path.Split(gzFilename)
		if dirErr := os.MkdirAll(dir, 0775); dirErr != nil {
			return dirErr
		}

		// FIXME(msolomon) buffer output?
		file, fileErr := os.OpenFile(gzFilename, os.O_CREATE|os.O_WRONLY, 0664)
		if fileErr != nil {
			return fileErr
		}
		defer file.Close()

		_, err = io.Copy(file, response.Body)
		if err != nil {
			return
		}
		file.Close()

		hash, hashErr := md5File(gzFilename)
		if hashErr != nil {
			return hashErr
		}
		if srcHash != hash {
			return errors.New("hash mismatch for " + gzFilename + ", " + srcHash + " != " + hash)
		}

		if err = uncompressFile(gzFilename, filename); err != nil {
			return
		}
		if err = os.Remove(gzFilename); err != nil {
			// don't stop the process for this error
			relog.Info("failed to remove %v", gzFilename)
		}
		tableName := strings.Replace(path.Base(filename), ".csv", "", -1)
		queryParams := map[string]string{
			"TableInputPath": filename,
			"TableName":      replicaSource.DbName + "." + tableName,
		}
		query := mustFillStringTemplate(loadDataInfile, queryParams)
		if err = mysqld.executeSuperQuery(query); err != nil {
			// FIXME(msolomon) on abort, we should just tear down
			// alternatively, we could just leave it and wait for the wrangler to
			// notice and start cleaning up
			return
		}

		relog.Info("%v ready", filename)
	}

	// FIXME(msolomon) start *split* replication, you need the new start/end
	// keys
	cmdList := StartSplitReplicationCommands(replicaSource.ReplicationState, replicaSource.StartKey, replicaSource.EndKey)
	relog.Info("StartSplitReplicationCommands %#v", cmdList)
	if err = mysqld.executeSuperQueryList(cmdList); err != nil {
		return
	}

	err = mysqld.WaitForSlaveStart(SlaveStartDeadline)
	if err != nil {
		return
	}
	// ok, now that replication is under way, wait for us to be caught up
	if err = mysqld.WaitForSlave(5); err != nil {
		return
	}
	// don't set readonly until the rest of the system is ready
	return
}

func StartSplitReplicationCommands(replState *ReplicationState, startKey string, endKey string) []string {
	return []string{
		"SET GLOBAL vt_enable_binlog_splitter_rbr = 1",
		"SET GLOBAL vt_shard_key_range_start = \"" + startKey + "\"",
		"SET GLOBAL vt_shard_key_range_end = \"" + endKey + "\"",
		"RESET SLAVE",
		mustFillStringTemplate(changeMasterCmd, replState),
		"START SLAVE"}
}
