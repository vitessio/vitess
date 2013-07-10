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
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"code.google.com/p/vitess/go/bufio2"
	"code.google.com/p/vitess/go/cgzip"
	"code.google.com/p/vitess/go/relog"
	"code.google.com/p/vitess/go/sync2"
	"code.google.com/p/vitess/go/vt/concurrency"
	"code.google.com/p/vitess/go/vt/key"
	"code.google.com/p/vitess/go/vt/mysqlctl/csvsplitter"
)

const (
	partialSnapshotManifestFile = "partial_snapshot_manifest.json"
	SnapshotURLPath             = "/snapshot"

	INSERT_INTO_RECOVERY = `insert into _vt.blp_checkpoint (uid, host, port, master_filename, master_position, relay_filename, relay_position, group_id, keyrange_start, keyrange_end, txn_timestamp, time_updated) 
	                          values (%v, '%v', %v, '%v', %v, '%v', %v, %v, '%v', '%v', unix_timestamp(), %v)`
)

// replaceError replaces original with recent if recent is not nil,
// logging original if it wasn't nil. This should be used in deferred
// cleanup functions if they change the returned error.
func replaceError(original, recent error) error {
	if recent == nil {
		return original
	}
	if original != nil {
		relog.Error("One of multiple error: %v", original)
	}
	return recent
}

type SplitSnapshotManifest struct {
	Source           *SnapshotManifest
	KeyRange         key.KeyRange
	SchemaDefinition *SchemaDefinition
}

// NewSplitSnapshotManifest creates a new SplitSnapshotManifest.
// myAddr and myMysqlAddr are the local server addresses.
// masterAddr is the address of the server to use as master.
// pos is the replication position to use on that master.
// myMasterPos is the local server master position
func NewSplitSnapshotManifest(myAddr, myMysqlAddr, masterAddr, dbName string, files []SnapshotFile, pos, myMasterPos *ReplicationPosition, startKey, endKey key.HexKeyspaceId, sd *SchemaDefinition) (*SplitSnapshotManifest, error) {
	s, err := startKey.Unhex()
	if err != nil {
		return nil, err
	}
	e, err := endKey.Unhex()
	if err != nil {
		return nil, err
	}
	sm, err := newSnapshotManifest(myAddr, myMysqlAddr, masterAddr, dbName, files, pos, myMasterPos)
	if err != nil {
		return nil, err
	}
	return &SplitSnapshotManifest{Source: sm, KeyRange: key.KeyRange{Start: s, End: e}, SchemaDefinition: sd}, nil
}

// SanityCheckManifests checks if the ssms can be restored together.
func SanityCheckManifests(ssms []*SplitSnapshotManifest) error {
	first := ssms[0]
	for _, ssm := range ssms[1:] {
		if ssm.SchemaDefinition.Version != first.SchemaDefinition.Version {
			return fmt.Errorf("multirestore sanity check: schema versions don't match: %v, %v", ssm, first)
		}
	}
	return nil
}

// In MySQL for both bigint and varbinary, 0x1234 is a valid value. For
// varbinary, it is left justified and for bigint it is correctly
// interpreted. So in all cases, we can use '0x' plus the hex version
// of the values.
// FIXME(alainjobart) use query format/bind vars
var selectIntoOutfile = `SELECT * INTO OUTFILE "{{.TableOutputPath}}"
  CHARACTER SET binary
  FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' ESCAPED BY '\\'
  LINES TERMINATED BY '\n'
  FROM {{.TableName}} WHERE
   {{if .StartKey}}{{ .KeyspaceIdColumnName }} >= 0x{{.StartKey}} {{end}}
   {{if and .StartKey .EndKey}} AND {{end}}
   {{if .EndKey}} {{.KeyspaceIdColumnName}} < 0x{{.EndKey}} {{end}}`

var loadDataInfile = `LOAD DATA INFILE '{{.TableInputPath}}' INTO TABLE {{.TableName}}
  CHARACTER SET binary
  FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' ESCAPED BY '\\'
  LINES TERMINATED BY '\n'`

func (mysqld *Mysqld) validateSplitReplicaTarget() error {
	// check activity
	qr, err := mysqld.fetchSuperQuery("SHOW PROCESSLIST")
	if err != nil {
		return err
	}
	if len(qr.Rows) > 4 {
		return fmt.Errorf("too many active db processes (%v > 4)", len(qr.Rows))
	}

	// make sure we can write locally
	if err := mysqld.ValidateSnapshotPath(); err != nil {
		return err
	}

	// NOTE: we expect that database was already created during tablet
	// assignment, and we'll check that issuing a 'USE dbname' later
	return nil
}

// this function runs on the machine acting as the source for the split
//
// Check master/slave status
// Check paths for storing data
// Create one file per table
// Compress each file
// Compute hash of each file
// Place in /vt/snapshot they will be served by http server (not rpc)

/*
copied from replication.
create a series of raw dump files the contain rows to be reinserted

dbName - mysql db name
keyName - name of the mysql column that is the leading edge of all primary keys
startKey, endKey - the row range to prepare
sourceAddr - the ip addr of the machine running the export
allowHierarchicalReplication - allow replication from a slave
*/
func (mysqld *Mysqld) CreateSplitSnapshot(dbName, keyName string, startKey, endKey key.HexKeyspaceId, sourceAddr string, allowHierarchicalReplication bool, snapshotConcurrency int, hookExtraEnv map[string]string) (snapshotManifestFilename string, err error) {
	if dbName == "" {
		err = fmt.Errorf("no database name provided")
		return
	}
	// same logic applies here
	relog.Info("validateCloneSource")
	if err = mysqld.validateCloneSource(false, hookExtraEnv); err != nil {
		return
	}

	// clean out and start fresh
	relog.Info("removing previous snapshots: %v", mysqld.SnapshotDir)
	if err = os.RemoveAll(mysqld.SnapshotDir); err != nil {
		return
	}

	cloneSourcePath := path.Join(mysqld.SnapshotDir, dataDir, dbName+"-"+string(startKey)+","+string(endKey))
	// clean out and start fresh
	for _, _path := range []string{cloneSourcePath} {
		if err = os.MkdirAll(_path, 0775); err != nil {
			return
		}
	}

	// get the schema for each table
	sd, fetchErr := mysqld.GetSchema(dbName, nil, false)
	if fetchErr != nil {
		return "", fetchErr
	}
	if len(sd.TableDefinitions) == 0 {
		return "", fmt.Errorf("empty table list for %v", dbName)
	}
	sd.SortByReverseDataLength()

	slaveStartRequired, readOnly, replicationPosition, myMasterPosition, masterAddr, err := mysqld.prepareToSnapshot(allowHierarchicalReplication)
	if err != nil {
		return
	}

	defer func() {
		err = replaceError(err, mysqld.restoreAfterSnapshot(slaveStartRequired, readOnly))
	}()

	var ssmFile string
	dataFiles, snapshotErr := mysqld.createSplitSnapshotManifest(dbName, keyName, startKey, endKey, cloneSourcePath, sd, snapshotConcurrency)
	if snapshotErr != nil {
		relog.Error("CreateSplitSnapshotManifest failed: %v", snapshotErr)
		return "", snapshotErr
	} else {
		ssm, err := NewSplitSnapshotManifest(sourceAddr, mysqld.IpAddr(),
			masterAddr, dbName, dataFiles, replicationPosition,
			myMasterPosition, startKey, endKey, sd)
		if err != nil {
			return "", err
		}
		ssmFile = path.Join(cloneSourcePath, partialSnapshotManifestFile)
		if snapshotErr = writeJson(ssmFile, ssm); snapshotErr != nil {
			return "", snapshotErr
		}
	}

	relative, err := filepath.Rel(mysqld.SnapshotDir, ssmFile)
	if err != nil {
		return "", err
	}
	return path.Join(SnapshotURLPath, relative), nil
}

// createSplitSnapshotManifest exports each table to a CSV-like file
// and compresses the results.
func (mysqld *Mysqld) createSplitSnapshotManifest(dbName, keyName string, startKey, endKey key.HexKeyspaceId, cloneSourcePath string, sd *SchemaDefinition, snapshotConcurrency int) ([]SnapshotFile, error) {
	n := len(sd.TableDefinitions)
	errors := make(chan error)
	work := make(chan int, n)

	filenames := make([]string, n)
	compressedFilenames := make([]string, n)
	for i := 0; i < n; i++ {
		td := sd.TableDefinitions[i]
		filenames[i] = path.Join(cloneSourcePath, td.Name+".csv")
		compressedFilenames[i] = filenames[i] + ".gz"
		work <- i
	}
	close(work)

	dataFiles := make([]SnapshotFile, n)

	for i := 0; i < snapshotConcurrency; i++ {
		go func() {
			for i := range work {
				td := sd.TableDefinitions[i]
				relog.Info("Dump table %v...", td.Name)
				filename := filenames[i]
				compressedFilename := compressedFilenames[i]

				// do the SQL query
				queryParams := map[string]string{
					"TableName":            dbName + "." + td.Name,
					"KeyspaceIdColumnName": keyName,
					// FIXME(alainjobart): move these to bind params
					"TableOutputPath": filename,
					"StartKey":        string(startKey),
					"EndKey":          string(endKey),
				}
				sio, err := fillStringTemplate(selectIntoOutfile, queryParams)
				if err != nil {
					errors <- err
					continue
				}
				err = mysqld.executeSuperQuery(sio)
				if err != nil {
					errors <- err
					continue
				}

				// compress the file
				snapshotFile, err := newSnapshotFile(filename, compressedFilename, mysqld.SnapshotDir, true)
				if err == nil {
					dataFiles[i] = *snapshotFile
				}

				errors <- err
			}
		}()
	}
	var err error
	for i := 0; i < n; i++ {
		if dumpErr := <-errors; dumpErr != nil {
			if err != nil {
				relog.Error("Multiple errors, this one happened but won't be returned: %v", err)
			}
			err = dumpErr
		}
	}

	if err != nil {
		// clean up files if we had an error
		// FIXME(alainjobart) it seems extreme to delete all files if
		// the last one failed. Since we only move the file into
		// its destination when it worked, we could assume if the file
		// already exists it's good, and re-compute its hash.
		relog.Info("Error happened, deleting all the files we already compressed")
		for i := 0; i < n; i++ {
			os.Remove(filenames[i])
			os.Remove(compressedFilenames[i])
		}
		return nil, err
	}

	return dataFiles, nil
}

func (mysqld *Mysqld) prepareToSnapshot(allowHierarchicalReplication bool) (slaveStartRequired, readOnly bool, replicationPosition, myMasterPosition *ReplicationPosition, masterAddr string, err error) {
	// save initial state so we can restore on Start()
	if slaveStatus, slaveErr := mysqld.slaveStatus(); slaveErr == nil {
		slaveStartRequired = (slaveStatus["Slave_IO_Running"] == "Yes" && slaveStatus["Slave_SQL_Running"] == "Yes")
	}
	// FIXME(szopa): is this necessary?
	readOnly = true
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

	// If the source is a slave use the master replication position,
	// unless we are allowing hierachical replicas.
	replicationPosition, err = mysqld.SlaveStatus()
	if err != nil {
		if err != ErrNotSlave {
			// this is a real error
			return
		}
		// we are really a master, so we need that position
		replicationPosition, err = mysqld.MasterStatus()
		if err != nil {
			return
		}
		masterAddr = mysqld.IpAddr()
	} else {
		// we are a slave, check our replication strategy
		if allowHierarchicalReplication {
			masterAddr = mysqld.IpAddr()
		} else {
			masterAddr, err = mysqld.GetMasterAddr()
			if err != nil {
				return
			}
		}
	}

	// get our master position, some targets may use it
	myMasterPosition, err = mysqld.MasterStatus()
	if err != nil && err != ErrNotMaster {
		// this is a real error
		return
	}

	relog.Info("Flush tables")
	if err = mysqld.executeSuperQuery("FLUSH TABLES WITH READ LOCK"); err != nil {
		return
	}
	return
}

func (mysqld *Mysqld) restoreAfterSnapshot(slaveStartRequired, readOnly bool) (err error) {
	// Try to fix mysqld regardless of snapshot success..
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
	return nil
}

type namedHasherWriter struct {
	// creation parameters
	filenamePattern string
	snapshotDir     string
	tableName       string
	maximumFilesize uint64

	// our current pipeline
	inputBuffer *bufio2.AsyncWriter
	gzip        *cgzip.Writer
	hasher      *hasher
	fileBuffer  *bufio.Writer
	file        *os.File

	// where we are
	currentSize   uint64
	currentIndex  uint
	snapshotFiles []SnapshotFile
}

func newCompressedNamedHasherWriter(filenamePattern, snapshotDir, tableName string, maximumFilesize uint64) (*namedHasherWriter, error) {
	w := &namedHasherWriter{filenamePattern: filenamePattern, snapshotDir: snapshotDir, tableName: tableName, maximumFilesize: maximumFilesize, snapshotFiles: make([]SnapshotFile, 0, 5)}
	if err := w.Open(); err != nil {
		return nil, err
	}
	return w, nil
}

func (nhw *namedHasherWriter) Open() (err error) {
	// The pipeline looks like this:
	//
	//                             +---> buffer +---> file
	//                             |      32K
	// buffer +---> gzip +---> tee +
	//   32K                       |
	//                             +---> hasher
	//
	// The buffer in front of gzip is needed so that the data is
	// compressed only when there's a reasonable amount of it.

	filename := fmt.Sprintf(nhw.filenamePattern, nhw.currentIndex)
	nhw.file, err = os.Create(filename)
	if err != nil {
		return
	}
	nhw.fileBuffer = bufio.NewWriterSize(nhw.file, 32*1024)
	nhw.hasher = newHasher()
	tee := io.MultiWriter(nhw.fileBuffer, nhw.hasher)
	// create the gzip compression filter
	nhw.gzip, err = cgzip.NewWriterLevel(tee, cgzip.Z_BEST_SPEED)
	if err != nil {
		return
	}
	nhw.inputBuffer = bufio2.NewAsyncWriterSize(nhw.gzip, 32*1024, 3)
	return
}

func (nhw *namedHasherWriter) Close() (err error) {
	// I have to dismantle the pipeline, starting from the
	// top. Some of the elements are flushers, others are closers,
	// which is why this code is so ugly.
	if err = nhw.inputBuffer.Flush(); err != nil {
		return
	}
	if err = nhw.gzip.Close(); err != nil {
		return
	}
	if err = nhw.fileBuffer.Flush(); err != nil {
		return
	}
	filename := nhw.file.Name()
	if err = nhw.file.Close(); err != nil {
		return
	}

	// then add the snapshot file we created to our list
	fi, err := os.Stat(filename)
	if err != nil {
		return err
	}
	relativePath, err := filepath.Rel(nhw.snapshotDir, filename)
	if err != nil {
		return err
	}
	nhw.snapshotFiles = append(nhw.snapshotFiles, SnapshotFile{relativePath, fi.Size(), nhw.hasher.HashString(), nhw.tableName})

	nhw.inputBuffer = nil
	nhw.hasher = nil
	nhw.gzip = nil
	nhw.file = nil
	nhw.fileBuffer = nil
	nhw.currentSize = 0
	return nil
}

func (nhw *namedHasherWriter) Rotate() error {
	if err := nhw.Close(); err != nil {
		return err
	}
	nhw.currentIndex++
	if err := nhw.Open(); err != nil {
		return err
	}
	return nil
}

func (nhw *namedHasherWriter) Write(p []byte) (n int, err error) {
	size := uint64(len(p))
	if size+nhw.currentSize > nhw.maximumFilesize && nhw.currentSize > 0 {
		// if we write this, we'll go over the file limit
		// (make sure we've written something at least to move
		// forward)
		if err := nhw.Rotate(); err != nil {
			return 0, err
		}
	}
	nhw.currentSize += size

	return nhw.inputBuffer.Write(p)
}

// SnapshotFiles returns the snapshot files appropriate for the data
// written by the namedHasherWriter. Calling SnapshotFiles will close
// any outstanding file.
func (nhw *namedHasherWriter) SnapshotFiles() ([]SnapshotFile, error) {
	if nhw.inputBuffer != nil {
		if err := nhw.Close(); err != nil {
			return nil, err
		}
	}
	return nhw.snapshotFiles, nil
}

func (mysqld *Mysqld) dumpTable(td TableDefinition, dbName, keyName, selectIntoOutfile, mainCloneSourcePath string, cloneSourcePaths map[key.KeyRange]string, maximumFilesize uint64) (map[key.KeyRange][]SnapshotFile, error) {
	filename := path.Join(mainCloneSourcePath, td.Name+".csv")
	queryParams := map[string]string{
		"TableName":            dbName + "." + td.Name,
		"Columns":              strings.Join(td.Columns, ", "),
		"KeyspaceIdColumnName": keyName,
		"TableOutputPath":      filename,
	}
	sio, err := fillStringTemplate(selectIntoOutfile, queryParams)
	if err != nil {
		return nil, err
	}
	if err := mysqld.executeSuperQuery(sio); err != nil {
		return nil, err
	}

	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}

	defer func() {
		file.Close()
		if e := os.Remove(filename); e != nil {
			relog.Error("Cannot remove %v: %v", filename, e)
		}
	}()

	hasherWriters := make(map[key.KeyRange]*namedHasherWriter)

	for kr, cloneSourcePath := range cloneSourcePaths {
		filenamePattern := path.Join(cloneSourcePath, td.Name+".%v.csv.gz")
		w, err := newCompressedNamedHasherWriter(filenamePattern, mysqld.SnapshotDir, td.Name, maximumFilesize)
		if err != nil {
			return nil, err
		}
		hasherWriters[kr] = w
	}

	splitter := csvsplitter.NewKeyspaceCSVReader(file, ',')
	for {
		keyspaceId, line, err := splitter.ReadRecord()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		for kr, w := range hasherWriters {
			if kr.Contains(keyspaceId) {
				_, err = w.Write(line)
				if err != nil {
					return nil, err
				}
				break
			}
		}
	}

	snapshotFiles := make(map[key.KeyRange][]SnapshotFile)
	for i, hw := range hasherWriters {
		if snapshotFiles[i], err = hw.SnapshotFiles(); err != nil {
			return nil, err
		}
	}

	return snapshotFiles, nil
}

func (mysqld *Mysqld) CreateMultiSnapshot(keyRanges []key.KeyRange, dbName, keyName string, sourceAddr string, allowHierarchicalReplication bool, snapshotConcurrency int, tables []string, skipSlaveRestart bool, maximumFilesize uint64, hookExtraEnv map[string]string) (snapshotManifestFilenames []string, err error) {
	if dbName == "" {
		err = fmt.Errorf("no database name provided")
		return
	}

	// same logic applies here
	relog.Info("validateCloneSource")
	if err = mysqld.validateCloneSource(false, hookExtraEnv); err != nil {
		return
	}

	// clean out and start fresh
	cloneSourcePaths := make(map[key.KeyRange]string)
	for _, keyRange := range keyRanges {
		cloneSourcePaths[keyRange] = path.Join(mysqld.SnapshotDir, dataDir, dbName+"-"+string(keyRange.Start.Hex())+","+string(keyRange.End.Hex()))
	}
	for _, _path := range cloneSourcePaths {
		if err = os.RemoveAll(_path); err != nil {
			return
		}
		if err = os.MkdirAll(_path, 0775); err != nil {
			return
		}
	}

	mainCloneSourcePath := path.Join(mysqld.SnapshotDir, dataDir, dbName+"-all")
	if err = os.RemoveAll(mainCloneSourcePath); err != nil {
		return
	}
	if err = os.MkdirAll(mainCloneSourcePath, 0775); err != nil {
		return
	}

	// get the schema for each table
	sd, fetchErr := mysqld.GetSchema(dbName, tables, true)
	if fetchErr != nil {
		return []string{}, fetchErr
	}
	if len(sd.TableDefinitions) == 0 {
		return []string{}, fmt.Errorf("empty table list for %v", dbName)
	}
	sd.SortByReverseDataLength()

	slaveStartRequired, readOnly, replicationPosition, myMasterPosition, masterAddr, err := mysqld.prepareToSnapshot(allowHierarchicalReplication)
	if err != nil {
		return
	}
	if skipSlaveRestart {
		if slaveStartRequired {
			relog.Info("Overriding slaveStartRequired to false")
		}
		slaveStartRequired = false
	}
	defer func() {
		err = replaceError(err, mysqld.restoreAfterSnapshot(slaveStartRequired, readOnly))
	}()

	selectIntoOutfile := `SELECT {{.KeyspaceIdColumnName}}, {{.Columns}} INTO OUTFILE "{{.TableOutputPath}}" CHARACTER SET binary FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' ESCAPED BY '\\' LINES TERMINATED BY '\n' FROM {{.TableName}}`

	datafiles := make([]map[key.KeyRange][]SnapshotFile, len(sd.TableDefinitions))
	dumpTableWorker := func(i int) (err error) {
		table := sd.TableDefinitions[i]
		if table.Type != TABLE_BASE_TABLE {
			// we just skip views here
			return nil
		}
		snapshotFiles, err := mysqld.dumpTable(table, dbName, keyName, selectIntoOutfile, mainCloneSourcePath, cloneSourcePaths, maximumFilesize)
		if err != nil {
			return
		}
		datafiles[i] = snapshotFiles
		return nil
	}
	if err = ConcurrentMap(snapshotConcurrency, len(sd.TableDefinitions), dumpTableWorker); err != nil {
		return
	}

	if e := os.Remove(mainCloneSourcePath); e != nil {
		relog.Error("Cannot remove %v: %v", mainCloneSourcePath, e)
	}

	ssmFiles := make([]string, len(keyRanges))
	for i, kr := range keyRanges {
		krDatafiles := make([]SnapshotFile, 0, len(datafiles))
		for _, m := range datafiles {
			krDatafiles = append(krDatafiles, m[kr]...)
		}
		ssm, err := NewSplitSnapshotManifest(sourceAddr, mysqld.IpAddr(),
			masterAddr, dbName, krDatafiles, replicationPosition,
			myMasterPosition, kr.Start.Hex(), kr.End.Hex(), sd)
		if err != nil {
			return nil, err
		}
		ssmFiles[i] = path.Join(cloneSourcePaths[kr], partialSnapshotManifestFile)
		if err = writeJson(ssmFiles[i], ssm); err != nil {
			return nil, err
		}
	}

	snapshotURLPaths := make([]string, len(keyRanges))
	for i := 0; i < len(keyRanges); i++ {
		relative, err := filepath.Rel(mysqld.SnapshotDir, ssmFiles[i])
		if err != nil {
			return []string{}, err
		}
		snapshotURLPaths[i] = path.Join(SnapshotURLPath, relative)
	}
	return snapshotURLPaths, nil
}

type localSnapshotFile struct {
	manifest *SplitSnapshotManifest
	file     *SnapshotFile
	basePath string
}

func (lsf localSnapshotFile) filename() string {
	return lsf.file.getLocalFilename(path.Join(lsf.basePath, lsf.manifest.Source.Addr))
}

func (lsf localSnapshotFile) url() string {
	return "http://" + lsf.manifest.Source.Addr + path.Join(SnapshotURLPath, lsf.file.Path)
}

func (lsf localSnapshotFile) tableName() string {
	return lsf.file.TableName
}

// makeCreateTableSql returns a table creation statement
// that is modified to be faster, and the associated optional
// 'alter table' to modify the table at the end.
// - If the strategy contains the string 'skipAutoIncrement(NNN)' then
// we do not re-add the auto_increment on that table.
// - If the strategy contains the string 'delaySecondaryIndexes',
// then non-primary key indexes will be added afterwards.
// - If the strategy contains the string 'useMyIsam' we load
// the data into a myisam table and we then convert to innodb
// - If the strategy contains the string 'delayPrimaryKey',
// then the primary key index will be added afterwards (use with useMyIsam)
func makeCreateTableSql(schema, tableName string, strategy string) (string, string, error) {
	alters := make([]string, 0, 5)
	lines := strings.Split(schema, "\n")
	delayPrimaryKey := strings.Contains(strategy, "delayPrimaryKey")
	delaySecondaryIndexes := strings.Contains(strategy, "delaySecondaryIndexes")
	useMyIsam := strings.Contains(strategy, "useMyIsam")

	for i, line := range lines {
		if strings.Contains(line, " AUTO_INCREMENT") {
			// only add to the final ALTER TABLE if we're not
			// dropping the AUTO_INCREMENT on the table
			if strings.Contains(strategy, "skipAutoIncrement("+tableName+")") {
				relog.Info("Will not add AUTO_INCREMENT back on table %v", tableName)
			} else {
				alters = append(alters, "MODIFY "+line[:len(line)-1])
			}
			lines[i] = strings.Replace(line, " AUTO_INCREMENT", "", 1)
			continue
		}

		isPrimaryKey := strings.Contains(line, " PRIMARY KEY")
		isSecondaryIndex := !isPrimaryKey && strings.Contains(line, " KEY")
		if (isPrimaryKey && delayPrimaryKey) || (isSecondaryIndex && delaySecondaryIndexes) {

			// remove the comma at the end of the previous line,
			lines[i-1] = lines[i-1][:len(lines[i-1])-1]

			// keep our comma if any (so the next index
			// might remove it)
			// also add the key definition to the alters
			if strings.HasSuffix(line, ",") {
				lines[i] = ","
				alters = append(alters, "ADD "+line[:len(line)-1])
			} else {
				lines[i] = ""
				alters = append(alters, "ADD "+line)
			}
		}

		if useMyIsam && strings.Contains(line, " ENGINE=InnoDB") {
			lines[i] = strings.Replace(line, " ENGINE=InnoDB", " ENGINE=MyISAM", 1)
			alters = append(alters, "ENGINE=InnoDB")
		}
	}

	alter := ""
	if len(alters) > 0 {
		alter = "ALTER TABLE `" + tableName + "` " + strings.Join(alters, ", ")
	}
	return strings.Join(lines, "\n"), alter, nil
}

// buildQueryList builds the list of queries to use to run the provided
// query on the provided database
func buildQueryList(destinationDbName, query string, writeBinLogs bool) []string {
	queries := make([]string, 0, 4)
	if !writeBinLogs {
		queries = append(queries, "SET sql_log_bin = OFF")
		queries = append(queries, "SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED")
	}
	queries = append(queries, "USE `"+destinationDbName+"`")
	queries = append(queries, query)
	return queries
}

// RestoreFromMultiSnapshot is the main entry point for multi restore.
// - If the strategy contains the string 'writeBinLogs' then we will
//   also write to the binary logs.
// - If the strategy contains the command 'populateBlpRecovery(NNN)' then we
//   will populate the blp_checkpoint table with master positions to start from
func (mysqld *Mysqld) RestoreFromMultiSnapshot(destinationDbName string, keyRange key.KeyRange, sourceAddrs []*url.URL, uids []uint32, snapshotConcurrency, fetchConcurrency, insertTableConcurrency, fetchRetryCount int, strategy string) (err error) {
	writeBinLogs := strings.Contains(strategy, "writeBinLogs")

	manifests := make([]*SplitSnapshotManifest, len(sourceAddrs))
	rc := concurrency.NewResourceConstraint(fetchConcurrency)
	for i, sourceAddr := range sourceAddrs {
		rc.Add(1)
		go func(sourceAddr *url.URL, i int) {
			rc.Acquire()
			defer rc.ReleaseAndDone()
			if rc.HasErrors() {
				return
			}

			var sourceDbName string
			if len(sourceAddr.Path) < 2 { // "" or "/"
				sourceDbName = destinationDbName
			} else {
				sourceDbName = sourceAddr.Path[1:]
			}
			ssm, e := fetchSnapshotManifestWithRetry("http://"+sourceAddr.Host, sourceDbName, keyRange, fetchRetryCount)
			manifests[i] = ssm
			rc.RecordError(e)
		}(sourceAddr, i)
	}
	if err = rc.Wait(); err != nil {
		return
	}

	if e := SanityCheckManifests(manifests); e != nil {
		return e
	}

	tempStoragePath := path.Join(mysqld.SnapshotDir, "multirestore", destinationDbName)

	// Start fresh
	if err = os.RemoveAll(tempStoragePath); err != nil {
		return
	}

	if err = os.MkdirAll(tempStoragePath, 0775); err != nil {
		return err
	}

	defer func() {
		if e := os.RemoveAll(tempStoragePath); e != nil {
			relog.Error("error removing %v: %v", tempStoragePath, e)
		}

	}()

	// Handle our concurrency:
	// - fetchConcurrency tasks for network
	// - insertTableConcurrency for table inserts from a file
	//   into an innodb table
	// - snapshotConcurrency tasks for table inserts / modify tables
	sems := make(map[string]sync2.Semaphore, len(manifests[0].SchemaDefinition.TableDefinitions)+3)
	sems["net"] = sync2.NewSemaphore(fetchConcurrency)
	sems["db"] = sync2.NewSemaphore(snapshotConcurrency)

	// Store the alter table statements for after restore,
	// and how many jobs we're running on each table
	// TODO(alainjobart) the jobCount map is a bit weird. replace it
	// with a map of WaitGroups, initialized to the number of files
	// per table. Have extra go routines for the tables with auto_increment
	// to wait on the waitgroup, and apply the modify_table.
	postSql := make(map[string]string, len(manifests[0].SchemaDefinition.TableDefinitions))
	jobCount := make(map[string]*sync2.AtomicInt32)

	// Create the database (it's a good check to know if we're running
	// multirestore a second time too!)
	manifest := manifests[0] // I am assuming they all match
	createDatabase, e := fillStringTemplate(manifest.SchemaDefinition.DatabaseSchema, map[string]string{"DatabaseName": destinationDbName})
	if e != nil {
		return e
	}
	if createDatabase == "" {
		return fmt.Errorf("Empty create database statement")
	}

	createDbCmds := make([]string, 0, len(manifest.SchemaDefinition.TableDefinitions)+2)
	if !writeBinLogs {
		createDbCmds = append(createDbCmds, "SET sql_log_bin = OFF")
	}
	createDbCmds = append(createDbCmds, createDatabase)
	createDbCmds = append(createDbCmds, "USE `"+destinationDbName+"`")
	createViewCmds := make([]string, 0, 16)
	for _, td := range manifest.SchemaDefinition.TableDefinitions {
		if td.Type == TABLE_BASE_TABLE {
			createDbCmd, alterTable, err := makeCreateTableSql(td.Schema, td.Name, strategy)
			if err != nil {
				return err
			}
			if alterTable != "" {
				postSql[td.Name] = alterTable
			}
			jobCount[td.Name] = new(sync2.AtomicInt32)
			createDbCmds = append(createDbCmds, createDbCmd)
			sems["table-"+td.Name] = sync2.NewSemaphore(insertTableConcurrency)
		} else {
			// views are just created with the right db name
			// and no data will ever go in them. We create them
			// after all tables are created, as they will
			// probably depend on real tables.
			createViewCmd, err := fillStringTemplate(td.Schema, map[string]string{"DatabaseName": destinationDbName})
			if err != nil {
				return err
			}
			createViewCmds = append(createViewCmds, createViewCmd)
		}
	}
	createDbCmds = append(createDbCmds, createViewCmds...)
	if err = mysqld.executeSuperQueryList(createDbCmds); err != nil {
		return
	}

	// compute how many jobs we will have
	for _, manifest := range manifests {
		for _, file := range manifest.Source.Files {
			jobCount[file.TableName].Add(1)
		}
	}

	loadDataInfile := `LOAD DATA INFILE '{{.TableInputPath}}' INTO TABLE {{.TableName}} CHARACTER SET binary FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' ESCAPED BY '\\' LINES TERMINATED BY '\n' ({{.Columns}})`

	// fetch all the csv files, and apply them one at a time. Note
	// this might start many go routines, and they'll all be
	// waiting on the resource semaphores.
	mrc := concurrency.NewMultiResourceConstraint(sems)
	for manifestIndex, manifest := range manifests {
		if err = os.Mkdir(path.Join(tempStoragePath, manifest.Source.Addr), 0775); err != nil {
			return err
		}

		for i := range manifest.Source.Files {
			lsf := localSnapshotFile{manifest: manifest, file: &manifest.Source.Files[i], basePath: tempStoragePath}
			mrc.Add(1)
			go func(manifestIndex, i int) {
				defer mrc.Done()

				// compute a few things now, so if we can't we
				// don't take resources:
				// - get the schema
				td, ok := manifest.SchemaDefinition.GetTable(lsf.tableName())
				if !ok {
					mrc.RecordError(fmt.Errorf("No table named %v in schema", lsf.tableName()))
					return
				}

				// - get the load data statement
				queryParams := map[string]string{
					"TableInputPath": lsf.filename(),
					"TableName":      lsf.tableName(),
					"Columns":        strings.Join(td.Columns, ", "),
				}
				loadStatement, e := fillStringTemplate(loadDataInfile, queryParams)
				if e != nil {
					mrc.RecordError(e)
					return
				}

				// get the file, using the 'net' resource
				mrc.Acquire("net")
				if mrc.HasErrors() {
					mrc.Release("net")
					return
				}
				e = fetchFileWithRetry(lsf.url(), lsf.file.Hash, lsf.filename(), fetchRetryCount)
				mrc.Release("net")
				if e != nil {
					mrc.RecordError(e)
					return
				}
				defer os.Remove(lsf.filename())

				// acquire the table lock (we do this first
				// so we maximize access to db. Otherwise
				// if 8 threads had gotten the db lock but
				// were writing to the same table, only one
				// load would go at once)
				tableLockName := "table-" + lsf.tableName()
				mrc.Acquire(tableLockName)
				defer func() {
					mrc.Release(tableLockName)
				}()
				if mrc.HasErrors() {
					return
				}

				// acquire the db lock
				mrc.Acquire("db")
				defer func() {
					mrc.Release("db")
				}()
				if mrc.HasErrors() {
					return
				}

				// load the data in
				queries := buildQueryList(destinationDbName, loadStatement, writeBinLogs)
				e = mysqld.executeSuperQueryList(queries)
				if e != nil {
					mrc.RecordError(e)
					return
				}

				// if we're running the last insert,
				// potentially re-add the auto-increments
				remainingInserts := jobCount[lsf.tableName()].Add(-1)
				if remainingInserts == 0 && postSql[lsf.tableName()] != "" {
					queries = buildQueryList(destinationDbName, postSql[lsf.tableName()], writeBinLogs)
					e = mysqld.executeSuperQueryList(queries)
					if e != nil {
						mrc.RecordError(e)
						return
					}
				}
			}(manifestIndex, i)
		}
	}

	if err = mrc.Wait(); err != nil {
		return err
	}

	// populate blp_checkpoint table if we want to
	if start := strings.Index(strategy, "populateBlpRecovery("); start != -1 {
		param := strategy[start+len("populateBlpRecovery("):]
		param = param[:strings.Index(param, ")")]
		port, err := strconv.ParseInt(param, 0, 32)
		if err != nil {
			return err
		}

		queries := make([]string, 0, 4)
		queries = append(queries, "USE `_vt`")
		if !writeBinLogs {
			queries = append(queries, "SET sql_log_bin = OFF")
			queries = append(queries, "SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED")
		}
		for manifestIndex, manifest := range manifests {
			insertRecovery := fmt.Sprintf(INSERT_INTO_RECOVERY,
				uids[manifestIndex],
				manifest.Source.MasterState.MasterHost,
				port,
				manifest.Source.MasterState.ReplicationPosition.MasterLogFile,
				manifest.Source.MasterState.ReplicationPosition.MasterLogPosition,
				"", 0, 0,
				keyRange.Start.Hex(),
				keyRange.End.Hex(),
				time.Now().Unix())
			queries = append(queries, insertRecovery)
		}
		if err = mysqld.executeSuperQueryList(queries); err != nil {
			return err
		}
	}
	return nil
}

/*
 This piece runs on the presumably empty machine acting as the target in the
 create replica action.

 validate target (self)
 shutdown_mysql()
 create temp data directory /vt/target/vt_<keyspace>
 copy compressed data files via HTTP
 verify hash of compressed files
 uncompress into /vt/vt_<target-uid>/data/vt_<keyspace>
 start_mysql()
 clean up compressed files
*/
func (mysqld *Mysqld) RestoreFromPartialSnapshot(snapshotManifest *SplitSnapshotManifest, fetchConcurrency, fetchRetryCount int) (err error) {
	if err = mysqld.validateSplitReplicaTarget(); err != nil {
		return
	}

	tempStoragePath := path.Join(mysqld.SnapshotDir, "partialrestore")
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

	// this will check that the database was properly created
	createDbCmds := []string{
		"USE " + snapshotManifest.Source.DbName}
	for _, td := range snapshotManifest.SchemaDefinition.TableDefinitions {
		createDbCmds = append(createDbCmds, td.Schema)
	}

	if err = mysqld.executeSuperQueryList(createDbCmds); err != nil {
		return
	}

	if err = fetchFiles(snapshotManifest.Source, tempStoragePath, fetchConcurrency, fetchRetryCount); err != nil {
		return
	}

	// FIXME(alainjobart) We recompute a lot of stuff that should be
	// in fileutil.go
	for _, fi := range snapshotManifest.Source.Files {
		filename := fi.getLocalFilename(tempStoragePath)
		tableName := strings.Replace(path.Base(filename), ".csv", "", -1)
		queryParams := map[string]string{
			"TableInputPath": filename,
			"TableName":      snapshotManifest.Source.DbName + "." + tableName,
		}
		var query string
		query, err = fillStringTemplate(loadDataInfile, queryParams)
		if err != nil {
			return
		}
		if err = mysqld.executeSuperQuery(query); err != nil {
			// FIXME(msolomon) on abort, we should just tear down
			// alternatively, we could just leave it and wait for the wrangler to
			// notice and start cleaning up
			return
		}

		relog.Info("%v ready", filename)
	}

	cmdList, err := StartSplitReplicationCommands(mysqld, snapshotManifest.Source.ReplicationState, snapshotManifest.KeyRange)
	if err != nil {
		return
	}
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

func ReadSplitSnapshotManifest(filename string) (*SplitSnapshotManifest, error) {
	data, err := ioutil.ReadFile(filename)
	if err != nil {
		return nil, err
	}
	ssm := new(SplitSnapshotManifest)
	if err = json.Unmarshal(data, ssm); err != nil {
		return nil, fmt.Errorf("ReadSplitSnapshotManifest failed: %v %v", filename, err)
	}
	return ssm, nil
}
