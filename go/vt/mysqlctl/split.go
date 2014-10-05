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
	"fmt"
	"io"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"time"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/bufio2"
	"github.com/youtube/vitess/go/cgzip"
	"github.com/youtube/vitess/go/sync2"
	"github.com/youtube/vitess/go/vt/binlog/binlogplayer"
	"github.com/youtube/vitess/go/vt/concurrency"
	"github.com/youtube/vitess/go/vt/dbconnpool"
	"github.com/youtube/vitess/go/vt/hook"
	"github.com/youtube/vitess/go/vt/key"
	"github.com/youtube/vitess/go/vt/mysqlctl/csvsplitter"
	"github.com/youtube/vitess/go/vt/mysqlctl/proto"
)

const (
	partialSnapshotManifestFile = "partial_snapshot_manifest.json"
	SnapshotURLPath             = "/snapshot"
)

// replaceError replaces original with recent if recent is not nil,
// logging original if it wasn't nil. This should be used in deferred
// cleanup functions if they change the returned error.
func replaceError(original, recent error) error {
	if recent == nil {
		return original
	}
	if original != nil {
		log.Errorf("One of multiple error: %v", original)
	}
	return recent
}

type SplitSnapshotManifest struct {
	// Source describes the files and our tablet
	Source *SnapshotManifest

	// KeyRange describes the data present in this snapshot
	// When splitting 40-80 into 40-60 and 60-80, this would
	// have 40-60 for instance.
	KeyRange key.KeyRange

	// The schema for this server
	SchemaDefinition *proto.SchemaDefinition
}

// NewSplitSnapshotManifest creates a new SplitSnapshotManifest.
// myAddr and myMysqlAddr are the local server addresses.
// masterAddr is the address of the server to use as master.
// pos is the replication position to use on that master.
// myMasterPos is the local server master position
func NewSplitSnapshotManifest(myAddr, myMysqlAddr, masterAddr, dbName string, files []SnapshotFile, pos, myMasterPos proto.ReplicationPosition, keyRange key.KeyRange, sd *proto.SchemaDefinition) (*SplitSnapshotManifest, error) {
	sm, err := newSnapshotManifest(myAddr, myMysqlAddr, masterAddr, dbName, files, pos, myMasterPos)
	if err != nil {
		return nil, err
	}
	return &SplitSnapshotManifest{
		Source:           sm,
		KeyRange:         keyRange,
		SchemaDefinition: sd,
	}, nil
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

// getReplicationPositionForClones returns what position the clones
// need to replicate from. Can be ours if we are a master, or our master's.
func (mysqld *Mysqld) getReplicationPositionForClones(allowHierarchicalReplication bool) (replicationPosition proto.ReplicationPosition, masterAddr string, err error) {
	// If the source is a slave use the master replication position,
	// unless we are allowing hierachical replicas.
	var status *proto.ReplicationStatus
	status, err = mysqld.SlaveStatus()
	if err == ErrNotSlave {
		// we are really a master, so we need that position
		replicationPosition, err = mysqld.MasterPosition()
		if err != nil {
			return
		}
		masterAddr = mysqld.IpAddr()
		return
	}
	if err != nil {
		return
	}
	replicationPosition = status.Position

	// we are a slave, check our replication strategy
	if allowHierarchicalReplication {
		masterAddr = mysqld.IpAddr()
	} else {
		masterAddr, err = mysqld.GetMasterAddr()
	}
	return
}

func (mysqld *Mysqld) prepareToSnapshot(allowHierarchicalReplication bool, hookExtraEnv map[string]string) (slaveStartRequired, readOnly bool, replicationPosition, myMasterPosition proto.ReplicationPosition, masterAddr string, connToRelease dbconnpool.PoolConnection, err error) {
	// save initial state so we can restore on Start()
	if slaveStatus, slaveErr := mysqld.SlaveStatus(); slaveErr == nil {
		slaveStartRequired = slaveStatus.SlaveRunning()
	}

	// For masters, set read-only so we don't write anything during snapshot
	readOnly = true
	if readOnly, err = mysqld.IsReadOnly(); err != nil {
		return
	}

	log.Infof("Set Read Only")
	if !readOnly {
		mysqld.SetReadOnly(true)
	}
	log.Infof("Stop Slave")
	if err = mysqld.StopSlave(hookExtraEnv); err != nil {
		return
	}

	// Get the replication position and master addr
	replicationPosition, masterAddr, err = mysqld.getReplicationPositionForClones(allowHierarchicalReplication)
	if err != nil {
		return
	}

	// get our master position, some targets may use it
	myMasterPosition, err = mysqld.MasterPosition()
	if err != nil && err != ErrNotMaster {
		// this is a real error
		return
	}

	log.Infof("Flush tables")
	if connToRelease, err = mysqld.dbaPool.Get(); err != nil {
		return
	}
	log.Infof("exec FLUSH TABLES WITH READ LOCK")
	if _, err = connToRelease.ExecuteFetch("FLUSH TABLES WITH READ LOCK", 10000, false); err != nil {
		connToRelease.Recycle()
		return
	}

	return
}

func (mysqld *Mysqld) restoreAfterSnapshot(slaveStartRequired, readOnly bool, hookExtraEnv map[string]string, connToRelease dbconnpool.PoolConnection) (err error) {
	// Try to fix mysqld regardless of snapshot success..
	log.Infof("exec UNLOCK TABLES")
	_, err = connToRelease.ExecuteFetch("UNLOCK TABLES", 10000, false)
	connToRelease.Recycle()
	if err != nil {
		return fmt.Errorf("failed to UNLOCK TABLES: %v", err)
	}

	// restore original mysqld state that we saved above
	if slaveStartRequired {
		if err = mysqld.StartSlave(hookExtraEnv); err != nil {
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

// dumpTableSplit will dump a table, and then split it according to keyspace_id
// into multiple files.
func (mysqld *Mysqld) dumpTableSplit(td *proto.TableDefinition, dbName, keyName string, keyType key.KeyspaceIdType, mainCloneSourcePath string, cloneSourcePaths map[key.KeyRange]string, maximumFilesize uint64) (map[key.KeyRange][]SnapshotFile, error) {
	filename := path.Join(mainCloneSourcePath, td.Name+".csv")
	selectIntoOutfile := `SELECT {{.KeyspaceIdColumnName}}, {{.Columns}} INTO OUTFILE "{{.TableOutputPath}}" CHARACTER SET binary FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' ESCAPED BY '\\' LINES TERMINATED BY '\n' FROM {{.TableName}}`
	queryParams := map[string]string{
		"TableName":            dbName + "." + td.Name,
		"Columns":              strings.Join(td.Columns, ", "),
		"KeyspaceIdColumnName": keyName,
		"TableOutputPath":      filename,
	}
	numberColumn := true
	if keyType == key.KIT_BYTES {
		numberColumn = false
		queryParams["KeyspaceIdColumnName"] = "HEX(" + keyName + ")"
	}
	sio, err := fillStringTemplate(selectIntoOutfile, queryParams)
	if err != nil {
		return nil, fmt.Errorf("fillStringTemplate for %v: %v", td.Name, err)
	}
	if err := mysqld.ExecuteSuperQuery(sio); err != nil {
		return nil, fmt.Errorf("ExecuteSuperQuery failed for %v with query %v: %v", td.Name, sio, err)
	}

	file, err := os.Open(filename)
	if err != nil {
		return nil, fmt.Errorf("Cannot open file %v for table %v: %v", filename, td.Name, err)
	}

	defer func() {
		file.Close()
		if e := os.Remove(filename); e != nil {
			log.Errorf("Cannot remove %v: %v", filename, e)
		}
	}()

	hasherWriters := make(map[key.KeyRange]*namedHasherWriter)

	for kr, cloneSourcePath := range cloneSourcePaths {
		filenamePattern := path.Join(cloneSourcePath, td.Name+".%v.csv.gz")
		w, err := newCompressedNamedHasherWriter(filenamePattern, mysqld.SnapshotDir, td.Name, maximumFilesize)
		if err != nil {
			return nil, fmt.Errorf("newCompressedNamedHasherWriter failed for %v: %v", td.Name, err)
		}
		hasherWriters[kr] = w
	}

	splitter := csvsplitter.NewKeyspaceCSVReader(file, ',', numberColumn)
	for {
		keyspaceId, line, err := splitter.ReadRecord()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("ReadRecord failed for table %v: %v", td.Name, err)
		}
		for kr, w := range hasherWriters {
			if kr.Contains(keyspaceId) {
				_, err = w.Write(line)
				if err != nil {
					return nil, fmt.Errorf("Write failed for %v: %v", td.Name, err)
				}
				break
			}
		}
	}

	snapshotFiles := make(map[key.KeyRange][]SnapshotFile)
	for i, hw := range hasherWriters {
		if snapshotFiles[i], err = hw.SnapshotFiles(); err != nil {
			return nil, fmt.Errorf("SnapshotFiles failed for %v: %v", td.Name, err)
		}
	}

	return snapshotFiles, nil
}

// dumpTableFull will dump the contents of a full table, and then
// chunk it up in multiple compressed files.
func (mysqld *Mysqld) dumpTableFull(td *proto.TableDefinition, dbName, mainCloneSourcePath string, cloneSourcePath string, maximumFilesize uint64) ([]SnapshotFile, error) {
	filename := path.Join(mainCloneSourcePath, td.Name+".csv")
	selectIntoOutfile := `SELECT {{.Columns}} INTO OUTFILE "{{.TableOutputPath}}" CHARACTER SET binary FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' ESCAPED BY '\\' LINES TERMINATED BY '\n' FROM {{.TableName}}`
	queryParams := map[string]string{
		"TableName":       dbName + "." + td.Name,
		"Columns":         strings.Join(td.Columns, ", "),
		"TableOutputPath": filename,
	}
	sio, err := fillStringTemplate(selectIntoOutfile, queryParams)
	if err != nil {
		return nil, err
	}
	if err := mysqld.ExecuteSuperQuery(sio); err != nil {
		return nil, err
	}

	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}

	defer func() {
		file.Close()
		if e := os.Remove(filename); e != nil {
			log.Errorf("Cannot remove %v: %v", filename, e)
		}
	}()

	filenamePattern := path.Join(cloneSourcePath, td.Name+".%v.csv.gz")
	hasherWriter, err := newCompressedNamedHasherWriter(filenamePattern, mysqld.SnapshotDir, td.Name, maximumFilesize)
	if err != nil {
		return nil, err
	}

	splitter := csvsplitter.NewCSVReader(file, ',')
	for {
		line, err := splitter.ReadRecord()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		_, err = hasherWriter.Write(line)
		if err != nil {
			return nil, err
		}
	}

	return hasherWriter.SnapshotFiles()
}

// CreateMultiSnapshot create snapshots of the data.
// - for a resharding snapshot, keyRanges+keyName+keyType are set,
//   and tables is empty. This action will create multiple snapshots,
//   one per keyRange.
// - for a vertical split, tables is set, keyRanges = [KeyRange{}] and
//   keyName+keyType are empty. It will create a single snapshot of
//   the contents of the tables.
// Note combinations of table subset and keyranges are not supported.
func (mysqld *Mysqld) CreateMultiSnapshot(keyRanges []key.KeyRange, dbName, keyName string, keyType key.KeyspaceIdType, sourceAddr string, allowHierarchicalReplication bool, snapshotConcurrency int, tables, excludeTables []string, skipSlaveRestart bool, maximumFilesize uint64, hookExtraEnv map[string]string) (snapshotManifestFilenames []string, err error) {
	if dbName == "" {
		err = fmt.Errorf("no database name provided")
		return
	}
	if len(tables) > 0 {
		if len(keyRanges) != 1 || keyRanges[0].IsPartial() {
			return nil, fmt.Errorf("With tables specified, can only have one full KeyRange")
		}
	}

	// same logic applies here
	log.Infof("validateCloneSource")
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
	sd, fetchErr := mysqld.GetSchema(dbName, tables, excludeTables, true)
	if fetchErr != nil {
		return []string{}, fetchErr
	}
	if len(sd.TableDefinitions) == 0 {
		return []string{}, fmt.Errorf("empty table list for %v", dbName)
	}
	sd.SortByReverseDataLength()

	// prepareToSnapshot will get the tablet in the rigth state,
	// and return the current mysql status.
	slaveStartRequired, readOnly, replicationPosition, myMasterPosition, masterAddr, conn, err := mysqld.prepareToSnapshot(allowHierarchicalReplication, hookExtraEnv)
	if err != nil {
		return
	}
	if skipSlaveRestart {
		if slaveStartRequired {
			log.Infof("Overriding slaveStartRequired to false")
		}
		slaveStartRequired = false
	}
	defer func() {
		err = replaceError(err, mysqld.restoreAfterSnapshot(slaveStartRequired, readOnly, hookExtraEnv, conn))
	}()

	// dump the files in parallel with a pre-defined concurrency
	datafiles := make([]map[key.KeyRange][]SnapshotFile, len(sd.TableDefinitions))
	dumpTableWorker := func(i int) (err error) {
		table := sd.TableDefinitions[i]
		if table.Type != proto.TABLE_BASE_TABLE {
			// we just skip views here
			return nil
		}
		if len(tables) > 0 {
			sfs, err := mysqld.dumpTableFull(table, dbName, mainCloneSourcePath, cloneSourcePaths[key.KeyRange{}], maximumFilesize)
			if err != nil {
				return err
			}
			datafiles[i] = map[key.KeyRange][]SnapshotFile{
				key.KeyRange{}: sfs,
			}
		} else {
			datafiles[i], err = mysqld.dumpTableSplit(table, dbName, keyName, keyType, mainCloneSourcePath, cloneSourcePaths, maximumFilesize)
		}
		return
	}
	if err = ConcurrentMap(snapshotConcurrency, len(sd.TableDefinitions), dumpTableWorker); err != nil {
		return
	}

	if e := os.Remove(mainCloneSourcePath); e != nil {
		log.Errorf("Cannot remove %v: %v", mainCloneSourcePath, e)
	}

	// Check the replication position after snapshot is done
	// hasn't changed, to be sure we haven't inserted any data
	newReplicationPosition, _, err := mysqld.getReplicationPositionForClones(allowHierarchicalReplication)
	if err != nil {
		return
	}
	if !newReplicationPosition.Equal(replicationPosition) {
		return nil, fmt.Errorf("replicationPosition position changed during snapshot, from %v to %v", replicationPosition, newReplicationPosition)
	}

	// Write all the manifest files
	ssmFiles := make([]string, len(keyRanges))
	for i, kr := range keyRanges {
		krDatafiles := make([]SnapshotFile, 0, len(datafiles))
		for _, m := range datafiles {
			krDatafiles = append(krDatafiles, m[kr]...)
		}
		ssm, err := NewSplitSnapshotManifest(sourceAddr, mysqld.IpAddr(),
			masterAddr, dbName, krDatafiles, replicationPosition,
			myMasterPosition, kr, sd)
		if err != nil {
			return nil, err
		}
		ssmFiles[i] = path.Join(cloneSourcePaths[kr], partialSnapshotManifestFile)
		if err = writeJson(ssmFiles[i], ssm); err != nil {
			return nil, err
		}
	}

	// Call the (optional) hook to send the files somewhere else
	wg := sync.WaitGroup{}
	rec := concurrency.AllErrorRecorder{}
	for _, kr := range keyRanges {
		wg.Add(1)
		go func(kr key.KeyRange) {
			defer wg.Done()
			h := hook.NewSimpleHook("copy_snapshot_to_storage")
			h.ExtraEnv = make(map[string]string)
			for k, v := range hookExtraEnv {
				h.ExtraEnv[k] = v
			}
			h.ExtraEnv["KEYRANGE"] = fmt.Sprintf("%v-%v", kr.Start.Hex(), kr.End.Hex())
			h.ExtraEnv["SNAPSHOT_PATH"] = cloneSourcePaths[kr]
			rec.RecordError(h.ExecuteOptional())
		}(kr)
	}
	wg.Wait()
	if rec.HasErrors() {
		return nil, err
	}

	// Return all the URLs for the MANIFESTs
	snapshotURLPaths := make([]string, len(keyRanges))
	for i := 0; i < len(keyRanges); i++ {
		relative, err := filepath.Rel(mysqld.SnapshotDir, ssmFiles[i])
		if err != nil {
			return nil, err
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

// MakeSplitCreateTableSql returns a table creation statement
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
func MakeSplitCreateTableSql(schema, databaseName, tableName string, strategy string) (string, string, error) {
	alters := make([]string, 0, 5)
	lines := strings.Split(schema, "\n")
	delayPrimaryKey := strings.Contains(strategy, "delayPrimaryKey")
	delaySecondaryIndexes := strings.Contains(strategy, "delaySecondaryIndexes")
	useMyIsam := strings.Contains(strategy, "useMyIsam")

	for i, line := range lines {
		if strings.HasPrefix(line, "CREATE TABLE `") {
			lines[i] = strings.Replace(line, "CREATE TABLE `", "CREATE TABLE `"+databaseName+"`.`", 1)
			continue
		}

		if strings.Contains(line, " AUTO_INCREMENT") {
			// only add to the final ALTER TABLE if we're not
			// dropping the AUTO_INCREMENT on the table
			if strings.Contains(strategy, "skipAutoIncrement("+tableName+")") {
				log.Infof("Will not add AUTO_INCREMENT back on table %v", tableName)
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
		alter = "ALTER TABLE `" + databaseName + "`.`" + tableName + "` " + strings.Join(alters, ", ")
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

// MultiRestore is the main entry point for multi restore.
//
// We will either:
// - read from the network if sourceAddrs != nil
// - read from a disk snapshot if fromStoragePaths != nil
//
// The strategy is used as follows:
// - If it contains the string 'writeBinLogs' then we will also write
//   to the binary logs.
// - If it contains the command 'populateBlpCheckpoint' then we will
//   populate the blp_checkpoint table with master positions to start from
//   - If is also contains the command 'dontStartBinlogPlayer' we won't
//   start binlog replication on the destination (but it will be configured)
func (mysqld *Mysqld) MultiRestore(destinationDbName string, keyRanges []key.KeyRange, sourceAddrs []*url.URL, fromStoragePaths []string, snapshotConcurrency, fetchConcurrency, insertTableConcurrency, fetchRetryCount int, strategy string) (err error) {
	writeBinLogs := strings.Contains(strategy, "writeBinLogs")

	var manifests []*SplitSnapshotManifest
	if sourceAddrs != nil {
		// get the manifests from the network
		manifests = make([]*SplitSnapshotManifest, len(sourceAddrs))
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
				ssm, e := fetchSnapshotManifestWithRetry("http://"+sourceAddr.Host, sourceDbName, keyRanges[i], fetchRetryCount)
				manifests[i] = ssm
				rc.RecordError(e)
			}(sourceAddr, i)
		}
		if err = rc.Wait(); err != nil {
			return
		}
	} else {
		// get the manifests from the local snapshots
		manifests = make([]*SplitSnapshotManifest, len(fromStoragePaths))
		for i, fromStoragePath := range fromStoragePaths {
			var err error
			manifests[i], err = readSnapshotManifest(fromStoragePath)
			if err != nil {
				return err
			}
		}
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
			log.Errorf("error removing %v: %v", tempStoragePath, e)
		}

	}()

	// Handle our concurrency:
	// - fetchConcurrency tasks for network / decompress from disk
	// - insertTableConcurrency for table inserts from a file
	//   into an innodb table
	// - snapshotConcurrency tasks for table inserts / modify tables
	sems := make(map[string]*sync2.Semaphore, len(manifests[0].SchemaDefinition.TableDefinitions)+2)
	sems["net"] = sync2.NewSemaphore(fetchConcurrency, 0)
	sems["db"] = sync2.NewSemaphore(snapshotConcurrency, 0)

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
		if td.Type == proto.TABLE_BASE_TABLE {
			createDbCmd, alterTable, err := MakeSplitCreateTableSql(td.Schema, destinationDbName, td.Name, strategy)
			if err != nil {
				return err
			}
			if alterTable != "" {
				postSql[td.Name] = alterTable
			}
			jobCount[td.Name] = new(sync2.AtomicInt32)
			createDbCmds = append(createDbCmds, createDbCmd)
			sems["table-"+td.Name] = sync2.NewSemaphore(insertTableConcurrency, 0)
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
	if err = mysqld.ExecuteSuperQueryList(createDbCmds); err != nil {
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
				if sourceAddrs == nil {
					e = uncompressLocalFile(path.Join(fromStoragePaths[manifestIndex], path.Base(lsf.file.Path)), lsf.file.Hash, lsf.filename())
				} else {
					e = fetchFileWithRetry(lsf.url(), lsf.file.Hash, lsf.filename(), fetchRetryCount)
				}
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
				e = mysqld.ExecuteSuperQueryList(queries)
				if e != nil {
					mrc.RecordError(e)
					return
				}

				// if we're running the last insert,
				// potentially re-add the auto-increments
				remainingInserts := jobCount[lsf.tableName()].Add(-1)
				if remainingInserts == 0 && postSql[lsf.tableName()] != "" {
					queries = buildQueryList(destinationDbName, postSql[lsf.tableName()], writeBinLogs)
					e = mysqld.ExecuteSuperQueryList(queries)
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
	if strings.Index(strategy, "populateBlpCheckpoint") != -1 {
		queries := make([]string, 0, 4)
		if !writeBinLogs {
			queries = append(queries, "SET sql_log_bin = OFF")
			queries = append(queries, "SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED")
		}
		queries = append(queries, binlogplayer.CreateBlpCheckpoint()...)
		flags := ""
		if strings.Index(strategy, "dontStartBinlogPlayer") != -1 {
			flags = binlogplayer.BLP_FLAG_DONT_START
		}
		for manifestIndex, manifest := range manifests {
			queries = append(queries, binlogplayer.PopulateBlpCheckpoint(uint32(manifestIndex), manifest.Source.MasterPosition, time.Now().Unix(), flags))
		}
		if err = mysqld.ExecuteSuperQueryList(queries); err != nil {
			return err
		}
	}
	return nil
}
