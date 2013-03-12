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
	"strings"

	"code.google.com/p/vitess/go/cgzip"
	"code.google.com/p/vitess/go/relog"
	"code.google.com/p/vitess/go/vt/key"
	"code.google.com/p/vitess/go/vt/mysqlctl/csvsplitter"
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
		relog.Error("One of multiple error: %v", original)
	}
	return recent
}

type SplitSnapshotManifest struct {
	Source           *SnapshotManifest
	KeyRange         key.KeyRange
	SchemaDefinition *SchemaDefinition
}

func NewSplitSnapshotManifest(addr, mysqlAddr, dbName string, files []SnapshotFile, pos *ReplicationPosition, startKey, endKey key.HexKeyspaceId, sd *SchemaDefinition) (*SplitSnapshotManifest, error) {
	s, err := startKey.Unhex()
	if err != nil {
		return nil, err
	}
	e, err := endKey.Unhex()
	if err != nil {
		return nil, err
	}
	sm, err := newSnapshotManifest(addr, mysqlAddr, dbName, files, pos)
	if err != nil {
		return nil, err
	}
	return &SplitSnapshotManifest{Source: sm, KeyRange: key.KeyRange{Start: s, End: e}, SchemaDefinition: sd}, nil
}

// SanityCheckManifests checks if the ssms can be restored together.
func SanityCheckManifests(ssms []*SplitSnapshotManifest) error {
	first := ssms[0]
	for _, ssm := range ssms[1:] {
		if ssm.Source.DbName != first.Source.DbName {
			return fmt.Errorf("multirestore sanity check: database names don't match: %v, %v", ssm, first)
		}
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
	rows, err := mysqld.fetchSuperQuery("SHOW PROCESSLIST")
	if err != nil {
		return err
	}
	if len(rows) > 4 {
		return fmt.Errorf("too many active db processes (%v > 4)", len(rows))
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
func (mysqld *Mysqld) CreateSplitSnapshot(dbName, keyName string, startKey, endKey key.HexKeyspaceId, sourceAddr string, allowHierarchicalReplication bool, concurrency int) (snapshotManifestFilename string, err error) {
	if dbName == "" {
		err = fmt.Errorf("no database name provided")
		return
	}
	// same logic applies here
	relog.Info("validateCloneSource")
	if err = mysqld.validateCloneSource(false); err != nil {
		return
	}

	cloneSourcePath := path.Join(mysqld.SnapshotDir, dataDir, dbName+"-"+string(startKey)+","+string(endKey))
	// clean out and start fresh
	for _, _path := range []string{cloneSourcePath} {
		if err = os.RemoveAll(_path); err != nil {
			return
		}
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

	slaveStartRequired, readOnly, replicationPosition, masterAddr, err := mysqld.prepareToSnapshot(allowHierarchicalReplication)
	if err != nil {
		return
	}

	defer func() {
		err = replaceError(err, mysqld.restoreAfterSnapshot(slaveStartRequired, readOnly))
	}()

	var ssmFile string
	dataFiles, snapshotErr := mysqld.createSplitSnapshotManifest(dbName, keyName, startKey, endKey, cloneSourcePath, sd, concurrency)
	if snapshotErr != nil {
		relog.Error("CreateSplitSnapshotManifest failed: %v", snapshotErr)
		return "", snapshotErr
	} else {
		ssm, err := NewSplitSnapshotManifest(sourceAddr, masterAddr,
			dbName, dataFiles, replicationPosition, startKey, endKey, sd)
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
func (mysqld *Mysqld) createSplitSnapshotManifest(dbName, keyName string, startKey, endKey key.HexKeyspaceId, cloneSourcePath string, sd *SchemaDefinition, concurrency int) ([]SnapshotFile, error) {
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

	for i := 0; i < concurrency; i++ {
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

func (mysqld *Mysqld) prepareToSnapshot(allowHierarchicalReplication bool) (slaveStartRequired, readOnly bool, replicationPosition *ReplicationPosition, masterAddr string, err error) {
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
		masterAddr, err = mysqld.Addr()
		if err != nil {
			return
		}
	} else {
		// we are a slave, check our replication strategy
		if allowHierarchicalReplication {
			masterAddr, err = mysqld.Addr()
			if err != nil {
				return
			}
		} else {
			masterAddr, err = mysqld.GetMasterAddr()
			if err != nil {
				return
			}
		}
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
	*bufio.Writer
	Hasher     *hasher
	Filename   string
	gzip       io.Closer
	file       *os.File
	fileBuffer *bufio.Writer
}

func newCompressedNamedHasherWriter(filename string) (*namedHasherWriter, error) {
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

	dstFile, err := os.Create(filename)
	if err != nil {
		return nil, err
	}
	fileBuffer := bufio.NewWriterSize(dstFile, 32*1024)
	hasher := newHasher()
	tee := io.MultiWriter(fileBuffer, hasher)
	// create the gzip compression filter
	gzip, err := cgzip.NewWriterLevel(tee, cgzip.Z_BEST_SPEED)
	if err != nil {
		return nil, err
	}
	gzipBuffer := bufio.NewWriterSize(gzip, 32*1024)
	return &namedHasherWriter{
		Writer:     gzipBuffer,
		Hasher:     hasher,
		Filename:   filename,
		gzip:       gzip,
		file:       dstFile,
		fileBuffer: fileBuffer,
	}, nil
}

func (w namedHasherWriter) Close() (err error) {
	// I have to dismantle the pipeline, starting from the
	// top. Some of the elements are flushers, others are closers,
	// which is why this code is so ugly.
	if err = w.Flush(); err != nil {
		return
	}
	if err = w.gzip.Close(); err != nil {
		return
	}
	if err = w.fileBuffer.Flush(); err != nil {
		return
	}
	if err = w.file.Close(); err != nil {
		return
	}
	return nil
}

// SnapshotFile returns the snapshot file appropriate for the data
// written by the namedHasherWriter. Calling SnapshotFile will close
// the namedHasherWriter.
func (nhw namedHasherWriter) SnapshotFile(snapshotDir string) (*SnapshotFile, error) {
	if err := nhw.Close(); err != nil {
		return nil, err
	}
	fi, err := os.Stat(nhw.Filename)
	if err != nil {
		return nil, err
	}
	relativePath, err := filepath.Rel(snapshotDir, nhw.Filename)
	if err != nil {
		return nil, err
	}
	return &SnapshotFile{relativePath, fi.Size(), nhw.Hasher.HashString()}, nil
}

func (mysqld *Mysqld) dumpTable(td TableDefinition, dbName, keyName, selectIntoOutfile, mainCloneSourcePath string, cloneSourcePaths map[key.KeyRange]string) (map[key.KeyRange]*SnapshotFile, error) {
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
		filename := path.Join(cloneSourcePath, td.Name+".csv.gz")
		w, err := newCompressedNamedHasherWriter(filename)
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

	snapshotFiles := make(map[key.KeyRange]*SnapshotFile)
	for i, hw := range hasherWriters {
		if snapshotFiles[i], err = hw.SnapshotFile(mysqld.SnapshotDir); err != nil {
			return nil, err
		}
	}

	return snapshotFiles, nil
}

func (mysqld *Mysqld) CreateMultiSnapshot(keyRanges []key.KeyRange, dbName, keyName string, sourceAddr string, allowHierarchicalReplication bool, concurrency int, tables []string) (snapshotManifestFilenames []string, err error) {
	if dbName == "" {
		err = fmt.Errorf("no database name provided")
		return
	}

	// same logic applies here
	relog.Info("validateCloneSource")
	if err = mysqld.validateCloneSource(false); err != nil {
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
	sd, fetchErr := mysqld.GetSchema(dbName, tables, false)
	if fetchErr != nil {
		return []string{}, fetchErr
	}

	if len(sd.TableDefinitions) == 0 {
		return []string{}, fmt.Errorf("empty table list for %v", dbName)
	}

	slaveStartRequired, readOnly, replicationPosition, masterAddr, err := mysqld.prepareToSnapshot(allowHierarchicalReplication)
	if err != nil {
		return
	}
	defer func() {
		err = replaceError(err, mysqld.restoreAfterSnapshot(slaveStartRequired, readOnly))
	}()

	selectIntoOutfile := `SELECT {{.KeyspaceIdColumnName}}, {{.Columns}} INTO OUTFILE "{{.TableOutputPath}}" CHARACTER SET binary FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' ESCAPED BY '\\' LINES TERMINATED BY '\n' FROM {{.TableName}}`

	datafiles := make([]map[key.KeyRange]*SnapshotFile, len(sd.TableDefinitions))
	dumpTableWorker := func(i int) (err error) {
		table := sd.TableDefinitions[i]
		snapshotFiles, err := mysqld.dumpTable(table, dbName, keyName, selectIntoOutfile, mainCloneSourcePath, cloneSourcePaths)
		if err != nil {
			return
		}
		datafiles[i] = snapshotFiles
		return nil
	}
	if err = ConcurrentMap(concurrency, len(sd.TableDefinitions), dumpTableWorker); err != nil {
		return
	}

	if e := os.Remove(mainCloneSourcePath); e != nil {
		relog.Error("Cannot remove %v: %v", mainCloneSourcePath, e)
	}

	ssmFiles := make([]string, len(keyRanges))
	for i, kr := range keyRanges {
		krDatafiles := make([]SnapshotFile, len(datafiles))
		for j, m := range datafiles {
			krDatafiles[j] = *m[kr]
		}
		ssm, err := NewSplitSnapshotManifest(sourceAddr, masterAddr, dbName, krDatafiles, replicationPosition, kr.Start.Hex(), kr.End.Hex(), sd)
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

func (lsf localSnapshotFile) queryParams(destinationDbName string) (map[string]string, error) {
	tableName := strings.Replace(path.Base(lsf.filename()), ".csv", "", -1)
	td, ok := lsf.manifest.SchemaDefinition.GetTable(tableName)
	if !ok {
		return nil, fmt.Errorf("no definition for table %v in %v", tableName, lsf.manifest.SchemaDefinition)
	}
	return map[string]string{
		"TableInputPath": lsf.filename(),
		"TableName":      destinationDbName + "." + tableName,
		"Columns":        strings.Join(td.Columns, ", "),
	}, nil

}

func (mysqld *Mysqld) RestoreFromMultiSnapshot(destinationDbName string, keyRange key.KeyRange, sourceAddrs []*url.URL, concurrency, fetchConcurrency, fetchRetryCount int, force bool) (err error) {
	manifests := make([]*SplitSnapshotManifest, len(sourceAddrs))
	err = ConcurrentMap(fetchConcurrency, len(sourceAddrs), func(i int) error {
		dbi := sourceAddrs[i]
		var sourceDbName string
		if len(dbi.Path) < 2 { // "" or "/"
			sourceDbName = destinationDbName
		} else {
			sourceDbName = dbi.Path[1:]
		}
		ssm, e := fetchSnapshotManifestWithRetry("http://"+dbi.Host, sourceDbName, keyRange, fetchRetryCount)
		manifests[i] = ssm
		return e
	})
	if err != nil {
		return
	}

	if err = mysqld.SetReadOnly(true); err != nil {
		return
	}

	if e := SanityCheckManifests(manifests); e != nil {
		if force {
			relog.Error("sanity check failing, continuing nevertheless: %v", e)
		} else {
			return e
		}
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

	// fetch all the csv files
	todo := []localSnapshotFile{}
	for _, manifest := range manifests {
		if err = os.Mkdir(path.Join(tempStoragePath, manifest.Source.Addr), 0775); err != nil {
			return err
		}

		// NOTE(szopa): I am not doing
		// for _, file := range ... { ... }
		// because I want a pointer to file.
		for i := range manifest.Source.Files {
			todo = append(todo, localSnapshotFile{manifest: manifest, file: &manifest.Source.Files[i], basePath: tempStoragePath})
		}
	}

	err = ConcurrentMap(fetchConcurrency, len(todo), func(i int) error {
		lsf := todo[i]
		return fetchFileWithRetry(lsf.url(), lsf.file.Hash, lsf.filename(), fetchRetryCount)
	})
	// FIXME: Should this create the database?
	// RestoreFromPartialSnapshot doesn't do this.
	manifest := manifests[0] // I am assuming they all match
	createDbCmds := []string{
		"USE " + destinationDbName}
	for _, td := range manifest.SchemaDefinition.TableDefinitions {
		createDbCmds = append(createDbCmds, td.Schema)
	}
	if err = mysqld.executeSuperQueryList(createDbCmds); err != nil {
		return
	}

	loadDataInfile := `LOAD DATA INFILE '{{.TableInputPath}}' INTO TABLE {{.TableName}} CHARACTER SET binary FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' ESCAPED BY '\\' LINES TERMINATED BY '\n' ({{.Columns}})`

	err = ConcurrentMap(concurrency, len(todo), func(i int) error {
		lsf := todo[i]
		queryParams, e := lsf.queryParams(destinationDbName)
		if e != nil {
			return e
		}
		query, e := fillStringTemplate(loadDataInfile, queryParams)
		if e != nil {
			return e
		}
		// NOTE(szopa): The binlog has to be disabled,
		// otherwise the whole thing trips on
		// max_binlog_cache_size.
		return mysqld.executeSuperQueryList([]string{"SET sql_log_bin = OFF", query})
		// TODO: Remove files after they were loaded? This
		// could decrease the peak amount of space needed for
		// the restore.
	})
	if err != nil {
		return err
	}

	// TODO: Enable Shruti's branchy replication.

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
