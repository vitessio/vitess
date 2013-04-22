// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package mysqlctl

import (
	"bufio"
	//	"crypto/md5"
	"encoding/hex"
	"fmt"
	"hash"
	//	"hash/crc64"
	"encoding/json"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"sync"

	"code.google.com/p/vitess/go/cgzip"
	"code.google.com/p/vitess/go/relog"
	"code.google.com/p/vitess/go/vt/key"
)

// Use this to simulate failures in tests
var (
	simulateFailures = false
	failureCounter   = 0
)

func init() {
	_, statErr := os.Stat("/tmp/vtSimulateFetchFailures")
	simulateFailures = statErr == nil
}

// our hasher, implemented using md5
// type hasher struct {
// 	hash.Hash
// }

// func newHasher() *hasher {
// 	return &hasher{md5.New()}
// }

// func (h *hasher) HashString() string {
// 	return hex.EncodeToString(h.Sum(nil))
// }

// our hasher, implemented using crc64
//type hasher struct {
//	hash.Hash64
//}

//func newHasher() *hasher {
//	return &hasher{crc64.New(crc64.MakeTable(crc64.ECMA))}
//}

//func (h *hasher) HashString() string {
//	return hex.EncodeToString(h.Sum(nil))
//}

// our hasher, implemented using cgzip crc32
type hasher struct {
	hash.Hash32
}

func newHasher() *hasher {
	return &hasher{cgzip.NewCrc32()}
}

func (h *hasher) HashString() string {
	return hex.EncodeToString(h.Sum(nil))
}

// SnapshotFile describes a file to serve.
// 'Path' is the path component of the URL. SnapshotManifest.Addr is
// the host+port component of the URL.
// If path ends in '.gz', it is compressed.
// Size and Hash are computed on the Path itself
// if TableName is set, this file belongs to that table
type SnapshotFile struct {
	Path      string
	Size      int64
	Hash      string
	TableName string
}

type SnapshotFiles []SnapshotFile

// sort.Interface
// we sort by descending file size
func (s SnapshotFiles) Len() int           { return len(s) }
func (s SnapshotFiles) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s SnapshotFiles) Less(i, j int) bool { return s[i].Size > s[j].Size }

// This function returns the local file used to store the SnapshotFile,
// relative to the basePath.
// for instance, if the source path is something like:
// /vt/snapshot/vt_0000062344/data/vt_snapshot_test-MA,Mw/vt_insert_test.csv.gz
// we will get everything starting with 'data/...', append it to basepath,
// and remove the .gz extension. So with basePath=myPath, it will return:
// myPath/data/vt_snapshot_test-MA,Mw/vt_insert_test.csv
func (dataFile *SnapshotFile) getLocalFilename(basePath string) string {
	filename := path.Join(basePath, dataFile.Path)
	// trim compression extension if present
	if strings.HasSuffix(filename, ".gz") {
		filename = filename[:len(filename)-3]
	}
	return filename
}

// newSnapshotFile behavior depends on the compress flag:
// - if compress is true , it compresses a single file with gzip, and
// computes the hash on the compressed version.
// - if compress is false, just symlinks and computes the hash on the file
// The source file is always left intact.
// The path of the returned SnapshotFile will be relative
// to root.
func newSnapshotFile(srcPath, dstPath, root string, compress bool) (*SnapshotFile, error) {
	// open the source file
	srcFile, err := os.OpenFile(srcPath, os.O_RDONLY, 0)
	if err != nil {
		return nil, err
	}
	defer srcFile.Close()
	src := bufio.NewReaderSize(srcFile, 2*1024*1024)

	var hash string
	var size int64
	if compress {
		relog.Info("newSnapshotFile: starting to compress %v into %v", srcPath, dstPath)

		// open the temporary destination file
		dir, filePrefix := path.Split(dstPath)
		dstFile, err := ioutil.TempFile(dir, filePrefix)
		if err != nil {
			return nil, err
		}
		defer func() {
			// try to close and delete the file.  in the
			// success case, the file will already be
			// closed and renamed, so all of this would
			// fail anyway, no biggie
			dstFile.Close()
			os.Remove(dstFile.Name())
		}()
		dst := bufio.NewWriterSize(dstFile, 2*1024*1024)

		// create the hasher and the tee on top
		hasher := newHasher()
		tee := io.MultiWriter(dst, hasher)

		// create the gzip compression filter
		gzip, err := cgzip.NewWriterLevel(tee, cgzip.Z_BEST_SPEED)
		if err != nil {
			return nil, err
		}

		// copy from the file to gzip to tee to output file and hasher
		_, err = io.Copy(gzip, src)
		if err != nil {
			return nil, err
		}

		// close gzip to flush it
		if err = gzip.Close(); err != nil {
			return nil, err
		}

		// close dst manually to flush all buffers to disk
		dst.Flush()
		dstFile.Close()
		hash = hasher.HashString()

		// atomically move completed compressed file
		err = os.Rename(dstFile.Name(), dstPath)
		if err != nil {
			return nil, err
		}

		// and get its size
		fi, err := os.Stat(dstPath)
		if err != nil {
			return nil, err
		}
		size = fi.Size()
	} else {
		relog.Info("newSnapshotFile: starting to hash and symlinking %v to %v", srcPath, dstPath)

		// get the hash
		hasher := newHasher()
		_, err = io.Copy(hasher, src)
		if err != nil {
			return nil, err
		}
		hash = hasher.HashString()

		// do the symlink
		err = os.Symlink(srcPath, dstPath)
		if err != nil {
			return nil, err
		}

		// and get the size
		fi, err := os.Stat(srcPath)
		if err != nil {
			return nil, err
		}
		size = fi.Size()
	}

	relog.Info("clone data ready %v:%v", dstPath, hash)
	relativeDst, err := filepath.Rel(root, dstPath)
	if err != nil {
		return nil, err
	}
	return &SnapshotFile{relativeDst, size, hash, ""}, nil
}

// newSnapshotFiles processes multiple files in parallel. The Paths of
// the returned SnapshotFiles will be relative to root.
// - if compress is true, we compress the files and compute the hash on
// the compressed version.
// - if compress is false, we symlink the files, and compute the hash on
// the original version.
func newSnapshotFiles(sources, destinations []string, root string, concurrency int, compress bool) ([]SnapshotFile, error) {
	if len(sources) != len(destinations) || len(sources) == 0 {
		return nil, fmt.Errorf("programming error: bad array lengths: %v %v", len(sources), len(destinations))
	}

	workQueue := make(chan int, len(sources))
	for i := 0; i < len(sources); i++ {
		workQueue <- i
	}
	close(workQueue)

	snapshotFiles := make([]SnapshotFile, len(sources))
	resultQueue := make(chan error, len(sources))
	for i := 0; i < concurrency; i++ {
		go func() {
			for i := range workQueue {
				sf, err := newSnapshotFile(sources[i], destinations[i], root, compress)
				if err == nil {
					snapshotFiles[i] = *sf
				}
				resultQueue <- err
			}
		}()
	}

	var err error
	for i := 0; i < len(sources); i++ {
		if compressErr := <-resultQueue; compressErr != nil {
			err = compressErr
		}
	}

	// clean up files if we had an error
	// FIXME(alainjobart) it seems extreme to delete all files if
	// the last one failed. Since we only move the file into
	// its destination when it worked, we could assume if the file
	// already exists it's good, and re-compute its hash.
	if err != nil {
		relog.Info("Error happened, deleting all the files we already compressed")
		for _, dest := range destinations {
			os.Remove(dest)
		}
		return nil, err
	}

	return snapshotFiles, nil
}

// a SnapshotManifest describes multiple SnapshotFiles and where
// to get them from.
type SnapshotManifest struct {
	Addr string // this is the address of the tabletserver, not mysql

	DbName string
	Files  SnapshotFiles

	ReplicationState *ReplicationState
	MasterState      *ReplicationState
}

func newSnapshotManifest(addr, mysqlAddr, masterAddr, dbName string, files []SnapshotFile, pos, masterPos *ReplicationPosition) (*SnapshotManifest, error) {
	nrs, err := NewReplicationState(masterAddr)
	if err != nil {
		return nil, err
	}
	mrs, err := NewReplicationState(mysqlAddr)
	if err != nil {
		return nil, err
	}
	rs := &SnapshotManifest{
		Addr:             addr,
		DbName:           dbName,
		Files:            files,
		ReplicationState: nrs,
		MasterState:      mrs,
	}
	sort.Sort(rs.Files)
	rs.ReplicationState.ReplicationPosition = *pos
	if masterPos != nil {
		rs.MasterState.ReplicationPosition = *masterPos
	}
	return rs, nil
}

func fetchSnapshotManifestWithRetry(addr, dbName string, keyRange key.KeyRange, retryCount int) (ssm *SplitSnapshotManifest, err error) {
	for i := 0; i < retryCount; i++ {
		if ssm, err = fetchSnapshotManifest(addr, dbName, keyRange); err == nil {
			return
		}
	}
	return
}

// fetchSnapshotManifest fetches the manifest for keyRange from
// vttablet serving at addr.
func fetchSnapshotManifest(addr, dbName string, keyRange key.KeyRange) (*SplitSnapshotManifest, error) {
	shardName := fmt.Sprintf("%v-%v,%v", dbName, keyRange.Start.Hex(), keyRange.End.Hex())
	path := path.Join(SnapshotURLPath, "data", shardName, partialSnapshotManifestFile)
	url := addr + path
	resp, err := http.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	data, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	if sc := resp.StatusCode; sc != 200 {
		return nil, fmt.Errorf("GET %v returned with a non-200 status code (%v): %q", url, sc, data)
	}

	ssm := new(SplitSnapshotManifest)
	if err = json.Unmarshal(data, ssm); err != nil {
		return nil, fmt.Errorf("ReadSplitSnapshotManifest failed: %v %v", url, err)
	}
	return ssm, nil
}

// fetchFile fetches data from the web server.  It then sends it to a
// tee, which on one side has an hash checksum reader, and on the other
// a gunzip reader writing to a file.  It will compare the hash
// checksum after the copy is done.
func fetchFile(srcUrl, srcHash, dstFilename string) error {
	relog.Info("fetchFile: starting to fetch %v from %v", dstFilename, srcUrl)

	// create destination directory
	dir, _ := path.Split(dstFilename)
	if dirErr := os.MkdirAll(dir, 0775); dirErr != nil {
		return dirErr
	}

	// open the URL
	req, err := http.NewRequest("GET", srcUrl, nil)
	if err != nil {
		return fmt.Errorf("NewRequest failed for %v: %v", srcUrl, err)
	}
	// we set the 'gzip' encoding ourselves so the library doesn't
	// do it for us and ends up using go gzip (we want to use our own
	// cgzip which is much faster)
	req.Header.Set("Accept-Encoding", "gzip")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	if resp.StatusCode != 200 {
		return fmt.Errorf("failed fetching %v: %v", srcUrl, resp.Status)
	}
	defer resp.Body.Close()

	// see if we need some uncompression
	var reader io.Reader = resp.Body
	ce := resp.Header.Get("Content-Encoding")
	if ce != "" {
		if ce == "gzip" {
			gz, err := cgzip.NewReader(reader)
			if err != nil {
				return err
			}
			defer gz.Close()
			reader = gz
		} else {
			return fmt.Errorf("unsupported Content-Encoding: %v", ce)
		}
	}

	// create a temporary file to uncompress to
	dir, filePrefix := path.Split(dstFilename)
	dstFile, err := ioutil.TempFile(dir, filePrefix)
	if err != nil {
		return err
	}
	defer func() {
		// try to close and delete the file.
		// in the success case, the file will already be closed
		// and renamed, so all of this would fail anyway, no biggie
		dstFile.Close()
		os.Remove(dstFile.Name())
	}()

	// create a buffering output
	dst := bufio.NewWriterSize(dstFile, 2*1024*1024)

	// create hash to write the compressed data to
	hasher := newHasher()

	// create a Tee: we split the HTTP input into the hasher
	// and into the gunziper
	tee := io.TeeReader(reader, hasher)

	// create the uncompresser
	var decompressor io.Reader
	if strings.HasSuffix(srcUrl, ".gz") {
		gz, err := cgzip.NewReader(tee)
		if err != nil {
			return err
		}
		defer gz.Close()
		decompressor = gz
	} else {
		decompressor = tee
	}

	// see if we need to introduce failures
	if simulateFailures {
		failureCounter++
		if failureCounter%10 == 0 {
			return fmt.Errorf("Simulated error")
		}
	}

	// copy the data. Will also write to the hasher
	if _, err = io.Copy(dst, decompressor); err != nil {
		return err
	}

	// check the hash
	hash := hasher.HashString()
	if srcHash != hash {
		return fmt.Errorf("hash mismatch for %v, %v != %v", dstFilename, srcHash, hash)
	}

	// we're good
	relog.Info("fetched snapshot file: %v", dstFilename)
	dst.Flush()
	dstFile.Close()

	// atomically move uncompressed file
	if err := os.Chmod(dstFile.Name(), 0664); err != nil {
		return err
	}
	return os.Rename(dstFile.Name(), dstFilename)
}

// fetchFileWithRetry fetches data from the web server, retrying a few
// times.
func fetchFileWithRetry(srcUrl, srcHash, dstFilename string, fetchRetryCount int) (err error) {
	for i := 0; i < fetchRetryCount; i++ {
		err = fetchFile(srcUrl, srcHash, dstFilename)
		if err == nil {
			return nil
		}
		relog.Warning("fetching snapshot file %v failed (try=%v): %v", dstFilename, i, err)
	}

	relog.Error("fetching snapshot file %v failed too many times", dstFilename)
	return err
}

// FIXME(msolomon) Should we add deadlines? What really matters more
// than a deadline is probably a sense of progress, more like a
// "progress timeout" - how long will we wait if there is no change in
// received bytes.
// FIXME(alainjobart) support fetching files in chunks: create a new
// struct fileChunk {
//    snapshotFile  *SnapshotFile
//    relatedChunks []*fileChunk
//    start,end     uint64
//    observedCrc32 uint32
// }
// Create a slice of fileChunk objects, populate it:
// For files smaller than <threshold>, create one fileChunk
// For files bigger than <threshold>, create N fileChunks
//   (the first one has the list of all the others)
// Fetch them all:
//   - change the workqueue to have indexes on the fileChunk slice
//   - compute the crc32 while fetching, but don't compare right away
// Collect results the same way, write observedCrc32 in the fileChunk
// For each fileChunk, compare checksum:
//   - if single file, compare snapshotFile.hash with observedCrc32
//   - if multiple chunks and first chunk, merge observedCrc32, and compare
func fetchFiles(snapshotManifest *SnapshotManifest, destinationPath string, fetchConcurrency, fetchRetryCount int) (err error) {
	// create a workQueue, a resultQueue, and the go routines
	// to process entries out of workQueue into resultQueue
	// the mutex protects the error response
	workQueue := make(chan SnapshotFile, len(snapshotManifest.Files))
	resultQueue := make(chan error, len(snapshotManifest.Files))
	mutex := sync.Mutex{}
	for i := 0; i < fetchConcurrency; i++ {
		go func() {
			for sf := range workQueue {
				// if someone else errored out, we skip our job
				mutex.Lock()
				previousError := err
				mutex.Unlock()
				if previousError != nil {
					resultQueue <- previousError
					continue
				}

				// do our fetch, save the error
				filename := sf.getLocalFilename(destinationPath)
				furl := "http://" + snapshotManifest.Addr + path.Join(SnapshotURLPath, sf.Path)
				fetchErr := fetchFileWithRetry(furl, sf.Hash, filename, fetchRetryCount)
				if fetchErr != nil {
					mutex.Lock()
					err = fetchErr
					mutex.Unlock()
				}
				resultQueue <- fetchErr
			}
		}()
	}

	// add the jobs (writing on the channel will block if the queue
	// is full, no big deal)
	jobCount := 0
	for _, fi := range snapshotManifest.Files {
		workQueue <- fi
		jobCount++
	}
	close(workQueue)

	// read the responses (we guarantee one response per job)
	for i := 0; i < jobCount; i++ {
		<-resultQueue
	}

	// clean up files if we had an error
	// FIXME(alainjobart) it seems extreme to delete all files if
	// the last one failed. Maybe we shouldn't, and if a file already
	// exists, we hash it before retransmitting.
	if err != nil {
		relog.Info("Error happened, deleting all the files we already got")
		for _, fi := range snapshotManifest.Files {
			filename := fi.getLocalFilename(destinationPath)
			os.Remove(filename)
		}
	}

	return err
}
