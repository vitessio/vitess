// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package mysqlctl

import (
	"bufio"
	"code.google.com/p/vitess/go/relog"
	"compress/gzip"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"path"
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

// compressFile compresses a single file with gzip, leaving the src
// file intact.  Also computes the md5 hash on the fly.
func compressFile(srcPath, dstPath string) (*SnapshotFile, error) {
	// FIXME(msolomon) not sure how well Go will schedule cpu intensive tasks
	// might be better if this forked off workers.
	relog.Info("compressFile: starting to compress %v into %v", srcPath, dstPath)
	srcFile, err := os.OpenFile(srcPath, os.O_RDONLY, 0)
	if err != nil {
		return nil, err
	}
	defer srcFile.Close()

	src := bufio.NewReaderSize(srcFile, 2*1024*1024)

	dir, filePrefix := path.Split(dstPath)

	dstFile, err := ioutil.TempFile(dir, filePrefix)
	if err != nil {
		return nil, err
	}
	defer func() {
		// try to close and delete the file.
		// in the success case, the file will already be closed
		// and renamed, so all of this would fail anyway, no biggie
		dstFile.Close()
		os.Remove(dstFile.Name())
	}()

	dst := bufio.NewWriterSize(dstFile, 2*1024*1024)

	hasher := md5.New()
	tee := io.MultiWriter(dst, hasher)

	compressor := gzip.NewWriter(tee)
	defer compressor.Close()

	_, err = io.Copy(compressor, src)
	if err != nil {
		return nil, err
	}

	// close dst manually to flush all buffers to disk
	compressor.Close()
	dst.Flush()
	dstFile.Close()
	hash := hex.EncodeToString(hasher.Sum(nil))

	// atomically move completed compressed file
	err = os.Rename(dstFile.Name(), dstPath)
	if err != nil {
		return nil, err
	}

	relog.Info("clone data ready %v:%v", dstPath, hash)
	return &SnapshotFile{dstPath, hash}, nil
}

// compressFile compresses multiple files in parallel.
func compressFiles(sources, destinations []string, compressConcurrency int) ([]SnapshotFile, error) {
	if len(sources) != len(destinations) || len(sources) == 0 {
		panic(fmt.Errorf("bad array lengths: %v %v", len(sources), len(destinations)))
	}

	workQueue := make(chan int, len(sources))
	for i := 0; i < len(sources); i++ {
		workQueue <- i
	}
	close(workQueue)

	snapshotFiles := make([]SnapshotFile, len(sources))
	resultQueue := make(chan error, len(sources))
	for i := 0; i < compressConcurrency; i++ {
		go func() {
			for i := range workQueue {
				sf, err := compressFile(sources[i], destinations[i])
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
	// already exists it's good, and re-compute its md5.
	if err != nil {
		relog.Info("Error happened, deleting all the files we already compressed")
		for _, dest := range destinations {
			os.Remove(dest)
		}
		return nil, err
	}

	return snapshotFiles, nil
}

// fetchFile fetches data from the web server.  It then sends it to a
// tee, which on one side has an md5 checksum reader, and on the other
// a gunzip reader writing to a file.  It will compare the md5
// checksum after the copy is done.
func fetchFile(srcUrl, srcHash, dstFilename string) error {
	relog.Info("fetchFile: starting to fetch %v", dstFilename)

	// create destination directory
	dir, _ := path.Split(dstFilename)
	if dirErr := os.MkdirAll(dir, 0775); dirErr != nil {
		return dirErr
	}

	// open the URL
	resp, err := http.Get(srcUrl)
	if resp.StatusCode != 200 {
		return fmt.Errorf("failed fetching %v: %v", srcUrl, resp.Status)
	}
	defer resp.Body.Close()

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

	// create md5 hash to write the compressed data to
	hasher := md5.New()

	// create a Tee: we split the HTTP input into the md5 hasher
	// and into the gunziper
	tee := io.TeeReader(resp.Body, hasher)

	// create the uncompresser
	decompressor, err := gzip.NewReader(tee)
	if err != nil {
		return err
	}
	defer decompressor.Close()

	// see if we need to introduce failures
	if simulateFailures {
		failureCounter++
		if failureCounter%3 == 0 {
			return fmt.Errorf("Simulated error")
		}
	}

	// copy the data. Will also write to the hasher
	if _, err = io.Copy(dst, decompressor); err != nil {
		return err
	}

	// check the md5
	hash := hex.EncodeToString(hasher.Sum(nil))
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
func fetchFiles(snapshotManifest *SnapshotManifest, destinationPath string, fetchConcurrency, fetchRetryCount int) (err error) {
	workQueue := make(chan SnapshotFile, len(snapshotManifest.Files))
	for _, fi := range snapshotManifest.Files {
		workQueue <- fi
	}
	close(workQueue)

	resultQueue := make(chan error, len(snapshotManifest.Files))
	for i := 0; i < fetchConcurrency; i++ {
		go func() {
			for fi := range workQueue {
				filename := fi.getLocalFilename(destinationPath)
				furl := "http://" + snapshotManifest.Addr + fi.Path
				resultQueue <- fetchFileWithRetry(furl, fi.Hash, filename, fetchRetryCount)
			}
		}()
	}

	for i := 0; i < len(snapshotManifest.Files); i++ {
		if fetchErr := <-resultQueue; fetchErr != nil {
			err = fetchErr
		}
	}

	// clean up files if we had an error
	// FIXME(alainjobart) it seems extreme to delete all files if
	// the last one failed. Maybe we shouldn't, and if a file already
	// exists, we md5 it before retransmitting.
	if err != nil {
		relog.Info("Error happened, deleting all the files we already got")
		for _, fi := range snapshotManifest.Files {
			filename := fi.getLocalFilename(destinationPath)
			os.Remove(filename)
		}
	}

	return err
}
