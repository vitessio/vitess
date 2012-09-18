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

/* compress a single file with gzip, leaving the src file intact.
Also computes the md5 hash on the fly.
FIXME(msolomon) not sure how well Go will schedule cpu intensive tasks
might be better if this forked off workers.
*/
func compressFile(srcPath, dstPath string) (*SnapshotFile, error) {
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
	defer dstFile.Close()

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

// This function fetches data from the web server.
// It then sends it to a tee, which on one side has an md5 checksum
// reader, and on the other a gunzip reader writing to a file.
// It will compare the md5 checksum after the copy is done.
func fetchFile(srcUrl, srcHash, dstFilename string) error {
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
	defer dstFile.Close()

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

// FIXME(msolomon) parallelize
// FIXME(msolomon) automatically retry a file transfer at least once
// FIXME(msolomon) deadlines?
func fetchFiles(replicaSource *ReplicaSource, destinationPath string) error {
	for _, fi := range replicaSource.Files {
		filename := fi.getLocalFilename(destinationPath)
		furl := "http://" + replicaSource.Addr + fi.Path
		if err := fetchFile(furl, fi.Hash, filename); err != nil {
			return err
		}
	}
	return nil
}
