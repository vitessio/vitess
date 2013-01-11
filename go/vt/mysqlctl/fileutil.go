// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package mysqlctl

import (
	"bufio"
	"compress/gzip"
	//	"crypto/md5"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"hash"
	"hash/crc64"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"strings"

	"code.google.com/p/vitess/go/relog"
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
type hasher struct {
	hash.Hash64
}

func newHasher() *hasher {
	return &hasher{crc64.New(crc64.MakeTable(crc64.ECMA))}
}

func (h *hasher) HashString() string {
	sum := h.Sum64()
	buf := make([]byte, 10)
	size := binary.PutUvarint(buf, sum)
	return hex.EncodeToString(buf[0:size])
}

// newSnapshotFile behavior depends on the compress flag:
// - if compress is true , it compresses a single file with gzip, and
// computes the hash on the compressed version.
// - if compress is false, just symlinks and computes the hash on the file
// The source file is always left intact.
// The path of the returned SnapshotFile will be relative
// to root.
func newSnapshotFile(srcPath, dstPath, root string, compress bool) (*SnapshotFile, error) {
	relog.Info("newSnapshotFile: starting to compress %v into %v", srcPath, dstPath)
	var hash string
	var size int64
	if compress {
		relog.Info("newSnapshotFile: starting to compress %v into %v", srcPath, dstPath)

		// forking gzip
		cmd := exec.Command("gzip", "--fast", "-c", srcPath)
		gzipStdout, err := cmd.StdoutPipe()
		if err != nil {
			return nil, err
		}
		if err = cmd.Start(); err != nil {
			return nil, err
		}
		defer func() {
			cmd.Wait()
		}()

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

		// copy from gzip's output to tee to output file and hasher
		size, err = io.Copy(tee, gzipStdout)
		if err != nil {
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
	} else {
		relog.Info("newSnapshotFile: starting to hash and symlinking %v to %v", srcPath, dstPath)

		// open the source file
		srcFile, err := os.OpenFile(srcPath, os.O_RDONLY, 0)
		if err != nil {
			return nil, err
		}
		defer srcFile.Close()
		src := bufio.NewReaderSize(srcFile, 2*1024*1024)

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
	return &SnapshotFile{relativeDst, size, hash}, nil
}

// newSnapshotFiles processes multiple files in parallel. The Paths of
// the returned SnapshotFiles will be relative to root.
// - if compress is true, we compress the files and compute the hash on
// the compressed version.
// - if compress is false, we symlink the files, and compute the hash on
// the original version.
func newSnapshotFiles(sources, destinations []string, root string, concurrency int, compress bool) ([]SnapshotFile, error) {
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

// fetchFile fetches data from the web server.  It then sends it to a
// tee, which on one side has an hash checksum reader, and on the other
// a gunzip reader writing to a file.  It will compare the hash
// checksum after the copy is done.
func fetchFile(srcUrl, srcHash, dstFilename, encoding string) error {
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
	if encoding != "" {
		req.Header.Set("Accept-Encoding", encoding)
	}
	resp, err := http.DefaultClient.Do(req)
	if resp.StatusCode != 200 {
		return fmt.Errorf("failed fetching %v: %v", srcUrl, resp.Status)
	}
	defer resp.Body.Close()

	// see if we need some uncompression
	var reader io.Reader = resp.Body
	ce := resp.Header.Get("Content-Encoding")
	if ce != "" {
		if ce == "gzip" {
			reader, err = gzip.NewReader(reader)
			if err != nil {
				return err
			}

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
		gz, err := gzip.NewReader(tee)
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
func fetchFileWithRetry(srcUrl, srcHash, dstFilename string, fetchRetryCount int, encoding string) (err error) {
	for i := 0; i < fetchRetryCount; i++ {
		err = fetchFile(srcUrl, srcHash, dstFilename, encoding)
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
func fetchFiles(snapshotManifest *SnapshotManifest, destinationPath string, fetchConcurrency, fetchRetryCount int, encoding string) (err error) {
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
				furl := "http://" + snapshotManifest.Addr + path.Join(SnapshotURLPath, fi.Path)
				resultQueue <- fetchFileWithRetry(furl, fi.Hash, filename, fetchRetryCount, encoding)
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
