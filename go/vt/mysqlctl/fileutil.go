// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package mysqlctl

import (
	"bufio"
	"compress/gzip"
	"crypto/md5"
	"encoding/hex"
	"io"
	"io/ioutil"
	"os"
	"path"
)

/* compress a single file with gzip, leaving the src file intact.
FIXME(msolomon) not sure how well Go will schedule cpu intensive tasks
might be better if this forked off workers.
*/
func compressFile(srcPath, dstPath string) error {
	srcFile, err := os.OpenFile(srcPath, os.O_RDONLY, 0)
	if err != nil {
		return err
	}
	defer srcFile.Close()

	src := bufio.NewReaderSize(srcFile, 2*1024*1024)

	dir, filePrefix := path.Split(dstPath)

	dstFile, err := ioutil.TempFile(dir, filePrefix)
	if err != nil {
		return err
	}
	defer dstFile.Close()

	dst := bufio.NewWriterSize(dstFile, 2*1024*1024)

	compressor := gzip.NewWriter(dst)
	defer compressor.Close()

	_, err = io.Copy(compressor, src)
	if err != nil {
		return err
	}

	// close dst manually to flush all buffers to disk
	compressor.Close()
	dst.Flush()
	dstFile.Close()
	// atomically move completed compressed file
	return os.Rename(dstFile.Name(), dstPath)
}

/* uncompress a single file with gzip, leaving the src file intact.
FIXME(msolomon) not sure how well Go will schedule cpu intensive tasks
might be better if this forked off workers.
*/
func uncompressFile(srcPath, dstPath string) error {
	srcFile, err := os.OpenFile(srcPath, os.O_RDONLY, 0)
	if err != nil {
		return err
	}
	defer srcFile.Close()

	src := bufio.NewReaderSize(srcFile, 2*1024*1024)
	if err != nil {
		return err
	}

	decompressor, err := gzip.NewReader(src)
	if err != nil {
		return err
	}
	defer decompressor.Close()

	dir, filePrefix := path.Split(dstPath)

	dstFile, err := ioutil.TempFile(dir, filePrefix)
	if err != nil {
		return err
	}
	defer dstFile.Close()

	dst := bufio.NewWriterSize(dstFile, 2*1024*1024)

	_, err = io.Copy(dst, decompressor)
	if err != nil {
		return err
	}

	// close dst manually to flush all buffers to disk
	dst.Flush()
	dstFile.Close()
	// atomically move uncompressed file
	if err := os.Chmod(dstFile.Name(), 0664); err != nil {
		return err
	}
	return os.Rename(dstFile.Name(), dstPath)
}

func md5File(filename string) (string, error) {
	file, err := os.OpenFile(filename, os.O_RDONLY, 0)
	if err != nil {
		return "", err
	}
	defer file.Close()

	src := bufio.NewReaderSize(file, 2*1024*1024)
	hasher := md5.New()
	_, err = io.Copy(hasher, src)
	if err != nil {
		return "", err
	}

	return hex.EncodeToString(hasher.Sum(nil)), nil
}
