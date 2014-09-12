// Copyright 2013, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tabletmanager

// This file handles the http server for snapshots, clones, ...

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/cgzip"
	vtenv "github.com/youtube/vitess/go/vt/env"
	"github.com/youtube/vitess/go/vt/mysqlctl"
)

// HttpHandleSnapshots handles the serving of files from the local tablet
func HttpHandleSnapshots(mycnf *mysqlctl.Mycnf, uid uint32) {
	// make a list of paths we can serve HTTP traffic from.
	// we don't resolve them here to real paths, as they might not exits yet
	snapshotDir := mysqlctl.SnapshotDir(uid)
	allowedPaths := []string{
		path.Join(vtenv.VtDataRoot(), "data"),
		mysqlctl.TabletDir(uid),
		mysqlctl.SnapshotDir(uid),
		mycnf.DataDir,
		mycnf.InnodbDataHomeDir,
		mycnf.InnodbLogGroupHomeDir,
	}

	// NOTE: trailing slash in pattern means we handle all paths with this prefix
	http.Handle(mysqlctl.SnapshotURLPath+"/", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handleSnapshot(w, r, snapshotDir, allowedPaths)
	}))

}

// serve an individual query
func handleSnapshot(rw http.ResponseWriter, req *http.Request, snapshotDir string, allowedPaths []string) {
	// if we get any error, we'll try to write a server error
	// (it will fail if the header has already been written, but at least
	// we won't crash vttablet)
	defer func() {
		if x := recover(); x != nil {
			log.Errorf("vttablet http server panic: %v", x)
			http.Error(rw, fmt.Sprintf("500 internal server error: %v", x), http.StatusInternalServerError)
		}
	}()

	// /snapshot must be rewritten to the actual location of the snapshot.
	relative, err := filepath.Rel(mysqlctl.SnapshotURLPath, req.URL.Path)
	if err != nil {
		log.Errorf("bad snapshot relative path %v %v", req.URL.Path, err)
		http.Error(rw, "400 bad request", http.StatusBadRequest)
		return
	}

	// Make sure that realPath is absolute and resolve any escaping from
	// snapshotDir through a symlink.
	realPath, err := filepath.Abs(path.Join(snapshotDir, relative))
	if err != nil {
		log.Errorf("bad snapshot absolute path %v %v", req.URL.Path, err)
		http.Error(rw, "400 bad request", http.StatusBadRequest)
		return
	}

	realPath, err = filepath.EvalSymlinks(realPath)
	if err != nil {
		log.Errorf("bad snapshot symlink eval %v %v", req.URL.Path, err)
		http.Error(rw, "400 bad request", http.StatusBadRequest)
		return
	}

	// Resolve all the possible roots and make sure we're serving
	// from one of them
	for _, allowedPath := range allowedPaths {
		// eval the symlinks of the allowed path
		allowedPath, err := filepath.EvalSymlinks(allowedPath)
		if err != nil {
			continue
		}
		if strings.HasPrefix(realPath, allowedPath) {
			sendFile(rw, req, realPath)
			return
		}
	}

	log.Errorf("bad snapshot real path %v %v", req.URL.Path, realPath)
	http.Error(rw, "400 bad request", http.StatusBadRequest)
}

// custom function to serve files
func sendFile(rw http.ResponseWriter, req *http.Request, path string) {
	log.Infof("serve %v %v", req.URL.Path, path)
	file, err := os.Open(path)
	if err != nil {
		http.NotFound(rw, req)
		return
	}
	defer file.Close()

	fileinfo, err := file.Stat()
	if err != nil {
		http.NotFound(rw, req)
		return
	}

	// for directories, or for files smaller than 1k, use library
	if fileinfo.Mode().IsDir() || fileinfo.Size() < 1024 {
		http.ServeFile(rw, req, path)
		return
	}

	// supports If-Modified-Since header
	if t, err := time.Parse(http.TimeFormat, req.Header.Get("If-Modified-Since")); err == nil && fileinfo.ModTime().Before(t.Add(1*time.Second)) {
		rw.WriteHeader(http.StatusNotModified)
		return
	}

	// support Accept-Encoding header
	var writer io.Writer = rw
	var reader io.Reader = file
	if !strings.HasSuffix(path, ".gz") {
		ae := req.Header.Get("Accept-Encoding")

		if strings.Contains(ae, "gzip") {
			gz, err := cgzip.NewWriterLevel(rw, cgzip.Z_BEST_SPEED)
			if err != nil {
				http.Error(rw, err.Error(), http.StatusInternalServerError)
				return
			}
			rw.Header().Set("Content-Encoding", "gzip")
			defer gz.Close()
			writer = gz
		}
	}

	// add content-length if we know it
	if writer == rw && reader == file {
		rw.Header().Set("Content-Length", fmt.Sprintf("%v", fileinfo.Size()))
	}

	// and just copy content out
	rw.Header().Set("Last-Modified", fileinfo.ModTime().UTC().Format(http.TimeFormat))
	rw.WriteHeader(http.StatusOK)
	if _, err := io.Copy(writer, reader); err != nil {
		log.Warningf("transfer failed %v: %v", path, err)
	}
}
