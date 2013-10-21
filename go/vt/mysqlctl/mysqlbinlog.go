// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package mysqlctl

import (
	"bytes"
	"fmt"
	"io"
	"os/exec"
	"path"

	log "github.com/golang/glog"
	vtenv "github.com/youtube/vitess/go/vt/env"
)

type MysqlBinlog struct {
	cmd *exec.Cmd
}

// MysqlBinlog launches mysqlbinlog and returns a ReadCloser into which its output
// will be piped. The stderr will be redirected to the log.
func (mbl *MysqlBinlog) Launch(dbname, filename string, pos int64) (stdout io.ReadCloser, err error) {
	dir, err := vtenv.VtMysqlRoot()
	if err != nil {
		return nil, err
	}
	mbl.cmd = exec.Command(
		path.Join(dir, "bin/mysqlbinlog"),
		fmt.Sprintf("--database=%s", dbname),
		fmt.Sprintf("--start-position=%d", pos),
		filename,
	)
	stdout, err = mbl.cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}
	mbl.cmd.Stderr = &logWrapper{}
	// TODO(sougou): redirect stderr to log
	err = mbl.cmd.Start()
	if err != nil {
		stdout.Close()
		return nil, err
	}
	return stdout, nil
}

// Kill terminates the current mysqlbinlog process.
func (mbl *MysqlBinlog) Kill() {
	mbl.cmd.Process.Kill()
	mbl.cmd.Wait()
}

// Wait waits for the mysqlbinlog process to terminate
// and returns an error if there was any.
func (mbl *MysqlBinlog) Wait() error {
	return mbl.cmd.Wait()
}

type logWrapper struct {
}

func (lwp *logWrapper) Write(p []byte) (n int, err error) {
	if bytes.HasPrefix(p, []byte("WARNING")) {
		log.Warningf("%s", p)
	} else {
		log.Errorf("%s", p)
	}
	return len(p), nil
}
