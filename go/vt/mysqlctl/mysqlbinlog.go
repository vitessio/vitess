// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package mysqlctl

import (
	"fmt"
	"io"
	"os"
	"path"

	"code.google.com/p/vitess/go/relog"
	vtenv "code.google.com/p/vitess/go/vt/env"
)

/*
 -d, --database=name List entries for just this database (local log only).
  -D, --disable-log-bin
                      Disable binary log. This is useful, if you enabled
                      --to-last-log and are sending the output to the same
                      MySQL server. This way you could avoid an endless loop.
                      You would also like to use it when restoring after a
                      crash to avoid duplication of the statements you already
 have. NOTE: you will need a SUPER privilege to use this
                      option.
  -F, --force-if-open Force if binlog was not closed properly.
  -f, --force-read    Force reading unknown binlog events.
 -h, --host=name     Get the binlog from server.
 -l, --local-load=name
                      Prepare local temporary files for LOAD DATA INFILE in the
                      specified directory.
 -o, --offset=#      Skip the first N entries.
 -p, --password[=name]
                      Password to connect to remote server.
 -P, --port=#        Port number to use for connection or 0 for default to, in
                      order of preference, my.cnf, $MYSQL_TCP_PORT,
 /etc/services, built-in default (3306).
 --protocol=name     The protocol to use for connection (tcp, socket, pipe,
 memory).
  -R, --read-from-remote-server
                      Read binary logs from a MySQL server.
 -r, --result-file=name
                      Direct output to a given file.
 --server-id=#       Extract only binlog entries created by the server having
                      the given id.
 --set-charset=name  Add 'SET NAMES character_set' to the output.
 -S, --socket=name   The socket file to use for connection.
 -j, --start-position=#
                      Start reading the binlog at position N. Applies to the
                      first binlog passed on the command line.
 --stop-position=#   Stop reading the binlog at position N. Applies to the
                      last binlog passed on the command line.
  -t, --to-last-log   Requires -R. Will not stop at the end of the requested
                      binlog but rather continue printing until the end of the
                      last binlog of the MySQL server. If you send the output
                      to the same MySQL server, that may lead to an endless
                      loop.
 -u, --user=name     Connect to the remote server as username.

*/

type BinlogDecoder struct {
	process *os.Process
}

// findVtMysqlbinlogDir finds the directory that contains vt_mysqlbinlog:
// could be with the mysql distribution, or with the vt distribution
func findVtMysqlbinlogDir() (string, error) {
	// first look in VtRoot
	dir, err := vtenv.VtRoot()
	if err == nil {
		if _, err = os.Stat(path.Join(dir, "bin/vt_mysqlbinlog")); err == nil {
			return dir, nil
		}
	}

	// then look in VtMysqlRoot
	dir, err = vtenv.VtMysqlRoot()
	if err == nil {
		if _, err = os.Stat(path.Join(dir, "bin/vt_mysqlbinlog")); err == nil {
			return dir, nil
		}
	}

	// then try current directory + bin/vt_mysqlbinlog
	if _, err = os.Stat("bin/vt_mysqlbinlog"); err == nil {
		return "", nil
	}

	return "", fmt.Errorf("Cannot find vt_mysqlbinlog binary")
}

// return a Reader from which the decoded binlog can be read
func (decoder *BinlogDecoder) DecodeMysqlBinlog(binlog *os.File) (io.Reader, error) {
	dir, err := findVtMysqlbinlogDir()
	if err != nil {
		return nil, err
	}
	dir = path.Join(dir, "bin")
	name := "vt_mysqlbinlog"
	arg := []string{"vt_mysqlbinlog", "-"}

	dataRdFile, dataWrFile, pipeErr := os.Pipe()
	if pipeErr != nil {
		relog.Error("DecodeMysqlBinlog: error in creating pipe %v", pipeErr)
		return nil, pipeErr
	}
	// let the caller close the read file
	defer dataWrFile.Close()

	fds := []*os.File{
		binlog,
		dataWrFile,
		os.Stderr,
	}

	attrs := &os.ProcAttr{Dir: dir, Files: fds}

	process, err := os.StartProcess(name, arg, attrs)
	if err != nil {
		relog.Error("DecodeMysqlBinlog: error in decoding binlog %v", err)
		return nil, err
	}
	decoder.process = process

	go func() {
		// just make sure we don't spawn zombies
		waitMsg, err := decoder.process.Wait()
		if err != nil {
			relog.Error("vt_mysqlbinlog exited: %v err: %v", waitMsg, err)
		} else {
			relog.Info("vt_mysqlbinlog exited: %v err: %v", waitMsg, err)
		}
	}()

	return dataRdFile, nil
}

func (decoder *BinlogDecoder) Kill() error {
	//relog.Info("Killing vt_mysqlbinlog pid %v", decoder.process.Pid)
	return decoder.process.Kill()
}
