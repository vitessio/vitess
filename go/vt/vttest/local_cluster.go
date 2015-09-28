// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package vttest provides the functionality to bring
// up a test cluster.
package vttest

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"os"
	"os/exec"
	"path"
	"strconv"
	"time"

	"github.com/youtube/vitess/go/sqldb"
)

// Handle allows you to interact with the processes launched by vttest.
type Handle struct {
	Data map[string]interface{}

	cmd   *exec.Cmd
	stdin io.WriteCloser

	// dbname is valid only for LaunchMySQL.
	dbname string
}

// LaunchVitess launches a vitess test cluster.
func LaunchVitess(topo, schemaDir string, verbose bool) (hdl *Handle, err error) {
	hdl = &Handle{}
	err = hdl.run(randomPort(), topo, schemaDir, false, verbose)
	if err != nil {
		return nil, err
	}
	return hdl, nil
}

// LaunchMySQL launches just a MySQL instance with the specified db name. The schema
// is specified as a string instead of a file.
func LaunchMySQL(dbName, schema string, verbose bool) (hdl *Handle, err error) {
	hdl = &Handle{
		dbname: dbName,
	}
	var schemaDir string
	if schema != "" {
		schemaDir, err = ioutil.TempDir("", "vt")
		if err != nil {
			return nil, err
		}
		defer os.RemoveAll(schemaDir)
		ksDir := path.Join(schemaDir, dbName)
		err = os.Mkdir(ksDir, os.ModeDir|0775)
		if err != nil {
			return nil, err
		}
		fileName := path.Join(ksDir, "schema.sql")
		f, err := os.Create(fileName)
		if err != nil {
			return nil, err
		}
		n, err := f.WriteString(schema)
		if n != len(schema) {
			return nil, errors.New("short write")
		}
		if err != nil {
			return nil, err
		}
		err = f.Close()
		if err != nil {
			return nil, err
		}
	}
	err = hdl.run(randomPort(), fmt.Sprintf("%s/0:%s", dbName, dbName), schemaDir, true, verbose)
	if err != nil {
		return nil, err
	}
	return hdl, nil
}

// TearDown tears down the launched processes.
func (hdl *Handle) TearDown() error {
	_, err := hdl.stdin.Write([]byte("\n"))
	if err != nil {
		return err
	}
	return hdl.cmd.Wait()
}

// MySQLConnParams builds the MySQL connection params.
// It's valid only if you used LaunchMySQL.
func (hdl *Handle) MySQLConnParams() (sqldb.ConnParams, error) {
	params := sqldb.ConnParams{
		Charset: "utf8",
		DbName:  hdl.dbname,
	}
	if hdl.Data == nil {
		return params, errors.New("no data")
	}
	iuser, ok := hdl.Data["username"]
	if !ok {
		return params, errors.New("no username")
	}
	user, ok := iuser.(string)
	if !ok {
		return params, fmt.Errorf("invalid user type: %T", iuser)
	}
	params.Uname = user
	if ipassword, ok := hdl.Data["password"]; ok {
		password, ok := ipassword.(string)
		if !ok {
			return params, fmt.Errorf("invalid password type: %T", ipassword)
		}
		params.Pass = password
	}
	if ihost, ok := hdl.Data["host"]; ok {
		host, ok := ihost.(string)
		if !ok {
			return params, fmt.Errorf("invalid host type: %T", ihost)
		}
		params.Host = host
	}
	if iport, ok := hdl.Data["port"]; ok {
		port, ok := iport.(float64)
		if !ok {
			return params, fmt.Errorf("invalid port type: %T", iport)
		}
		params.Port = int(port)
	}
	if isocket, ok := hdl.Data["socket"]; ok {
		socket, ok := isocket.(string)
		if !ok {
			return params, fmt.Errorf("invalid socket type: %T", isocket)
		}
		params.UnixSocket = socket
	}
	return params, nil
}

func (hdl *Handle) run(port int, topo, schemaDir string, mysqlOnly, verbose bool) error {
	launcher, err := launcherPath()
	if err != nil {
		return err
	}
	hdl.cmd = exec.Command(
		launcher,
		"--port", strconv.Itoa(port),
		"--topology", topo,
	)
	if schemaDir != "" {
		hdl.cmd.Args = append(hdl.cmd.Args, "--schema_dir", schemaDir)
	}
	if mysqlOnly {
		hdl.cmd.Args = append(hdl.cmd.Args, "--mysql_only")
	}
	if verbose {
		hdl.cmd.Args = append(hdl.cmd.Args, "--verbose")
	}
	hdl.cmd.Stderr = os.Stderr
	stdout, err := hdl.cmd.StdoutPipe()
	if err != nil {
		return err
	}
	decoder := json.NewDecoder(stdout)
	hdl.stdin, err = hdl.cmd.StdinPipe()
	if err != nil {
		return err
	}
	err = hdl.cmd.Start()
	if err != nil {
		return err
	}
	return decoder.Decode(&hdl.Data)
}

// randomPort returns a random number between 10k & 30k.
func randomPort() int {
	v := rand.Int31n(20000)
	return int(v + 10000)
}

func init() {
	rand.Seed(time.Now().UnixNano())
}
