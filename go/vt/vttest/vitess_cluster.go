// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package vttest provides the functionality to bring
// up a test cluster.
package vttest

import (
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"strings"
)

var (
	curShardNames []string
	curReplicas   int
	curRdonly     int
	curKeyspace   string
	curSchema     string
	curVSchema    string
)

func run(shardNames []string, replicas, rdonly int, keyspace, schema, vschema, op string) error {
	curShardNames = shardNames
	curReplicas = replicas
	curRdonly = rdonly
	curKeyspace = keyspace
	curSchema = schema
	curVSchema = vschema

	vttop := os.Getenv("VTTOP")
	if vttop == "" {
		return errors.New("VTTOP not set")
	}
	cfg, err := json.Marshal(map[string]int{
		"replica": replicas,
		"rdonly":  rdonly,
	})
	if err != nil {
		return err
	}
	cmd := exec.Command(
		"python",
		vttop+"/test/java_vtgate_test_helper.py",
		"--shards",
		strings.Join(shardNames, ","),
		"--tablet-config",
		string(cfg),
		"--keyspace",
		keyspace,
	)
	if schema != "" {
		cmd.Args = append(cmd.Args, "--schema", schema)
	}
	if vschema != "" {
		cmd.Args = append(cmd.Args, "--vschema", vschema)
	}
	cmd.Args = append(cmd.Args, op)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

// Launch launches the cluster. Only one cluster can be active at a time.
func Launch(shardNames []string, replicas, rdonly int, keyspace, schema, vschema string) error {
	err := run(shardNames, replicas, rdonly, keyspace, schema, vschema, "setup")
	if err != nil {
		Teardown()
	}
	return err
}

// Teardown shuts down the previously launched cluster.
func Teardown() error {
	return run(curShardNames, curReplicas, curRdonly, curKeyspace, curSchema, curVSchema, "teardown")
}
