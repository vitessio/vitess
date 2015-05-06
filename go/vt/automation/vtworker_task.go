// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package automation

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"time"

	pb "github.com/youtube/vitess/go/vt/proto/automation"
)

// VtworkerTask triggers an action on a running vtworker process using the HTTP server and waits until the process has finished.
// TODO(mberlin): Add an RPC interface at vtworker and use that instead.
type VtworkerTask struct {
}

func getWorkerStatus(vtworkerEndpoint string) (string, error) {
	resp, err := http.Get("http://" + vtworkerEndpoint + "/debug/vars")
	if err != nil {
		fmt.Println("Failed to download varz:", err)
		return "", err
	}
	defer resp.Body.Close()
	body, errReadAll := ioutil.ReadAll(resp.Body)
	if errReadAll != nil {
		fmt.Println("Failed to read the response:", errReadAll)
		return "", errReadAll
	}

	var m map[string]interface{}
	errJSON := json.Unmarshal(body, &m)
	if errJSON != nil {
		fmt.Println("Failed to unmarshall varz:", err, "varz:", body)
		return "", errJSON
	}

	return m["WorkerState"].(string), nil
}

func resetWorker(vtworkerEndpoint string) error {
	resp, err := http.Get("http://" + vtworkerEndpoint + "/reset")
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if err != nil {
		return err
	}

	return nil
}

func (t *VtworkerTask) run(parameters map[string]string) ([]*pb.TaskContainer, string, error) {
	vtworkerEndpoint := parameters["vtworker_endpoint"]

	var actionPath string
	switch parameters["command"] {
	case "SplitClone":
		actionPath = "/Clones/SplitClone"
	case "SplitDiff":
		actionPath = "/Diffs/SplitDiff"
	default:
		return nil, "", fmt.Errorf("Unknown vtworker command: %v", parameters["command"])
	}

	// Run vtworker command.
	// TODO(mberlin): Process "command" parameter and differ between clone and diff.
	resp, err := http.PostForm(
		"http://"+vtworkerEndpoint+actionPath,
		url.Values{"strategy": {"-populate_blp_checkpoint"},
			"keyspace": {parameters["keyspace"]},
			"shard":    {parameters["shard"]},
			// TODO(mberlin): Remove/change this line when the "form was sent" detection was fixed in split_clone.go.
			"sourceReaderCount": {"10"},
			// TODO(mberlin): Remove these fields when they are no longer necessary.
			"destinationPackCount":   {"10"},
			"minTableSizeForSplit":   {"1048576"},
			"destinationWriterCount": {"20"},
			"excludeTables":          {" "},
		})
	if err != nil {
		return nil, "", err
	}
	defer resp.Body.Close()

	// Wait for it to finish.
	// TODO(mberlin): Add timeout.
outer:
	for {
		state, err := getWorkerStatus(vtworkerEndpoint)
		if err != nil {
			return nil, "", err
		}
		switch state {
		case "done":
			break outer
		case "error":
			return nil, "", fmt.Errorf("vtworker did not succeed")
		default:
			// Waiting for state to change.
			time.Sleep(50 * time.Millisecond)
		}
	}

	// If successful, reset the worker.
	errReset := resetWorker(vtworkerEndpoint)
	if errReset != nil {
		return nil, "", errReset
	}

	return nil, "", nil
}

func (t *VtworkerTask) requiredParameters() []string {
	return []string{"command", "keyspace", "shard"}
}
