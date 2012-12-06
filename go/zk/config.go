// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package zk

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

//	"launchpad.net/gozk/zookeeper"
)

const (
	DEFAULT_BASE_TIMEOUT = 5 * time.Second
)

var zkConfigPaths = []string{"/etc/zookeeper/zk_client.json"}
var localCell = flag.String("zk.local-cell", "",
	"closest zk cell used for /zk/local paths")
var localAddrs = flag.String("zk.local-addrs", "",
	"list of zookeeper servers (host:port, ...)")
var globalAddrs = flag.String("zk.global-addrs", "",
	"list of global zookeeper servers (host:port, ...)")
var baseTimeout = flag.Duration("zk.base-timeout", DEFAULT_BASE_TIMEOUT,
	"zk or zkocc base timeout (see zkconn.go and zkoccconn.go)")

// Read the cell from -zk.local-cell, or the environment ZK_CLIENT_LOCAL_CELL
// or guess the cell by the hostname. This is either the first two characters
// or the character before a dash '-'.
func GuessLocalCell() string {
	if *localCell != "" {
		return *localCell
	}

	envCell := os.Getenv("ZK_CLIENT_LOCAL_CELL")
	if envCell != "" {
		return envCell
	}

	hostname, err := os.Hostname()
	if err != nil {
		panic(err)
	}

	shortHostname := strings.Split(hostname, ".")[0]
	hostParts := strings.Split(shortHostname, "-")
	if len(hostParts) > 1 {
		return hostParts[0]
	}
	return shortHostname[:2]
}

func ZkCellFromZkPath(zkPath string) string {
	defer func() {
		if x := recover(); x != nil {
			panic(fmt.Errorf("no cell name in path: %v", zkPath))
		}
	}()
	pathParts := strings.Split(zkPath, "/")
	cell := pathParts[2]
	if strings.Contains(cell, "-") {
		panic(fmt.Errorf("invalid cell name %v", cell))
	}
	return cell
}

func getConfigPaths() []string {
	zkConfigPath := os.Getenv("ZK_CLIENT_CONFIG")
	if zkConfigPath != "" {
		return []string{zkConfigPath}
	}
	return zkConfigPaths
}

func ZkPathToZkAddr(zkPath string, useCache bool) string {
	if !strings.HasPrefix(zkPath, "/zk") {
		panic(fmt.Errorf("invalid zk path: %v", zkPath))
	}

	var cellAddrMap map[string]string
	for _, configPath := range getConfigPaths() {
		file, err := os.Open(configPath)
		if err != nil {
			log.Printf("error reading config file: %v: %v", configPath, err)
			continue
		}
		err = json.NewDecoder(file).Decode(&cellAddrMap)
		file.Close()
		if err != nil {
			log.Printf("error decoding config file %v: %v", configPath, err)
			continue
		}

		break
	}

	cell := ZkCellFromZkPath(zkPath)
	if cell == "local" {
		cell = GuessLocalCell()
	} else if cell == "global" {
		if *globalAddrs != "" {
			return *globalAddrs
		} else if _, ok := cellAddrMap[cell]; !ok {
			// if there is no "global" cell, look for a dc-specific
			// address for the global cell
			cell = GuessLocalCell() + "-global"
		}
	}
	if useCache {
		cell += ":_zkocc"
	}

	addr := cellAddrMap[cell]
	if addr != "" {
		return addr
	}

	panic(fmt.Errorf("no addr found for zk cell: %#v", cell))
}

// GetZkSubprocessFlags returns the flags necessary to run a sub-process
// that would connect to the same zk servers as this process
// (assuming the environment of the sub-process is the same as ours)
func GetZkSubprocessFlags() []string {
	result := make([]string, 0, 0)
	if *localCell != "" {
		result = append(result, "-zk.local-cell", *localCell)
	}
	if *localAddrs != "" {
		result = append(result, "-zk.local-addrs", *localAddrs)
	}
	if *globalAddrs != "" {
		result = append(result, "-zk.global-addrs", *globalAddrs)
	}
	if *baseTimeout != DEFAULT_BASE_TIMEOUT {
		result = append(result, "-zk.base-timeout", baseTimeout.String())
	}
	return result
}
