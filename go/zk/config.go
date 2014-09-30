// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package zk

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"
	"unicode"

	log "github.com/golang/glog"
)

const (
	DEFAULT_BASE_TIMEOUT = 5 * time.Second
)

var (
	// DefaultZkConfigPaths is the default list of config files to check.
	DefaultZkConfigPaths = []string{"/etc/zookeeper/zk_client.json"}
	// MagicPrefix is the Default name for the root note in the zookeeper tree.
	MagicPrefix = "zk"

	localCell      = flag.String("zk.local-cell", "", "closest zk cell used for /zk/local paths")
	localAddrs     = flag.String("zk.local-addrs", "", "list of zookeeper servers (host:port, ...)")
	globalAddrs    = flag.String("zk.global-addrs", "", "list of global zookeeper servers (host:port, ...)")
	baseTimeout    = flag.Duration("zk.base-timeout", DEFAULT_BASE_TIMEOUT, "zk or zkocc base timeout (see zkconn.go and zkoccconn.go)")
	connectTimeout = flag.Duration("zk.connect-timeout", 30*time.Second, "zk connect timeout")
)

// Read the cell from -zk.local-cell, or the environment
// ZK_CLIENT_LOCAL_CELL or guess the cell by the hostname. The
// letter-only prefix of the string is used as the cell name. For
// instance:
//
// pa1 -> pa
// sjl-1 -> sjl
// lwc1 -> lwc
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
		panic(err) // should never happen
	}

	shortHostname := strings.Split(hostname, ".")[0]
	return letterPrefix(shortHostname)
}

// Return the string prefix up to the first non-letter.
func letterPrefix(str string) string {
	for i, rune := range str {
		if !unicode.IsLetter(rune) {
			return str[:i]
		}
	}
	return str
}

func ZkCellFromZkPath(zkPath string) (string, error) {
	pathParts := strings.Split(zkPath, "/")
	if len(pathParts) < 3 {
		return "", fmt.Errorf("no cell name in path: %v", zkPath)
	}
	if pathParts[0] != "" || pathParts[1] != MagicPrefix {
		return "", fmt.Errorf("path should start with /%v: %v", MagicPrefix, zkPath)
	}
	cell := pathParts[2]
	if strings.Contains(cell, "-") {
		return "", fmt.Errorf("invalid cell name %v", cell)
	}
	return cell, nil
}

func getConfigPaths() []string {
	zkConfigPath := os.Getenv("ZK_CLIENT_CONFIG")
	if zkConfigPath != "" {
		return []string{zkConfigPath}
	}
	return DefaultZkConfigPaths
}

func getCellAddrMap() map[string]string {
	var cellAddrMap map[string]string
	for _, configPath := range getConfigPaths() {
		file, err := os.Open(configPath)
		if err != nil {
			log.Infof("error reading config file: %v: %v", configPath, err)
			continue
		}
		err = json.NewDecoder(file).Decode(&cellAddrMap)
		file.Close()
		if err != nil {
			log.Infof("error decoding config file %v: %v", configPath, err)
			continue
		}

		break
	}
	return cellAddrMap
}

func ZkPathToZkAddr(zkPath string, useCache bool) (string, error) {
	cell, err := ZkCellFromZkPath(zkPath)
	if err != nil {
		return "", err
	}

	cellAddrMap := getCellAddrMap()
	if cell == "local" {
		cell = GuessLocalCell()
	} else if cell == "global" {
		if *globalAddrs != "" {
			return *globalAddrs, nil
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
		return addr, nil
	}

	return "", fmt.Errorf("no addr found for zk cell: %#v", cell)
}

// returns all the known cells, alphabetically ordered. It will
// include 'global' if there is a dc-specific global cell or a global cell
func ZkKnownCells(useCache bool) []string {
	localCell := GuessLocalCell()
	cellAddrMap := getCellAddrMap()
	result := make([]string, 0, len(cellAddrMap))
	foundGlobal := false
	for cell := range cellAddrMap {
		// figure out cell name and if it's zkocc
		isZkoccCell := false
		name := cell
		if strings.HasSuffix(name, ":_zkocc") {
			name = name[:len(name)-7]
			isZkoccCell = true
		}
		if (useCache && !isZkoccCell) || (!useCache && isZkoccCell) {
			// this is not the zkocc you're looking for
			continue
		}

		// handle global, we just remember it
		if name == "global" {
			foundGlobal = true
			continue
		}

		// skip global cells, remember if we have our global
		if strings.HasSuffix(name, "-global") {
			if name == localCell+"-global" {
				foundGlobal = true
			}
			continue
		}

		// non-global cell
		result = append(result, name)
	}
	if foundGlobal {
		result = append(result, "global")
	}
	sort.Strings(result)
	return result
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
