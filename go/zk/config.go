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
)

var (
	// DefaultZkConfigPaths is the default list of config files to check.
	DefaultZkConfigPaths = []string{"/etc/zookeeper/zk_client.json"}
	// MagicPrefix is the Default name for the root note in the zookeeper tree.
	MagicPrefix = "zk"

	localCell      = flag.String("zk.local-cell", "", "closest zk cell used for /zk/local paths")
	globalAddrs    = flag.String("zk.global-addrs", "", "list of global zookeeper servers (host:port, ...)")
	baseTimeout    = flag.Duration("zk.base-timeout", 30*time.Second, "zk base timeout (see zkconn.go)")
	connectTimeout = flag.Duration("zk.connect-timeout", 30*time.Second, "zk connect timeout")
)

// GuessLocalCell reads the cell from -zk.local-cell, or the environment
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

// ZkCellFromZkPath extracts the cell name from a zkPath.
func ZkCellFromZkPath(zkPath string) (string, error) {
	pathParts := strings.Split(zkPath, "/")
	if len(pathParts) < 3 {
		return "", fmt.Errorf("no cell name in path: %v", zkPath)
	}
	if pathParts[0] != "" || pathParts[1] != MagicPrefix {
		return "", fmt.Errorf("path should start with /%v: %v", MagicPrefix, zkPath)
	}
	cell := pathParts[2]
	if cell == "" || strings.Contains(cell, "-") {
		return "", fmt.Errorf("invalid cell name %q", cell)
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

func getCellAddrMap() (map[string]string, error) {
	var cellAddrMap map[string]string
	for _, configPath := range getConfigPaths() {
		file, err := os.Open(configPath)
		if err != nil {
			return nil, fmt.Errorf("error reading config file %v: %v", configPath, err)
		}
		err = json.NewDecoder(file).Decode(&cellAddrMap)
		file.Close()
		if err != nil {
			return nil, fmt.Errorf("error decoding config file %v: %v", configPath, err)
		}

		return cellAddrMap, nil
	}
	return nil, fmt.Errorf("no config file paths found")
}

// ZkPathToZkAddr returns the zookeeper server address to use for the
// given path.
func ZkPathToZkAddr(zkPath string) (string, error) {
	cell, err := ZkCellFromZkPath(zkPath)
	if err != nil {
		return "", err
	}

	cellAddrMap, err := getCellAddrMap()
	if err != nil {
		return "", err
	}
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

	addr := cellAddrMap[cell]
	if addr != "" {
		return addr, nil
	}

	return "", fmt.Errorf("no addr found for zk cell: %#v", cell)
}

// ZkKnownCells returns all the known cells, alphabetically ordered. It will
// include 'global' if there is a dc-specific global cell or a global cell.
func ZkKnownCells() ([]string, error) {
	localCell := GuessLocalCell()
	cellAddrMap, err := getCellAddrMap()
	if err != nil {
		return nil, err
	}
	result := make([]string, 0, len(cellAddrMap))
	foundGlobal := false
	for cell := range cellAddrMap {
		// handle global, we just remember it
		if cell == "global" {
			foundGlobal = true
			continue
		}

		// skip global cells, remember if we have our global
		if strings.HasSuffix(cell, "-global") {
			if cell == localCell+"-global" {
				foundGlobal = true
			}
			continue
		}

		// non-global cell
		result = append(result, cell)
	}
	if foundGlobal {
		result = append(result, "global")
	}
	sort.Strings(result)
	return result, nil
}
