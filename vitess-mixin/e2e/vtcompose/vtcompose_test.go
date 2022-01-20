/*
 * Copyright 2019 The Vitess Authors.

 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at

 *     http://www.apache.org/licenses/LICENSE-2.0

 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package main

import (
	"fmt"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
)

var (
	referenceYaml         = string(readFile("./docker-compose.test.yml"))
	testComposeFile       = readFile("./docker-compose.base.yml")
	testKeyspaceInfoMap   = parseKeyspaceInfo(DefaultKeyspaceData)
	testExternalDbInfoMap = parseExternalDbData(DefaultExternalDbData)

	testVtOpts = vtOptions{
		webPort:       DefaultWebPort,
		gRpcPort:      DefaultGrpcPort,
		mySqlPort:     DefaultMysqlPort,
		topologyFlags: DefaultTopologyFlags,
		cell:          DefaultCell,
	}
)

func TestGenerateCorrectFileWithDefaultOpts(t *testing.T) {
	baseFile := testComposeFile
	finalFile := applyDockerComposePatches(baseFile, testKeyspaceInfoMap, testExternalDbInfoMap, testVtOpts)

	yamlString := string(finalFile)
	assert.YAMLEq(t, referenceYaml, yamlString)
}

func TestOptsAppliedThroughoutGeneratedFile(t *testing.T) {
	baseFile := testComposeFile
	options := vtOptions{
		webPort:       55_555,
		gRpcPort:      66_666,
		mySqlPort:     77_777,
		topologyFlags: "-custom -flags",
		cell:          "custom cell",
	}
	finalFile := applyDockerComposePatches(baseFile, testKeyspaceInfoMap, testExternalDbInfoMap, options)
	yamlString := string(finalFile)

	// These asserts are not exhaustive, but should cover most cases.
	assert.NotContains(t, yamlString, strconv.Itoa(DefaultWebPort))
	assert.Contains(t, yamlString, strconv.Itoa(options.webPort))

	assert.NotContains(t, yamlString, strconv.Itoa(DefaultGrpcPort))
	assert.Contains(t, yamlString, strconv.Itoa(options.gRpcPort))

	assert.NotContains(t, yamlString, ":"+strconv.Itoa(DefaultMysqlPort))
	assert.Contains(t, yamlString, ":"+strconv.Itoa(options.webPort))

	assert.NotContains(t, yamlString, fmt.Sprintf("-cell %s", DefaultCell))
	assert.Contains(t, yamlString, fmt.Sprintf("-cell %s", options.cell))

	assert.Contains(t, yamlString, fmt.Sprintf("- TOPOLOGY_FLAGS=%s", options.topologyFlags))
	assert.NotContains(t, yamlString, DefaultTopologyFlags)
}
