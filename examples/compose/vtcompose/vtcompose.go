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
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"math"
	"os"
	"regexp"
	"strconv"
	"strings"

	jsonpatch "github.com/evanphx/json-patch"
	yamlpatch "github.com/krishicks/yaml-patch"

	"vitess.io/vitess/go/vt/log"
)

var (
	tabletsUsed           = 0
	tablesPath            = "tables/"
	baseDockerComposeFile = flag.String("base_yaml", "vtcompose/docker-compose.base.yml", "Starting docker-compose yaml")
	baseVschemaFile       = flag.String("base_vschema", "vtcompose/base_vschema.json", "Starting vschema json")

	topologyFlags = flag.String("topologyFlags",
		"-topo_implementation consul -topo_global_server_address consul1:8500 -topo_global_root vitess/global",
		"Vitess Topology Flags config")
	webPort        = flag.String("webPort", "8080", "Web port to be used")
	gRpcPort       = flag.String("gRpcPort", "15999", "gRPC port to be used")
	mySqlPort      = flag.String("mySqlPort", "15306", "mySql port to be used")
	cell           = flag.String("cell", "test", "Vitess Cell name")
	keyspaceData   = flag.String("keyspaceData", "test_keyspace:2:1:create_messages.sql,create_tokens.sql;unsharded_keyspace:0:0:create_dinosaurs.sql,create_eggs.sql", "List of keyspace_name/external_db_name:num_of_shards:num_of_replica_tablets:schema_files:<optional>lookup_keyspace_name separated by ';'")
	externalDbData = flag.String("externalDbData", "", "List of Data corresponding to external DBs. List of <external_db_name>,<DB_HOST>,<DB_PORT>,<DB_USER>,<DB_PASS>,<DB_CHARSET> separated by ';'")
)

type keyspaceInfo struct {
	keyspace        string
	shards          int
	replicaTablets  int
	lookupKeyspace  string
	useLookups      bool
	schemaFile      *os.File
	schemaFileNames []string
}

type externalDbInfo struct {
	dbName    string
	dbHost    string
	dbPort    string
	dbUser    string
	dbPass    string
	dbCharset string
}

func newKeyspaceInfo(keyspace string, shards int, replicaTablets int, schemaFiles []string, lookupKeyspace string) keyspaceInfo {
	k := keyspaceInfo{keyspace: keyspace, shards: shards, replicaTablets: replicaTablets, schemaFileNames: schemaFiles, lookupKeyspace: lookupKeyspace}
	if len(strings.TrimSpace(lookupKeyspace)) == 0 {
		k.useLookups = false
	} else {
		k.useLookups = true
	}

	k.schemaFile = nil
	return k
}

func newExternalDbInfo(dbName, dbHost, dbPort, dbUser, dbPass, dbCharset string) externalDbInfo {
	d := externalDbInfo{dbName: dbName, dbHost: dbHost, dbPort: dbPort, dbUser: dbUser, dbPass: dbPass, dbCharset: dbCharset}
	return d
}

func main() {
	flag.Parse()
	keyspaceInfoMap := make(map[string]keyspaceInfo)
	externalDbInfoMap := make(map[string]externalDbInfo)

	for _, v := range strings.Split(*keyspaceData, ";") {
		tokens := strings.Split(v, ":")
		shards, _ := strconv.Atoi(tokens[1])
		replicaTablets, _ := strconv.Atoi(tokens[2])
		schemaFileNames := strings.Split(tokens[3], ",")
		print(shards)

		if len(tokens) > 4 {
			keyspaceInfoMap[tokens[0]] = newKeyspaceInfo(tokens[0], shards, replicaTablets, schemaFileNames, tokens[4])
		} else {
			keyspaceInfoMap[tokens[0]] = newKeyspaceInfo(tokens[0], shards, replicaTablets, schemaFileNames, "")
		}
	}

	for _, v := range strings.Split(*externalDbData, ";") {
		tokens := strings.Split(v, ":")
		if len(tokens) > 1 {
			externalDbInfoMap[tokens[0]] = newExternalDbInfo(tokens[0], tokens[1], tokens[2], tokens[3], tokens[4], tokens[5])
		}
	}

	for k, v := range keyspaceInfoMap {
		if _, ok := externalDbInfoMap[k]; !ok {
			v.schemaFile = createFile(fmt.Sprintf("%s%s_schema_file.sql", tablesPath, v.keyspace))
			appendtoSqlFile(v.schemaFileNames, v.schemaFile)
			closeFile(v.schemaFile)
		}
	}

	// Vschema Patching
	for k, keyspaceData := range keyspaceInfoMap {
		vSchemaFile := readFile(*baseVschemaFile)
		if keyspaceData.shards == 0 {
			vSchemaFile = applyJsonInMemoryPatch(vSchemaFile, `[{"op": "replace","path": "/sharded", "value": false}]`)
		}

		// Check if it is an external_db
		if _, ok := externalDbInfoMap[k]; ok {
			//This is no longer necessary, but we'll keep it for reference
			//https://github.com/vitessio/vitess/pull/4868, https://github.com/vitessio/vitess/pull/5010
			//vSchemaFile = applyJsonInMemoryPatch(vSchemaFile,`[{"op": "add","path": "/tables/*", "value": {}}]`)
		} else {
			var primaryTableColumns map[string]string
			vSchemaFile, primaryTableColumns = addTablesVschemaPatch(vSchemaFile, keyspaceData.schemaFileNames)

			if keyspaceData.useLookups {
				lookupKeyspace := keyspaceInfoMap[keyspaceData.lookupKeyspace]
				vSchemaFile = addLookupDataToVschema(vSchemaFile, lookupKeyspace.schemaFileNames, primaryTableColumns, lookupKeyspace.keyspace)
			}
		}

		writeVschemaFile(vSchemaFile, fmt.Sprintf("%s_vschema.json", keyspaceData.keyspace))
	}

	// Docker Compose File Patches
	dockerComposeFile := readFile(*baseDockerComposeFile)
	dockerComposeFile = applyDockerComposePatches(dockerComposeFile, keyspaceInfoMap, externalDbInfoMap)
	writeFile(dockerComposeFile, "docker-compose.yml")
}

func applyFilePatch(dockerYaml []byte, patchFile string) []byte {
	yamlPatch, err := ioutil.ReadFile(patchFile)
	if err != nil {
		log.Fatalf("reading yaml patch file %s: %s", patchFile, err)
	}

	patch, err := yamlpatch.DecodePatch(yamlPatch)
	if err != nil {
		log.Fatalf("decoding patch failed: %s", err)
	}

	bs, err := patch.Apply(dockerYaml)
	if err != nil {
		log.Fatalf("applying patch failed: %s", err)
	}
	return bs
}

func applyJsonInMemoryPatch(vSchemaFile []byte, patchString string) []byte {
	patch, err := jsonpatch.DecodePatch([]byte(patchString))
	if err != nil {
		log.Fatalf("decoding vschema patch failed: %s", err)
	}

	modified, err := patch.Apply(vSchemaFile)
	if err != nil {
		log.Fatalf("applying vschema patch failed: %s", err)
	}
	return modified
}

func applyInMemoryPatch(dockerYaml []byte, patchString string) []byte {
	patch, err := yamlpatch.DecodePatch([]byte(patchString))
	if err != nil {
		log.Fatalf("decoding patch failed: %s", err)
	}

	bs, err := patch.Apply(dockerYaml)
	if err != nil {
		log.Fatalf("applying patch failed: %s", err)
	}
	return bs
}

func createFile(filePath string) *os.File {
	f, err := os.Create(filePath)
	if err != nil {
		log.Fatalf("creating %s %s", filePath, err)
	}
	return f
}

func readFile(filePath string) []byte {
	file, err := ioutil.ReadFile(filePath)

	if err != nil {
		log.Fatalf("reading %s: %s", filePath, err)
	}

	return file
}

func closeFile(file *os.File) {
	err := file.Close()
	if err != nil {
		log.Fatalf("Closing schema_file.sql %s", err)
	}
}

func handleError(err error) {
	if err != nil {
		log.Fatalf("Error: %s", err)
	}
}

func appendtoSqlFile(schemaFileNames []string, f *os.File) {
	for _, file := range schemaFileNames {
		data, err := ioutil.ReadFile(tablesPath + file)
		_, err = f.Write(data)
		handleError(err)

		_, err = f.WriteString("\n\n")
		handleError(err)

		err = f.Sync()
		handleError(err)
	}
}

func getTableName(sqlFile string) string {
	sqlFileData, err := ioutil.ReadFile(sqlFile)
	if err != nil {
		log.Fatalf("reading sqlFile file %s: %s", sqlFile, err)
	}

	r, _ := regexp.Compile("CREATE TABLE ([a-z_-]*) \\(")
	rs := r.FindStringSubmatch(string(sqlFileData))
	// replace all ` from table name if exists
	return strings.ReplaceAll(rs[1], "`", "")
}

func getPrimaryKey(sqlFile string) string {
	sqlFileData, err := ioutil.ReadFile(sqlFile)
	if err != nil {
		log.Fatalf("reading sqlFile file %s: %s", sqlFile, err)
	}

	r, _ := regexp.Compile("PRIMARY KEY \\((.*)\\).*")
	rs := r.FindStringSubmatch(string(sqlFileData))

	return rs[1]
}

func getKeyColumns(sqlFile string) string {
	sqlFileData, err := ioutil.ReadFile(sqlFile)
	if err != nil {
		log.Fatalf("reading sqlFile file %s: %s", sqlFile, err)
	}

	r, _ := regexp.Compile("[^PRIMARY] (KEY|UNIQUE KEY) .*\\((.*)\\).*")
	rs := r.FindStringSubmatch(string(sqlFileData))
	print(rs[2])
	// replace all ` from column names if exists
	return strings.ReplaceAll(rs[2], "`", "")
}

func addTablesVschemaPatch(vSchemaFile []byte, schemaFileNames []string) ([]byte, map[string]string) {
	indexedColumns := ""
	primaryTableColumns := make(map[string]string)
	for _, fileName := range schemaFileNames {
		tableName := getTableName(tablesPath + fileName)
		indexedColumns = getPrimaryKey(tablesPath + fileName)
		firstColumnName := strings.Split(indexedColumns, ", ")[0]
		vSchemaFile = applyJsonInMemoryPatch(vSchemaFile, generatePrimaryVIndex(tableName, firstColumnName, "hash"))
		primaryTableColumns[tableName] = firstColumnName
	}

	return vSchemaFile, primaryTableColumns
}

func addLookupDataToVschema(vSchemaFile []byte, schemaFileNames []string, primaryTableColumns map[string]string, keyspace string) []byte {
	for _, fileName := range schemaFileNames {
		tableName := fileName[7 : len(fileName)-4]
		lookupTableOwner := ""

		// Find owner of lookup table
		for primaryTableName, _ := range primaryTableColumns {
			if strings.HasPrefix(tableName, primaryTableName) && len(primaryTableName) > len(lookupTableOwner) {
				lookupTableOwner = primaryTableName
			}
		}

		indexedColumns := getKeyColumns(tablesPath + fileName)
		firstColumnName := strings.Split(indexedColumns, ", ")[0]

		// Lookup patch under "tables"
		vSchemaFile = applyJsonInMemoryPatch(vSchemaFile, addToColumnVIndexes(lookupTableOwner, firstColumnName, tableName))

		// Generate Vschema lookup hash types
		vSchemaFile = applyJsonInMemoryPatch(vSchemaFile,
			generateVschemaLookupHash(tableName, keyspace, firstColumnName, primaryTableColumns[lookupTableOwner], lookupTableOwner))
	}

	return vSchemaFile
}

func writeVschemaFile(file []byte, fileName string) {
	// Format json file
	var buf bytes.Buffer
	err := json.Indent(&buf, file, "", "\t")
	handleError(err)
	file = buf.Bytes()

	writeFile(file, fileName)
}

func writeFile(file []byte, fileName string) {
	err := ioutil.WriteFile(fileName, file, 0644)
	if err != nil {
		log.Fatalf("writing %s %s", fileName, err)
	}
}

func applyKeyspaceDependentPatches(dockerComposeFile []byte, keyspaceData keyspaceInfo, externalDbInfoMap map[string]externalDbInfo) []byte {
	var externalDbInfo externalDbInfo
	if val, ok := externalDbInfoMap[keyspaceData.keyspace]; ok {
		externalDbInfo = val
	}
	tabAlias := 0 + tabletsUsed*100
	shard := "-"
	var masterTablets []string
	if tabletsUsed == 0 {
		masterTablets = append(masterTablets, "101")
	} else {
		masterTablets = append(masterTablets, strconv.Itoa((tabletsUsed+1)*100+1))
	}
	interval := int(math.Floor(256 / float64(keyspaceData.shards)))

	for i := 1; i < keyspaceData.shards; i++ {
		masterTablets = append(masterTablets, strconv.Itoa((i+1)*100+1))
	}

	dockerComposeFile = applyInMemoryPatch(dockerComposeFile, generateSchemaload(masterTablets, "", keyspaceData.keyspace, externalDbInfo))

	// Append Master and Replica Tablets
	if keyspaceData.shards < 2 {
		tabAlias = tabAlias + 100
		dockerComposeFile = applyTabletPatches(dockerComposeFile, tabAlias, shard, keyspaceData, externalDbInfoMap)
	} else {
		// Determine shard range
		for i := 0; i < keyspaceData.shards; i++ {
			if i == 0 {
				shard = fmt.Sprintf("-%x", interval)
			} else if i == (keyspaceData.shards - 1) {
				shard = fmt.Sprintf("%x-", interval*i)
			} else {
				shard = fmt.Sprintf("%x-%x", interval*(i), interval*(i+1))
			}
			tabAlias = tabAlias + 100
			dockerComposeFile = applyTabletPatches(dockerComposeFile, tabAlias, shard, keyspaceData, externalDbInfoMap)
		}
	}

	tabletsUsed += len(masterTablets)
	return dockerComposeFile
}

func applyDefaultDockerPatches(dockerComposeFile []byte) []byte {
	dockerComposeFile = applyInMemoryPatch(dockerComposeFile, generateVtctld())
	dockerComposeFile = applyInMemoryPatch(dockerComposeFile, generateVtgate())
	dockerComposeFile = applyInMemoryPatch(dockerComposeFile, generateVtwork())
	return dockerComposeFile
}

func applyDockerComposePatches(dockerComposeFile []byte, keyspaceInfoMap map[string]keyspaceInfo, externalDbInfoMap map[string]externalDbInfo) []byte {
	//Vtctld, vtgate, vtwork patches
	dockerComposeFile = applyDefaultDockerPatches(dockerComposeFile)
	for _, keyspaceData := range keyspaceInfoMap {
		dockerComposeFile = applyKeyspaceDependentPatches(dockerComposeFile, keyspaceData, externalDbInfoMap)
	}

	return dockerComposeFile
}

func applyTabletPatches(dockerComposeFile []byte, tabAlias int, shard string, keyspaceData keyspaceInfo, externalDbInfoMap map[string]externalDbInfo) []byte {
	var dbInfo externalDbInfo
	if val, ok := externalDbInfoMap[keyspaceData.keyspace]; ok {
		dbInfo = val
	}
	dockerComposeFile = applyInMemoryPatch(dockerComposeFile, generateDefaultTablet(strconv.Itoa(tabAlias+1), shard, "master", keyspaceData.keyspace, dbInfo))
	for i := 0; i < keyspaceData.replicaTablets; i++ {
		dockerComposeFile = applyInMemoryPatch(dockerComposeFile, generateDefaultTablet(strconv.Itoa(tabAlias+2+i), shard, "replica", keyspaceData.keyspace, dbInfo))
	}
	return dockerComposeFile
}

// Default Tablet
func generateDefaultTablet(tabAlias, shard, role, keyspace string, dbInfo externalDbInfo) string {
	externalDb := "0"
	if dbInfo.dbName != "" {
		externalDb = "1"
	}
	data := fmt.Sprintf(`
- op: add
  path: /services/vttablet%[1]s
  value:
    image: vitess/base
    ports:
    - "15%[1]s:%[4]s"
    - "%[5]s"
    - "3306"
    volumes:
    - ".:/script"
    environment:
    - TOPOLOGY_FLAGS=%[7]s
    - WEB_PORT=%[4]s
    - GRPC_PORT=%[5]s
    - CELL=%[8]s
    - KEYSPACE=%[6]s
    - SHARD=%[2]s
    - ROLE=%[3]s
    - VTHOST=vttablet%[1]s
    - EXTERNAL_DB=%[9]s
    - DB_PORT=%[10]s
    - DB_HOST=%[11]s
    - DB_USER=%[12]s
    - DB_PASS=%[13]s
    - DB_CHARSET=%[14]s
    command: ["sh", "-c", "/script/vttablet-up.sh %[1]s"]
    depends_on:
      - vtctld
    healthcheck:
        test: ["CMD-SHELL","curl localhost:%[4]s/debug/health"]
        interval: 30s
        timeout: 10s
        retries: 15
`, tabAlias, shard, role, *webPort, *gRpcPort, keyspace, *topologyFlags, *cell, externalDb, dbInfo.dbPort, dbInfo.dbHost, dbInfo.dbUser, dbInfo.dbPass, dbInfo.dbCharset)

	return data
}

// Generate Vtctld
func generateVtctld() string {
	data := fmt.Sprintf(`
- op: add
  path: /services/vtctld
  value:
    image: vitess/base
    ports:
      - "15000:%[1]s"
      - "%[2]s"
    command: ["sh", "-c", " $$VTROOT/bin/vtctld \
        %[3]s \
        -cell %[4]s \
        -workflow_manager_init \
        -workflow_manager_use_election \
        -service_map 'grpc-vtctl' \
        -backup_storage_implementation file \
        -file_backup_storage_root $$VTDATAROOT/backups \
        -logtostderr=true \
        -port %[1]s \
        -grpc_port %[2]s \
        -pid_file $$VTDATAROOT/tmp/vtctld.pid
        "]
    volumes:
      - .:/script
    depends_on:
      - consul1
      - consul2
      - consul3
`, *webPort, *gRpcPort, *topologyFlags, *cell)

	return data
}

// Generate Vtgate
func generateVtgate() string {
	data := fmt.Sprintf(`
- op: add
  path: /services/vtgate
  value:
    image: vitess/base
    ports:
      - "15099:%[1]s"
      - "%[2]s"
      - "15306:%[3]s"
    command: ["sh", "-c", "/script/run-forever.sh $$VTROOT/bin/vtgate \
        %[4]s \
        -logtostderr=true \
        -port %[1]s \
        -grpc_port %[2]s \
        -mysql_server_port %[3]s \
        -mysql_auth_server_impl none \
        -cell %[5]s \
        -cells_to_watch %[5]s \
        -tablet_types_to_wait MASTER,REPLICA,RDONLY \
        -gateway_implementation discoverygateway \
        -service_map 'grpc-vtgateservice' \
        -pid_file $$VTDATAROOT/tmp/vtgate.pid \
        -normalize_queries=true \
        "]
    volumes:
      - .:/script
    depends_on:
      - vtctld
`, *webPort, *gRpcPort, *mySqlPort, *topologyFlags, *cell)

	return data
}

func generateVtwork() string {
	data := fmt.Sprintf(`
- op: add
  path: /services/vtwork
  value:
    image: vitess/base
    ports:
      - "15100:%[1]s"
      - "%[2]s"
    command: ["sh", "-c", "$$VTROOT/bin/vtworker \
        %[3]s \
        -cell %[4]s \
        -logtostderr=true \
        -service_map 'grpc-vtworker' \
        -port %[1]s \
        -grpc_port %[2]s \
        -use_v3_resharding_mode=true \
        -pid_file $$VTDATAROOT/tmp/vtwork.pid \
        "]
    depends_on:
      - vtctld
`, *webPort, *gRpcPort, *topologyFlags, *cell)

	return data
}

func generateSchemaload(tabletAliases []string, postLoadFile string, keyspace string, dbInfo externalDbInfo) string {
	targetTab := tabletAliases[0]
	schemaFileName := fmt.Sprintf("%s_schema_file.sql", keyspace)
	externalDb := "0"

	if dbInfo.dbName != "" {
		schemaFileName = ""
		externalDb = "1"
	}

	// Formatting for list in yaml
	for i, tabletId := range tabletAliases {
		//tabletAliases[i] = "\"vttablet" + tabletId + "\""
		tabletAliases[i] = "vttablet" + tabletId + ": " + "{condition : service_healthy}"
	}
	//dependsOn := "[" + strings.Join(tabletAliases, ", ") + "]"
	dependsOn := "depends_on: {" + strings.Join(tabletAliases, ", ") + "}"

	data := fmt.Sprintf(`
- op: add
  path: /services/schemaload_%[7]s
  value:
    image: vitess/base
    volumes:
      - ".:/script"
    environment:
      - TOPOLOGY_FLAGS=%[3]s
      - WEB_PORT=%[4]s
      - GRPC_PORT=%[5]s
      - CELL=%[6]s
      - KEYSPACE=%[7]s
      - TARGETTAB=%[6]s-0000000%[2]s
      - SLEEPTIME=15
      - VSCHEMA_FILE=%[7]s_vschema.json
      - SCHEMA_FILES=%[9]s
      - POST_LOAD_FILE=%[8]s
      - EXTERNAL_DB=%[10]s
    command: ["sh", "-c", "/script/schemaload.sh"]
    %[1]s
`, dependsOn, targetTab, *topologyFlags, *webPort, *gRpcPort, *cell, keyspace, postLoadFile, schemaFileName, externalDb)

	return data
}

func generatePrimaryVIndex(tableName, column string, name string) string {
	data := fmt.Sprintf(`
[{"op": "add",
"path": "/tables/%[1]s",
"value": 
  {"column_vindexes": [
    {
      "column": "%[2]s",
      "name":  "%[3]s"
    }
  ]}
}]
`, tableName, column, name)

	return data
}

func generateVschemaLookupHash(tableName, tableKeyspace, from, to, owner string) string {
	data := fmt.Sprintf(`
[{"op": "add",
"path": "/vindexes/%[1]s",
"value":
  {"type": "lookup_hash",
    "params": {
      "table": "%[2]s.%[1]s",
      "from": "%[3]s",
      "to": "%[4]s",
      "autocommit": "true"
    },
  "owner": "%[5]s"
  }
}]
`, tableName, tableKeyspace, from, to, owner)

	return data
}

func addToColumnVIndexes(tableName, column, referenceName string) string {
	data := fmt.Sprintf(`
[{"op": "add",
"path": "/tables/%[1]s/column_vindexes/-",
"value":
    {
      "column": "%[2]s",
      "name":  "%[3]s"
    }
}]
`, tableName, column, referenceName)

	return data
}
