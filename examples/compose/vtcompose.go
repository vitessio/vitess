package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	jsonpatch "github.com/evanphx/json-patch"
	"io/ioutil"
	"math"
	"os"
	"regexp"
	"strconv"
	"strings"

	"github.com/krishicks/yaml-patch"
	"vitess.io/vitess/go/vt/log"
)

var (
	baseYamlFile    = flag.String("base_yaml", "docker-compose.base.yml", "Starting docker-compose yaml")
	baseVschemaFile = "base_vschema.json"
	finalVschemaFile = "vschema.json"

	schemaFiles = flag.String("schemaFiles", "create_messages.sql,create_tokens.sql", "Tables to create")
	replicaTablets = flag.Int("replicaTablets", 1, "Number of replica tablets")
	readOnlyTablets = flag.Int("readOnlyTablets", 0, "Number of read only tablets")
	shards = flag.Int("shards", 0, "How many shards to configure (0 is unsharded)")
	lookupSchemaFiles = flag.String("lookupSchemaFiles", "create_messages_message_lookup.sql,create_tokens_token_lookup.sql", "Lookup Tables to create")
	useLookups = flag.Bool("useLookups", false, "Whether to use lookup tables or not")

	topologyFlags = flag.String("topologyFlags",
		"-topo_implementation consul -topo_global_server_address consul1:8500 -topo_global_root vitess/global",
		"Vitess Topology Flags config")
	webPort   = flag.String("webPort", "8080", "Web port to be used")
	gRpcPort  = flag.String("gRpcPort", "15999", "gRPC port to be used")
	mySqlPort = flag.String("mySqlPort", "15306", "mySql port to be used")
	cell      = flag.String("cell", "test", "Vitess Cell name")
	primaryKeyspace  = flag.String("keyspace", "test_keyspace", "Name of primary keyspace to use")
)

func main() {
	primaryTableColumns := make(map[string]string)
	flag.Parse()
	schemaFileNames := strings.Split(*schemaFiles, ",")
	lookupSchemaFileNames := strings.Split(*lookupSchemaFiles, ",")

	f := createFile("tables/schema_file.sql")
	appendtoSqlFile(schemaFileNames, f)
	if *useLookups {
		appendtoSqlFile(lookupSchemaFileNames, f)
	}
	closeFile(f)

	// Vschema Patching
	vSchemaFile := readFile(baseVschemaFile)

	if *shards == 0 {
		vSchemaFile = applyJsonInMemoryPatch(vSchemaFile,`[{"op": "replace","path": "/sharded", "value": false}]`)
	}

	vSchemaFile, primaryTableColumns =  addTablesVschemaPatch(vSchemaFile, schemaFileNames)
	if *useLookups {
		vSchemaFile = addLookupDataToVschema(vSchemaFile, lookupSchemaFileNames, primaryTableColumns, "test_keyspace")
	}

	writeVschemaFile(vSchemaFile, "vschema.json")

	// Docker Compose File Patches
	dockerFile := readFile(*baseYamlFile)
  dockerFile = applyDockerComposePatches(dockerFile)
	writeFile(dockerFile, "docker-compose.yml")
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
	fmt.Println(patchString)
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
	fmt.Println(patchString)
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
		data, err := ioutil.ReadFile("tables/" + file)
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
		tableName := getTableName("tables/" + fileName)
		indexedColumns = getPrimaryKey("tables/" + fileName)
		firstColumnName := strings.Split(indexedColumns, ", ")[0]
		vSchemaFile = applyJsonInMemoryPatch(vSchemaFile, generatePrimaryVIndex(tableName, firstColumnName, "hash"))
		primaryTableColumns[tableName] = firstColumnName
	}

	return vSchemaFile, primaryTableColumns
}

func addLookupDataToVschema(vSchemaFile []byte, schemaFileNames []string, primaryTableColumns map[string]string, keyspace string) []byte {
	for _, fileName := range schemaFileNames {
		tableName := fileName[7:len(fileName)-4]
		lookupTableOwner := ""

		// Find owner of lookup table
		for primaryTableName, _ := range primaryTableColumns {
			if strings.HasPrefix(tableName, primaryTableName) && len(primaryTableName) > len(lookupTableOwner) {
				lookupTableOwner = primaryTableName
			}
		}

		indexedColumns := getKeyColumns("tables/" + fileName)
		firstColumnName := strings.Split(indexedColumns, ", ")[0]

		// Lookup patch under "tables"
		vSchemaFile = applyJsonInMemoryPatch(vSchemaFile, generatePrimaryVIndex(tableName, firstColumnName, "hash"))
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

func writeFile (file []byte, fileName string) {
	err := ioutil.WriteFile(fileName, file, 0644)
	if err != nil {
		log.Fatalf("writing %s %s", fileName, err)
	}
}

func applyKeyspaceDependentPatches(dockerFile []byte, keyspace string) []byte {
	tabAlias := 0
	shard := "-"
	masterTablets := []string{"101"}
	numShards := *shards
	interval := int(math.Floor(256 / float64(numShards)))

	for i:=1; i < numShards; i++ {
		masterTablets = append(masterTablets, strconv.Itoa((i+1)*100+1))
	}

	dockerFile = applyInMemoryPatch(dockerFile, generateSchemaload(masterTablets, finalVschemaFile, "", keyspace))

	// Append Master and Replica Tablets
	if numShards < 2 {
		tabAlias = tabAlias + 100
		dockerFile = applyTabletPatches(dockerFile, tabAlias, shard, keyspace)
	} else {
		for i:=0; i < numShards; i++ {
			if i == 0 {
				shard = fmt.Sprintf("-%x", interval)
			} else if i == (numShards - 1) {
				shard = fmt.Sprintf("%x-", interval*i)
			} else {
				shard = fmt.Sprintf("%x-%x", interval*(i) ,interval*(i+1))
			}
			tabAlias = tabAlias + 100
			dockerFile = applyTabletPatches(dockerFile, tabAlias, shard, keyspace)
		}
	}

	return dockerFile
}

func applyDefaultDockerPatches(dockerFile []byte) []byte {
	dockerFile = applyInMemoryPatch(dockerFile, generateVtctld())
	dockerFile = applyInMemoryPatch(dockerFile, generateVtgate())
	dockerFile = applyInMemoryPatch(dockerFile, generateVtwork())
	return dockerFile
}

func applyDockerComposePatches(dockerFile []byte) []byte {
	keyspace := *primaryKeyspace

	//Vtctld, vtgate, vtwork, schemaload patches
	dockerFile = applyDefaultDockerPatches(dockerFile)
	dockerFile = applyKeyspaceDependentPatches(dockerFile, keyspace)

	return dockerFile
}

func applyTabletPatches(dockerFile []byte, tabAlias int, shard string, keyspace string) []byte {
	dockerFile = applyInMemoryPatch(dockerFile, generateDefaultTablet(strconv.Itoa(tabAlias+1), shard, "master", keyspace))
	for i:=0; i < *replicaTablets; i++ {
		dockerFile = applyInMemoryPatch(dockerFile, generateDefaultTablet(strconv.Itoa(tabAlias+ 2 + i), shard, "replica", keyspace))
	}
	return dockerFile
}

// Default Tablet
func generateDefaultTablet(tabAlias, shard, role, keyspace string) string {
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
    command: ["sh", "-c", "/script/vttablet-up.sh %[1]s"]
    depends_on:
      - vtctld
`, tabAlias, shard, role, *webPort, *gRpcPort, keyspace, *topologyFlags, *cell)

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
        -web_dir $$VTTOP/web/vtctld \
        -web_dir2 $$VTTOP/web/vtctld2/app \
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

func generateSchemaload(tabletAliases []string, vschemaFile string, postLoadFile string, keyspace string) string {
	targetTab := tabletAliases[0]

	// Formatting for list in yaml
	for i, tabletId := range tabletAliases {
		tabletAliases[i] = "\"vttablet" + tabletId + "\""
	}
	dependsOn := "[" + strings.Join(tabletAliases, ", ") + "]"

	data := fmt.Sprintf(`
- op: add
  path: /services/schemaload
  value:
    image: vitess/base
    volumes:
      - ".:/script"
    environment:
      - TOPOLOGY_FLAGS=%[4]s
      - WEB_PORT=%[5]s
      - GRPC_PORT=%[6]s
      - CELL=%[7]s
      - KEYSPACE=%[8]s
      - TARGETTAB=test-0000000%[2]s
      - SLEEPTIME=15
      - VSCHEMA_FILE=%[3]s
      - SCHEMA_FILES=schema_file.sql
      - POST_LOAD_FILE=%[9]s
    command: ["sh", "-c", "/script/schemaload.sh"]
    depends_on: %[1]s
`, dependsOn, targetTab, vschemaFile, *topologyFlags, *webPort, *gRpcPort, *cell, keyspace, postLoadFile)

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