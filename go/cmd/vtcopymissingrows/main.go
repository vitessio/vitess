package main

import (
	"fmt"

	"context"
	"flag"
	"os"
	"strings"
	"time"

	log "github.com/golang/glog"
	"vitess.io/vitess/go/bytes2"
	"vitess.io/vitess/go/exit"
	"vitess.io/vitess/go/sqlescape"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/topoproto"
	_ "vitess.io/vitess/go/vt/topo/zk2topo"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
	_ "vitess.io/vitess/go/vt/vttablet/grpctabletconn"
	_ "vitess.io/vitess/go/vt/vttablet/grpctmclient"
	"vitess.io/vitess/go/vt/vttablet/queryservice"
	"vitess.io/vitess/go/vt/vttablet/tabletconn"
	"vitess.io/vitess/go/vt/vttablet/tmclient"
)

var (
	cell               = flag.String("cell", "", "which cell to search for the MASTER tablet in")
	timeout            = flag.Duration("timeout", 2*time.Hour, "timeout for the copy process")
	dry                = flag.Bool("dry", true, "dry run")
	readFromTabletType = flag.String("read_from_tablet_type", "REPLICA", "which type of tablet to read data from")
)

var doc = `
Copies missing rows from one shard to another
`

func main() {
	defer exit.Recover()
	defer logutil.Flush()
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "%v <from shard> <to shard> <table>\n", os.Args[0])
		flag.PrintDefaults()
		fmt.Fprintf(os.Stderr, doc)
	}

	flag.Parse()
	args := flag.Args()
	if len(args) != 3 {
		flag.Usage()
		exit.Return(1)
	}

	ctx, cancelFunc := context.WithTimeout(context.Background(), *timeout)
	defer cancelFunc()

	sourceTarget, err := parseTarget(args[0])
	if err != nil {
		log.Fatalln(err)
	}

	// TODO ideally we'd just figure out which shard it belongs to and move it there
	destTarget, err := parseTarget(args[1])
	if err != nil {
		log.Fatalln(err)
	}

	if sourceTarget.Keyspace != destTarget.Keyspace {
		log.Fatalln("can only move between shards in the same keyspace")
	}

	tableName := args[2]

	ts := topo.Open()

	vschema, err := loadVSchema(ctx, ts, sourceTarget.Keyspace)
	if err != nil {
		log.Fatalln(err)
	}

	sourceQs, _, err := connect(ctx, ts, sourceTarget)
	if err != nil {
		log.Fatalln(err)
	}

	sourceReplicaTarget := *sourceTarget
	sourceReplicaTarget.TabletType, err = topoproto.ParseTabletType(*readFromTabletType)
	if err != nil {
		log.Fatalln(err)
	}
	sourceReplicaQs, _, err := connect(ctx, ts, &sourceReplicaTarget)
	if err != nil {
		log.Fatalln(err)
	}

	destTablet, err := getTablet(ctx, ts, destTarget)
	if err != nil {
		log.Fatalln(err)
	}
	destShard, err := ts.GetShard(ctx, destTarget.Keyspace, destTarget.Shard)
	if err != nil {
		log.Fatalln(err)
	}

	destReplicaTarget := *destTarget
	destReplicaTarget.TabletType, err = topoproto.ParseTabletType(*readFromTabletType)
	if err != nil {
		log.Fatalln(err)
	}
	destReplicaQs, _, err := connect(ctx, ts, &destReplicaTarget)
	if err != nil {
		log.Fatalln(err)
	}

	tabletManager := tmclient.NewTabletManagerClient()

	selectMaxPkSql := fmt.Sprintf("SELECT MAX(id) FROM %v", tableName)
	selectSql := fmt.Sprintf("SELECT * FROM %v", tableName)

	fieldResult, err := sourceQs.Execute(ctx, sourceTarget, selectSql+" WHERE 1=0", make(map[string]*query.BindVariable), 0, nil)
	if err != nil {
		log.Fatalln(err)
	}

	maxIdResult, err := sourceQs.Execute(ctx, sourceTarget, selectMaxPkSql, make(map[string]*query.BindVariable), 0, nil)
	if err != nil {
		log.Fatalln(err)
	}
	maxIdValue := maxIdResult.Rows[0][0]
	maxId := encodeSQL(maxIdValue)

	table := vschema.Tables[tableName]
	primaryVindex := table.ColumnVindexes[0]
	if len(primaryVindex.Columns) > 1 {
		log.Fatalln("can only handle vindex on single column")
	}

	if !primaryVindex.Vindex.IsUnique() {
		log.Fatalln("primary vindex have to be unqiue")
	}

	// TODO handle primary keys properly
	idColumnIndex := -1
	for i, field := range fieldResult.Fields {
		if field.Name == "id" {
			idColumnIndex = i
		}
	}
	if idColumnIndex == -1 {
		log.Fatalf("no column named 'id' in: %#v", fieldResult.Fields)
	}

	selectPkAndPrimaryVindexSql := fmt.Sprintf("SELECT id, %v FROM %v WHERE id <= %v", primaryVindex.Columns[0].Lowered(), tableName, maxId)

	selectPkSql := fmt.Sprintf("SELECT id FROM %v WHERE id <= %v", tableName, maxId)

	// This disables rowlimit and query timeout so we can process the entire table
	executeOptions := query.ExecuteOptions{Workload: query.ExecuteOptions_OLAP}

	var rowsInDest = make(map[string]bool)
	var destRowCount = 0
	log.Infoln(selectPkSql)
	err = destReplicaQs.StreamExecute(ctx, &destReplicaTarget, selectPkSql, make(map[string]*query.BindVariable), 0, &executeOptions, func(result *sqltypes.Result) error {
		if len(result.Rows) == 0 {
			return nil
		}

		destRowCount += len(result.Rows)

		for _, row := range result.Rows {
			idVal := row[0]
			rowsInDest[idVal.String()] = true
		}

		return nil
	})
	if err != nil {
		log.Fatal(err)
	}

	_, _, allShards, err := getKeyspaceShards(ctx, ts, sourceTarget.Keyspace, sourceTarget.TabletType)
	allShards = []*topodata.ShardReference{
		{Name: destShard.ShardName(), KeyRange: destShard.KeyRange},
	}

	var idsToCopy []sqltypes.Value
	var sourceRowCount = 0
	log.Infoln(selectPkAndPrimaryVindexSql)
	streamCtx, streamCancel := context.WithTimeout(context.Background(), *timeout)
	defer streamCancel()
	err = sourceReplicaQs.StreamExecute(streamCtx, &sourceReplicaTarget, selectPkAndPrimaryVindexSql, make(map[string]*query.BindVariable), 0, &executeOptions, func(result *sqltypes.Result) error {
		if len(result.Rows) == 0 {
			return nil
		}

		sourceRowCount += len(result.Rows)

		for _, row := range result.Rows {
			// Check that the row actually belongs to the destination shard
			primaryVindexVal := row[1]
			destination, err := primaryVindex.Vindex.Map(nil, []sqltypes.Value{primaryVindexVal})
			if err != nil {
				return err
			}
			err = destination[0].Resolve(allShards, func(shard string) error {
				idVal := row[0]
				_, ok := rowsInDest[idVal.String()]
				if ok {
					// We got it already
					return nil
				}

				log.Infof("row %v belongs to target shard. queueing up for copy", idVal)
				idsToCopy = append(idsToCopy, idVal)
				return nil
			})
			if err != nil {
				if strings.Contains(err.Error(), "didn't match any shards") {
					continue
				}
				return err
			}

			if len(idsToCopy) > 5000 {
				log.Infof("got 5000. will insert these and you have to run again")
				streamCancel()
				break
			}
		}

		return nil
	})
	if err != nil && !strings.Contains(err.Error(), "context canceled") {
		log.Fatal(err)
	}

	if len(idsToCopy) > 0 {
		log.Infof("copying %v rows", len(idsToCopy))

		selectWhereIdSql := selectSql + " WHERE id = :id"

		var insertCount uint64
		for _, id := range idsToCopy {
			// Load the row from the source
			log.Infoln(selectWhereIdSql)
			selectResult, err := sourceReplicaQs.Execute(ctx, &sourceReplicaTarget, selectWhereIdSql, map[string]*query.BindVariable{
				"id": sqltypes.ValueBindVariable(id),
			}, 0, nil)
			if err != nil {
				log.Fatalln(err)
			}
			row := selectResult.Rows[0]

			// Insert the row in the destination
			var values = make([]string, len(fieldResult.Fields))
			for i, value := range row {
				values[i] = encodeSQL(value)
			}
			insertSql := "INSERT INTO " + sqlescape.EscapeID(topoproto.TabletDbName(destTablet.Tablet)) + "." +
				sqlescape.EscapeID(tableName) + " (" + strings.Join(escapeAll(fieldResult.Fields), ", ") +
				") VALUES (" + strings.Join(values, ",") + ")"
			log.Infoln(insertSql)
			if *dry {
				log.Infoln("not running because dry")
				continue
			}

			// Have to use the TabletManager to insert the row because the MASTER tablet query service is disabled while running filtered replication
			insertResult, err := tabletManager.ExecuteFetchAsApp(ctx, destTablet.Tablet, true, []byte(insertSql), 10000)
			if err != nil {
				errMessage := fmt.Sprintf("%v", err)
				if !strings.Contains(errMessage, "Duplicate entry") {
					log.Fatalln(err)
				}
				if !strings.Contains(errMessage, "PRIMARY") {
					log.Fatalln(err)
				}
				log.Warningln(err)
			} else {
				if insertResult.RowsAffected != 1 {
					log.Fatalln("row was not inserted. something is wrong. aborting")
				}
				insertCount += insertResult.RowsAffected
			}
		}

		log.Infof("inserted %v rows", insertCount)
	} else {
		log.Infoln("nothing to copy. done")
	}
}

func encodeSQL(value sqltypes.Value) string {
	b := bytes2.Buffer{}
	value.EncodeSQL(&b)
	maxId := b.String()
	return maxId
}

func connect(ctx context.Context, ts *topo.Server, target *query.Target) (queryservice.QueryService, *topodata.Tablet, error) {
	tabletInfo, err := getTablet(ctx, ts, target)
	if err != nil {
		return nil, nil, err
	}
	queryService, err := tabletconn.GetDialer()(tabletInfo.Tablet, true)
	if err != nil {
		return nil, nil, err
	}
	return queryService, tabletInfo.Tablet, nil
}

func getTablet(ctx context.Context, ts *topo.Server, target *query.Target) (*topo.TabletInfo, error) {
	var tabletInfos map[string]*topo.TabletInfo
	var err error
	if *cell != "" {
		tabletInfos, err = ts.GetTabletMapForShardByCell(ctx, target.Keyspace, target.Shard, []string{*cell})
		if err != nil {
			return nil, err
		}
	} else {
		tabletInfos, err = ts.GetTabletMapForShard(ctx, target.Keyspace, target.Shard)
		if err != nil {
			return nil, err
		}
	}
	for _, tabletInfo := range tabletInfos {
		if tabletInfo.Tablet.Type == target.TabletType {
			return tabletInfo, nil
		}
	}
	return nil, fmt.Errorf("could not find tablet matching: %v", target)
}

func escapeAll(identifiers []*query.Field) []string {
	result := make([]string, len(identifiers))
	for i := range identifiers {
		result[i] = sqlescape.EscapeID(identifiers[i].Name)
	}
	return result
}

func parseTarget(targetString string) (*query.Target, error) {
	// Default tablet type is master.
	target := &query.Target{
		TabletType: topodata.TabletType_MASTER,
	}
	last := strings.LastIndexAny(targetString, "@")
	if last != -1 {
		// No need to check the error. UNKNOWN will be returned on
		// error and it will fail downstream.
		tabletType, err := topoproto.ParseTabletType(targetString[last+1:])
		if err != nil {
			return target, err
		}
		target.TabletType = tabletType
		targetString = targetString[:last]
	}
	last = strings.LastIndexAny(targetString, "/:")
	if last != -1 {
		target.Shard = targetString[last+1:]
		targetString = targetString[:last]
	}
	target.Keyspace = targetString
	if target.Keyspace == "" {
		return target, fmt.Errorf("no keyspace in: %v", targetString)
	}
	if target.Shard == "" {
		return target, fmt.Errorf("no shard in: %v", targetString)
	}
	return target, nil
}

func loadVSchema(ctx context.Context, ts *topo.Server, keyspace string) (*vindexes.KeyspaceSchema, error) {
	kschema, err := ts.GetVSchema(ctx, keyspace)
	if err != nil {
		return nil, fmt.Errorf("cannot load VSchema for keyspace %v: %v", keyspace, err)
	}
	if kschema == nil {
		return nil, fmt.Errorf("no VSchema for keyspace %v", keyspace)
	}

	keyspaceSchema, err := vindexes.BuildKeyspaceSchema(kschema, keyspace)
	if err != nil {
		return nil, fmt.Errorf("cannot build vschema for keyspace %v: %v", keyspace, err)
	}
	return keyspaceSchema, nil
}

func getKeyspaceShards(ctx context.Context, ts *topo.Server, keyspace string, tabletType topodata.TabletType) (string, *topodata.SrvKeyspace, []*topodata.ShardReference, error) {
	srvKeyspace, err := ts.GetSrvKeyspace(ctx, *cell, keyspace)
	if err != nil {
		return "", nil, nil, fmt.Errorf("keyspace %v fetch error: %v", keyspace, err)
	}

	// check if the keyspace has been redirected for this tabletType.
	for _, sf := range srvKeyspace.ServedFrom {
		if sf.TabletType == tabletType {
			keyspace = sf.Keyspace
			srvKeyspace, err = ts.GetSrvKeyspace(ctx, *cell, keyspace)
			if err != nil {
				return "", nil, nil, fmt.Errorf("keyspace %v fetch error: %v", keyspace, err)
			}
		}
	}

	partition := topoproto.SrvKeyspaceGetPartition(srvKeyspace, tabletType)
	if partition == nil {
		return "", nil, nil, fmt.Errorf("no partition found for tabletType %v in keyspace %v", topoproto.TabletTypeLString(tabletType), keyspace)
	}
	return keyspace, srvKeyspace, partition.ShardReferences, nil
}
