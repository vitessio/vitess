package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"vitess.io/vitess/go/trace"
	"vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"

	"vitess.io/vitess/go/vt/vttablet/tmclient"

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
)

const (
	MaxRows = 50000
)

var (
	tableArg           = flag.String("table", "", "the table to copy")
	shardingKeyColumn  = flag.String("sharding_key_column", "", "the column name of the sharding key")
	sourceKeysapceArg  = flag.String("source_keyspace", "", "the source keyspace the table is copied from")
	targetKeyspaceArg  = flag.String("target_keyspace", "", "the target keyspace the table will be copied to")
	targetVindexName   = flag.String("target_vindex", "hash", "the name of the primary vindex in the target keyspace")
	cell               = flag.String("cell", "", "which cell to search for the MASTER tablet in")
	timeout            = flag.Duration("timeout", 2*time.Hour, "timeout for the copy process")
	dry                = flag.Bool("dry", true, "dry run")
	copyFromTabletType = flag.String("read_from_tablet_type", "REPLICA", "which type of tablet to read data from")
)

var (
	tabletManager = tmclient.NewTabletManagerClient()
	tabletMap     = make(map[string]*topo.TabletInfo)
)

var doc = `
Copies a table from an unsharded keyspace to another keyspace
`

func main() {
	defer exit.Recover()
	defer logutil.Flush()
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "%v <from keyspace> <to keyspace> <table>\n", os.Args[0])
		flag.PrintDefaults()
		fmt.Fprintf(os.Stderr, doc)
	}

	flag.Parse()

	closer := trace.StartTracing("vtcopytable")
	defer trace.LogErrorsWhenClosing(closer)

	tableName := *tableArg
	if tableName == "" || *sourceKeysapceArg == "" || *targetKeyspaceArg == "" {
		flag.Usage()
		exit.Return(1)
	}

	ctx, cancelFunc := context.WithTimeout(context.Background(), *timeout)
	defer cancelFunc()
	topoServer := topo.Open()

	tabletType, err := topoproto.ParseTabletType(*copyFromTabletType)
	logAndExitIfError(err)

	result := selectRows(ctx, topoServer, tableName, tabletType)
	insertRowsInShards(ctx, topoServer, tableName, tabletType, result)
}

func insertRowsInShards(ctx context.Context, topoServer *topo.Server, tableName string, tabletType topodata.TabletType, result *query.QueryResult) {
	targetKeyspace, err := getKeyspace(ctx, topoServer, *targetKeyspaceArg)
	logAndExitIfError(err)

	_, _, allShards, err := getKeyspaceShards(ctx, topoServer, targetKeyspace.KeyspaceName(), tabletType)
	logAndExitIfError(err)

	vschema, err := loadVSchema(ctx, topoServer, targetKeyspace.KeyspaceName())
	logAndExitIfError(err)

	for _, row := range result.Rows {
		var values = make([]string, len(result.Fields))
		var offset int64
		var destTablet *topo.TabletInfo
		for i, field := range result.Fields {
			var val []byte
			typ := field.Type
			if row.Lengths[i] > 0 {
				val = row.Values[offset : offset+row.Lengths[i]]
			}

			value, err := sqltypes.NewValue(typ, val)
			logAndExitIfError(err)
			values[i] = encodeSQL(value)

			if field.Name == *shardingKeyColumn {
				destTablet, err = getDestinationTablet(ctx, topoServer, value, allShards, targetKeyspace, vschema)
			}

			offset += row.Lengths[i]
		}

		insertSql := fmt.Sprintf("INSERT INTO %s.%s (%s) VALUES (%s)",
			sqlescape.EscapeID(topoproto.TabletDbName(destTablet.Tablet)),
			sqlescape.EscapeID(tableName),
			strings.Join(escapeAll(result.Fields), ", "),
			strings.Join(values, ","))

		log.Infoln(insertSql)
		if *dry {
			log.Infoln("not running because dry")
			continue
		}

		fmt.Printf("Inserting row %s:%s %s:%s to shard %s/%s", result.Fields[0].Name, values[0],
			result.Fields[1].Name, values[1], destTablet.Keyspace, destTablet.Shard)
		_, err := tabletManager.ExecuteFetchAsDba(ctx, destTablet.Tablet, true, []byte(insertSql), MaxRows, true, false)
		logAndExitIfError(err)
	}
}

func getDestinationTablet(ctx context.Context, topoServer *topo.Server, value sqltypes.Value, targetShards []*topodata.ShardReference, targetKeyspace *topo.KeyspaceInfo, vschema *vindexes.KeyspaceSchema) (*topo.TabletInfo, error) {
	destinations, err := vschema.Vindexes[*targetVindexName].Map(nil, []sqltypes.Value{value})
	logAndExitIfError(err)

	var destTablet *topo.TabletInfo
	err = destinations[0].Resolve(targetShards, func(shard string) error {
		shardId := fmt.Sprintf("%s/%s", targetKeyspace.KeyspaceName(), shard)
		if dt, ok := tabletMap[shardId]; ok {
			destTablet = dt
			return nil
		} else {
			destTarget, err := parseTarget(shardId)
			if err != nil {
				return err
			}
			dt, err := getTablet(ctx, topoServer, destTarget)
			if err != nil {
				return err
			}

			tabletMap[shardId] = dt
			destTablet = dt
			return nil
		}
	})

	if err != nil {
		return nil, err
	}
	return destTablet, nil
}

func selectRows(ctx context.Context, topoServer *topo.Server, tableName string, tabletType topodata.TabletType) *query.QueryResult {
	sourceKeyspace, err := getKeyspace(ctx, topoServer, *sourceKeysapceArg)
	logAndExitIfError(err)

	_, _, shardReferences, err := getKeyspaceShards(ctx, topoServer, sourceKeyspace.KeyspaceName(), tabletType)
	logAndExitIfError(err)

	numberOfShards := len(shardReferences)
	if numberOfShards != 1 {
		log.Fatalln("Source keyspace must be unsharded. Keyspace %s has %d shards", sourceKeyspace.KeyspaceName(), numberOfShards)
	}

	sourceTarget, err := parseTarget(fmt.Sprintf("%s/%s", *sourceKeysapceArg, shardReferences[0].Name))
	logAndExitIfError(err)
	sourceTarget.TabletType = tabletType

	sourceShardName := shardReferences[0].Name
	tabletAliases, err := topoServer.FindAllTabletAliasesInShardByCell(ctx, *sourceKeysapceArg, sourceShardName, []string{*cell})
	logAndExitIfError(err)

	sourceTabletAlias := tabletAliases[0]
	sourceTablet, err := topoServer.GetTablet(ctx, sourceTabletAlias)
	logAndExitIfError(err)

	selectSql := fmt.Sprintf("SELECT * FROM %s", tableName)
	result, err := tabletManager.ExecuteFetchAsDba(ctx, sourceTablet.Tablet, true, []byte(selectSql), MaxRows, true, false)
	logAndExitIfError(err)

	return result
}

func getKeyspace(ctx context.Context, topoServer *topo.Server, keyspace string) (*topo.KeyspaceInfo, error) {
	fmt.Fprintf(os.Stdout, "Retrievieing keyspace info %s\n", keyspace)
	sourceKeyspace, err := topoServer.GetKeyspace(ctx, keyspace)
	if err != nil {
		log.Fatalln(err)
	}
	fmt.Fprintf(os.Stdout, "Found source keyspace %s\n", sourceKeyspace.KeyspaceName())
	return sourceKeyspace, err
}

func logAndExitIfError(err error) {
	if err != nil {
		log.Fatalln(err)
	}
}

func encodeSQL(value sqltypes.Value) string {
	b := bytes2.Buffer{}
	value.EncodeSQL(&b)
	maxId := b.String()
	return maxId
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
	return nil, vterrors.Errorf(vtrpc.Code_NOT_FOUND, "could not find tablet matching: %v", target)
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
		return target, vterrors.Errorf(vtrpc.Code_NOT_FOUND, "no keyspace in: %v", targetString)
	}
	if target.Shard == "" {
		return target, vterrors.Errorf(vtrpc.Code_NOT_FOUND, "no shard in: %v", targetString)
	}
	return target, nil
}

func loadVSchema(ctx context.Context, ts *topo.Server, keyspace string) (*vindexes.KeyspaceSchema, error) {
	kschema, err := ts.GetVSchema(ctx, keyspace)
	if err != nil {
		return nil, vterrors.Errorf(vtrpc.Code_UNKNOWN, "cannot load VSchema for keyspace %v: %v", keyspace, err)
	}
	if kschema == nil {
		return nil, vterrors.Errorf(vtrpc.Code_NOT_FOUND, "no VSchema for keyspace %v", keyspace)
	}

	keyspaceSchema, err := vindexes.BuildKeyspaceSchema(kschema, keyspace)
	if err != nil {
		return nil, vterrors.Errorf(vtrpc.Code_UNKNOWN, "cannot build vschema for keyspace %v: %v", keyspace, err)
	}
	return keyspaceSchema, nil
}

func getKeyspaceShards(ctx context.Context, ts *topo.Server, keyspace string, tabletType topodata.TabletType) (string, *topodata.SrvKeyspace, []*topodata.ShardReference, error) {
	srvKeyspace, err := ts.GetSrvKeyspace(ctx, *cell, keyspace)
	if err != nil {
		return "", nil, nil, vterrors.Errorf(vtrpc.Code_UNKNOWN, "keyspace %v fetch error: %v", keyspace, err)
	}

	// check if the keyspace has been redirected for this tabletType.
	for _, sf := range srvKeyspace.ServedFrom {
		if sf.TabletType == tabletType {
			keyspace = sf.Keyspace
			srvKeyspace, err = ts.GetSrvKeyspace(ctx, *cell, keyspace)
			if err != nil {
				return "", nil, nil, vterrors.Errorf(vtrpc.Code_UNKNOWN, "keyspace %v fetch error: %v", keyspace, err)
			}
		}
	}

	partition := topoproto.SrvKeyspaceGetPartition(srvKeyspace, tabletType)
	if partition == nil {
		return "", nil, nil, vterrors.Errorf(vtrpc.Code_NOT_FOUND, "no partition found for tabletType %v in keyspace %v", topoproto.TabletTypeLString(tabletType), keyspace)
	}
	return keyspace, srvKeyspace, partition.ShardReferences, nil
}
