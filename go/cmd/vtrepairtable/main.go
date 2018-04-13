package main

import (
	"fmt"

	"context"
	"flag"
	"os"
	"strings"
	"time"

	log "github.com/golang/glog"
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
	"vitess.io/vitess/go/vt/vttablet/queryservice"
	"vitess.io/vitess/go/vt/vttablet/tabletconn"
)

var (
	cell                           = flag.String("cell", "", "which cell to search for the MASTER tablet in")
	timeout                        = flag.Duration("deadline", 5*time.Minute, "timeout for the copy process")
	dry                            = flag.Bool("dry", true, "dry run")
	ignoreDuplicateEntryConstraint = flag.String("ignore_duplicate_entry", "", "ignore the named DuplicateEntry constraint when inserting (NOTE: Will still delete the row in the source shard so may cause data loss unless used with care)")
	readFromTabletType             = flag.String("read_from_tablet_type", "REPLICA", "which type of tablet to read data from")

	committed = false
)

var doc = `
Moves rows from one shard to another. Works non-transactionally like this:
select all the rows from the source
for each row:
  insert row with autocommit
  delete row with autocommit
If there is a failure it will stop working but will not roll back any of the work so far.
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

	destShard, err := ts.GetShard(ctx, destTarget.Keyspace, destTarget.Shard)
	if err != nil {
		log.Fatalln(err)
	}

	destQs, _, err := connect(ctx, ts, destTarget)
	if err != nil {
		log.Fatalln(err)
	}

	selectSql := fmt.Sprintf("SELECT * FROM %v", tableName)
	log.Infoln(selectSql)

	fieldResult, err := sourceQs.Execute(ctx, sourceTarget, selectSql+" WHERE 1=0", make(map[string]*query.BindVariable), 0, nil)
	if err != nil {
		log.Fatalln(err)
	}

	var fieldVars = make([]string, len(fieldResult.Fields))
	for i, field := range fieldResult.Fields {
		if field.Name == "updated_at" {
			fieldVars[i] = "now()"
		} else {
			fieldVars[i] = ":" + field.Name
		}
	}
	insertSql := "INSERT INTO " + sqlescape.EscapeID(tableName) +
		" (" + strings.Join(escapeAll(fieldResult.Fields), ", ") + ") VALUES (" +
		strings.Join(fieldVars, ",") + ")"
	// TODO handle primary keys properly
	deleteSql := "DELETE FROM " + sqlescape.EscapeID(tableName) + " WHERE id = :id"

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
	primaryVindexedColumn := primaryVindex.Columns[0].Lowered()
	primaryVindexColumIndex := -1
	for i, field := range fieldResult.Fields {
		if field.Name == "id" {
			idColumnIndex = i
		}
		if field.Name == primaryVindexedColumn {
			primaryVindexColumIndex = i
		}
	}
	if idColumnIndex == -1 {
		log.Fatalf("no column named 'id' in: %#v", fieldResult.Fields)
	}
	if primaryVindexColumIndex == -1 {
		log.Fatalf("could not find primary vindexed column '%v' in: %#v", primaryVindexedColumn, fieldResult.Fields)
	}

	_, _, allShards, err := getKeyspaceShards(ctx, ts, sourceTarget.Keyspace, sourceTarget.TabletType)

	var rowsToMove [][]sqltypes.Value
	var rowCount = 0
	// This disables rowlimit and query timeout so we can process the entire table
	executeOptions := query.ExecuteOptions{Workload: query.ExecuteOptions_OLAP}
	err = sourceReplicaQs.StreamExecute(ctx, &sourceReplicaTarget, selectSql, make(map[string]*query.BindVariable), 0, &executeOptions, func(result *sqltypes.Result) error {
		if len(result.Rows) == 0 {
			return nil
		}

		rowCount += len(result.Rows)

		for _, row := range result.Rows {
			// Check that the row actually belongs to the destination shard
			primaryVindexVal := row[primaryVindexColumIndex]
			destination, err := primaryVindex.Vindex.Map(nil, []sqltypes.Value{primaryVindexVal})
			if err != nil {
				return err
			}
			err = destination[0].Resolve(allShards, func(shard string) error {
				if destShard.ShardName() != shard {
					// Not the correct shard
					return nil
				}

				idVal := row[idColumnIndex]

				log.Infof("row %v belongs to target shard. queueing up for move", idVal)

				rowsToMove = append(rowsToMove, row)
				return nil
			})
		}

		log.Infof("processed %v rows", rowCount)
		return nil
	})
	if err != nil {
		log.Fatal(err)
	}

	if len(rowsToMove) > 0 {
		log.Infof("moving %v rows", len(rowsToMove))

		sourceTx, err := sourceQs.Begin(ctx, sourceTarget, nil)
		if err != nil {
			log.Fatal(err)
		}
		defer maybeRollback(sourceQs, sourceTarget, sourceTx)

		destTx, err := destQs.Begin(ctx, destTarget, nil)
		if err != nil {
			log.Fatalln(err)
		}
		defer maybeRollback(destQs, destTarget, destTx)

		var insertCount uint64
		var deleteCount uint64
		for _, row := range rowsToMove {
			// Insert the row in the source
			insertVars := make(map[string]*query.BindVariable)
			for i, field := range fieldResult.Fields {
				if field.Name != "updated_at" {
					insertVars[field.Name] = sqltypes.ValueBindVariable(row[i])
				}
			}

			log.Infoln(insertSql)
			insertResult, err := destQs.Execute(ctx, destTarget, insertSql, insertVars, destTx, nil)
			if err != nil {
				if *ignoreDuplicateEntryConstraint == "" {
					log.Fatalln(err)
				} else {
					// Handle the Duplicate entry constraint
					errMessage := fmt.Sprintf("%v", err)
					if !strings.Contains(errMessage, "code = AlreadyExists") {
						log.Fatalln(err)
					}
					if !strings.Contains(errMessage, *ignoreDuplicateEntryConstraint) {
						log.Fatalln(err)
					}
					log.Warningf("ignoring constraint violation: %v", err)
				}
			} else {
				if insertResult.RowsAffected != 1 {
					log.Fatalln("row was not inserted. something is wrong. aborting and rolling back")
				}
				insertCount += insertResult.RowsAffected
			}

			// Delete the row in the source
			idVal := row[idColumnIndex]
			deleteVars := map[string]*query.BindVariable{"id": sqltypes.ValueBindVariable(idVal)}

			log.Infoln(deleteSql)
			deleteResult, err := sourceQs.Execute(ctx, sourceTarget, deleteSql, deleteVars, sourceTx, nil)
			if err != nil {
				log.Fatalln(err)
			}
			if deleteResult.RowsAffected != 1 {
				log.Fatalln("row was not inserted. something is wrong. aborting and rolling back")
			}
			deleteCount += deleteResult.RowsAffected
		}

		log.Infof("inserted %v rows", insertCount)
		log.Infof("deleted %v rows", deleteCount)

		if *dry {
			log.Infof("dry run. rolling back")
			destQs.Rollback(ctx, destTarget, destTx)
			sourceQs.Rollback(ctx, sourceTarget, sourceTx)
		} else {
			log.Infof("committing")
			// Commit the destination first so we don't "drop" the rows
			destQs.Commit(ctx, destTarget, destTx)
			sourceQs.Commit(ctx, sourceTarget, sourceTx)
		}
		committed = true
	} else {
		log.Infoln("nothing to move. done")
	}
}

func maybeRollback(qs queryservice.QueryService, target *query.Target, tx int64) {
	if !committed {
		// TODO handle failures properly
		log.Infof("rolling back %v", target)
		qs.Rollback(context.Background(), target, tx)
	}
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
