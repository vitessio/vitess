package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"time"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/discovery"
	"vitess.io/vitess/go/vt/log"
	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	logutilpb "vitess.io/vitess/go/vt/proto/logutil"
	"vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/topo"
	_ "vitess.io/vitess/go/vt/topo/etcd2topo"
	_ "vitess.io/vitess/go/vt/vtgate/grpcvtgateconn"
	_ "vitess.io/vitess/go/vt/vttablet/grpctabletconn"

	_ "vitess.io/vitess/go/vt/vtctl/grpcvtctlclient"
	"vitess.io/vitess/go/vt/vtctl/vtctlclient"
	"vitess.io/vitess/go/vt/vtgate/vtgateconn"
)

type RowLogConfig struct {
	ids, cells []string

	sourceKeyspace, targetKeyspace, table, vtgate, vtctld, pk string
}

func (rlc *RowLogConfig) String() string {
	s := fmt.Sprintf("\tsource:%s, target:%s, table:%s, ids:%s, pk:%s\n",
		rlc.sourceKeyspace, rlc.targetKeyspace, rlc.table, strings.Join(rlc.ids, ","), rlc.pk)
	s += fmt.Sprintf("\tvtgate:%s, vtctld:%s, cells:%s", rlc.vtgate, rlc.vtctld, strings.Join(rlc.cells, ","))
	return s
}

func main() {
	defer log.Flush()
	ctx := context.Background()
	config := parseCommandLine()
	log.Infof("Starting rowlogger with config: %s", config)
	fmt.Printf("Starting rowlogger with\n%v\n", config)
	ts := topo.Open()
	sourceTablet := getTablet(ctx, ts, config.cells, config.sourceKeyspace)
	targetTablet := getTablet(ctx, ts, config.cells, config.targetKeyspace)
	log.Infof("Using tablets %s and %s to get positions", sourceTablet, targetTablet)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		startStreaming(ctx, config.vtgate, config.vtctld, config.sourceKeyspace, sourceTablet, config.table, config.pk, config.ids)
		log.Infof("rowlog done streaming from source")
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		startStreaming(ctx, config.vtgate, config.vtctld, config.targetKeyspace, targetTablet, config.table, config.pk, config.ids)
		log.Infof("rowlog done streaming from target")
	}()
	wg.Wait()
	log.Infof("rowlog done streaming from both source and target")
	fmt.Printf("done\n")
}

func startStreaming(ctx context.Context, vtgate, vtctld, keyspace, tablet, table, pk string, ids []string) {
	startPos, stopPos := getPositions(ctx, vtctld, keyspace, tablet)
	log.Infof("Streaming keyspace %s from %s upto %s", keyspace, startPos, stopPos)
	fmt.Printf("Streaming keyspace %s from %s upto %s\n", keyspace, startPos, stopPos)
	vgtid := &binlogdatapb.VGtid{
		ShardGtids: []*binlogdatapb.ShardGtid{{
			Keyspace: keyspace,
			Shard:    "0",
			Gtid:     startPos,
		}},
	}
	filter := &binlogdatapb.Filter{
		Rules: []*binlogdatapb.Rule{{
			Match:  table,
			Filter: "select * from " + table,
		}},
		FieldEventMode: 1,
	}
	conn, err := vtgateconn.Dial(ctx, vtgate)
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()
	reader, err := conn.VStream(ctx, topodatapb.TabletType_MASTER, vgtid, filter)
	var fields []*query.Field
	var gtid string
	var plan *TablePlan
	var lastLoggedAt int64
	for {
		evs, err := reader.Recv()
		//fmt.Printf("events received: %d\n",len(evs))
		switch err {
		case nil:
			for _, ev := range evs {
				now := time.Now().Unix()
				if now-lastLoggedAt > 1 && ev.Timestamp != 0 { // log progress every ten seconds
					lastLoggedAt = now
					log.Infof("%s Progress: %s: %s", keyspace, time.Unix(ev.Timestamp, 0).Format(time.RFC3339), gtid)
					fmt.Printf(".")
				}
				switch ev.Type {
				case binlogdatapb.VEventType_VGTID:
					gtid = ev.Vgtid.ShardGtids[0].Gtid
					//fmt.Printf("gtid %s\n", gtid)
				case binlogdatapb.VEventType_FIELD:
					fields = ev.FieldEvent.Fields
					//fmt.Printf("field %s\n", fields)
					plan = getTablePlan(keyspace, fields, ev.FieldEvent.TableName, pk, ids)
					outputHeader(plan)
				case binlogdatapb.VEventType_ROW:
					rows := processRowEvent(plan, gtid, ev)
					if len(rows) > 0 {
						//fmt.Printf("#rows %d\n", len(rows))
						outputRows(plan, rows)
					}
				default:
					//fmt.Printf("event type %v\n",ev.Type)
				}
			}
			if gtid == stopPos {
				return
			}
		case io.EOF:
			log.Infof("stream ended")
			fmt.Printf("stream ended\n")
			return
		default:
			log.Errorf("remote error: %s", err)
			fmt.Printf("remote error: %s\n", err.Error())
			return
		}
	}
	log.Infof("Finished streaming keyspace %s from %s upto %s", keyspace, startPos, stopPos)
}

func output(filename, s string) {
	f, err := os.OpenFile(filename+".log",
		os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Errorf(err.Error())
	}
	defer f.Close()
	if _, err := f.WriteString(s + "\n"); err != nil {
		log.Errorf(err.Error())
	}
	log.Infof("Writing to %s.log: %s", filename, s)
}

func outputHeader(plan *TablePlan) {
	s := getHeader(plan)
	output(plan.keyspace, s)
}

func getHeader(plan *TablePlan) string {
	s := ""
	for _, field := range plan.fields {
		s += field.Name + "\t"
	}
	s += fmt.Sprintf("op\ttimestamp\tgtid")
	return s
}

func outputRows(plan *TablePlan, rows []*RowLog) {
	for _, row := range rows {
		s := ""
		for _, val := range row.values {
			s += val + "\t"
		}
		s += fmt.Sprintf("%s\t%s\t%s", row.op, row.when, row.gtid)
		output(plan.keyspace, s)
	}
}

func mustSend(plan *TablePlan, afterVals, beforeVals []string) bool {
	if len(afterVals) > 0 {
		if _, ok := plan.allowedIds[afterVals[plan.pkIndex]]; ok {
			return true
		}
	}
	if len(beforeVals) > 0 {
		if _, ok := plan.allowedIds[beforeVals[plan.pkIndex]]; ok {
			return true
		}
	}
	return false
}

type RowLog struct {
	op     string
	values []string
	when   string
	gtid   string
}

func processRowEvent(plan *TablePlan, gtid string, ev *binlogdatapb.VEvent) []*RowLog {
	op := "insert"
	var rowLogs []*RowLog
	for _, change := range ev.RowEvent.RowChanges {
		var after, before []sqltypes.Value
		var afterVals, beforeVals []string
		if change.After != nil {
			after = sqltypes.MakeRowTrusted(plan.fields, change.After)
		}
		if change.Before != nil {
			before = sqltypes.MakeRowTrusted(plan.fields, change.Before)
		}
		for _, val := range after {
			afterVals = append(afterVals, string(val.ToBytes()))
		}
		for _, val := range before {
			beforeVals = append(beforeVals, string(val.ToBytes()))
		}
		if !mustSend(plan, afterVals, beforeVals) {
			continue
		}

		if len(after) > 0 && len(before) > 0 {
			op = "update"
		} else if len(before) > 0 {
			op = "delete"
			afterVals = beforeVals
		}

		rowLog := &RowLog{
			op:     op,
			values: afterVals,
			when:   time.Unix(ev.Timestamp, 0).Format(time.RFC3339),
			gtid:   gtid,
		}
		rowLogs = append(rowLogs, rowLog)
	}
	return rowLogs
}

func getTablePlan(keyspace string, fields []*query.Field, table, pk string, ids []string) *TablePlan {
	allowedIds := make(map[string]bool)
	for _, id := range ids {
		allowedIds[id] = true
	}
	var pkIndex int64
	for i, field := range fields {
		if field.Name == pk {
			pkIndex = int64(i)
			break
		}
	}
	return &TablePlan{
		table:      table,
		pk:         pk,
		allowedIds: allowedIds,
		pkIndex:    pkIndex,
		fields:     fields,
		keyspace:   keyspace,
	}
}

type TablePlan struct {
	table, pk  string
	allowedIds map[string]bool
	pkIndex    int64
	fields     []*query.Field
	keyspace   string
}

func getPositions(ctx context.Context, server, keyspace, tablet string) (string, string) {
	curPos, err := getPosition(ctx, server, keyspace, "0")
	if err != nil {
		return "", ""
	}
	if curPos == "" {
		return "", ""
	}
	flavor := strings.Split(curPos, "/")[0]
	firstPos, err := getLastPosition(ctx, server, tablet)
	firstPos = flavor + "/" + firstPos
	return firstPos, curPos
}

func getTablet(ctx context.Context, ts *topo.Server, cells []string, keyspace string) string {
	picker, err := discovery.NewTabletPicker(ts, cells, keyspace, "0", "master,replica,rdonly")
	if err != nil {
		return ""
	}
	tab, err := picker.PickForStreaming(ctx)
	if err != nil {
		return ""
	}
	tabletId := fmt.Sprintf("%s-%d", tab.Alias.Cell, tab.Alias.Uid)
	return tabletId

}
func parseCommandLine() *RowLogConfig {
	sourceKeyspace := flag.String("source", "", "")
	targetKeyspace := flag.String("target", "", "")
	ids := flag.String("ids", "", "")
	pk := flag.String("pk", "", "")
	table := flag.String("table", "", "")
	vtgate := flag.String("vtgate", "", "")
	vtctld := flag.String("vtctld", "", "")
	cells := flag.String("cells", "", "")

	flag.Parse()

	return &RowLogConfig{
		sourceKeyspace: *sourceKeyspace,
		targetKeyspace: *targetKeyspace,
		table:          *table,
		pk:             *pk,
		ids:            strings.Split(*ids, ","),
		vtctld:         *vtctld,
		vtgate:         *vtgate,
		cells:          strings.Split(*cells, ","),
	}
}

func getLastPosition(ctx context.Context, server, tablet string) (string, error) {
	query := "select GTID_SUBTRACT(@@GLOBAL.gtid_executed, @@GLOBAL.gtid_purged);"
	results, err := execVtctl(ctx, server, []string{"ExecuteFetchAsDba", tablet, query})
	if err != nil {
		fmt.Println(err)
		return "", err
	}
	line := results[14]
	line = strings.Trim(strings.Replace(line, "|", "", 10), " \n")
	arr := strings.Split(line, ":")
	subs := strings.Split(arr[1], "-")
	lastPos := arr[0] + ":" + subs[0]
	return lastPos, nil
}

func getPosition(ctx context.Context, server, keyspace, shard string) (string, error) {
	results, err := execVtctl(ctx, server, []string{"ShardReplicationPositions", fmt.Sprintf("%s:%s", keyspace, shard)})
	if err != nil {
		fmt.Println(err)
		return "", err
	}
	splits := strings.Split(results[0], " ")
	return splits[8], nil
}

func execVtctl(ctx context.Context, server string, args []string) ([]string, error) {
	client, err := vtctlclient.New(server)
	if err != nil {
		fmt.Println(err)
		return nil, err
	}
	if client == nil {
		fmt.Printf("Unable to contact %s\n", server)
		return nil, fmt.Errorf("unable to contact %s\n", server)
	}
	defer client.Close()

	stream, err := client.ExecuteVtctlCommand(ctx, args, 10*time.Second)
	if err != nil {
		return nil, fmt.Errorf("cannot execute remote command: %v", err)
	}

	var results []string
	for {
		e, err := stream.Recv()
		switch err {
		case nil:
			if e.Level == logutilpb.Level_CONSOLE {
				results = append(results, e.Value)
			}
		case io.EOF:
			return results, nil
		default:
			log.Errorf("remote error: %v", err)
			return nil, fmt.Errorf("remote error: %v", err)
		}
	}
}
