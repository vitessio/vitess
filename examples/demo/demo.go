/*
Copyright 2020 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package main

import (
	"bufio"
	"context"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"path"
	"strings"
	"syscall"
	"time"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/log"
	vttestpb "vitess.io/vitess/go/vt/proto/vttest"
	"vitess.io/vitess/go/vt/vttest"
)

var cluster *vttest.LocalCluster
var querylog <-chan string

func main() {
	flag.Parse()

	runCluster()
	querylog, _ = streamQuerylog(cluster.Env.PortForProtocol("vtcombo", ""))

	mux := http.NewServeMux()
	mux.Handle("/", http.FileServer(http.Dir("./")))
	mux.HandleFunc("/exec", exec)
	go http.ListenAndServe(":8000", mux)

	wait()
	cluster.TearDown()
}

func runCluster() {
	cluster = &vttest.LocalCluster{
		Config: vttest.Config{
			Topology: &vttestpb.VTTestTopology{
				Keyspaces: []*vttestpb.Keyspace{{
					Name: "customer",
					Shards: []*vttestpb.Shard{{
						Name: "-80",
					}, {
						Name: "80-",
					}},
				}, {
					Name: "product",
					Shards: []*vttestpb.Shard{{
						Name: "0",
					}},
				}},
			},
			SchemaDir:     path.Join(os.Getenv("VTROOT"), "examples/demo/schema"),
			MySQLBindHost: "0.0.0.0",
			// VSchemaDDLAuthorizedUsers allows you to experiment with vschema DDLs.
			VSchemaDDLAuthorizedUsers: "%",
		},
	}
	env, err := vttest.NewLocalTestEnv("", 12345)
	if err != nil {
		log.Exitf("Error: %v", err)
	}
	cluster.Env = env
	err = cluster.Setup()
	if err != nil {
		cluster.TearDown()
		log.Exitf("Error: %v", err)
	}
}

func wait() {
	c := make(chan os.Signal, 2)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	<-c
}

func exec(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(true)

	cp := &mysql.ConnParams{
		Host: "127.0.0.1",
		Port: cluster.Env.PortForProtocol("vtcombo_mysql_port", ""),
	}
	conn, err := mysql.Connect(context.Background(), cp)
	if err != nil {
		response := map[string]string{
			"error": err.Error(),
		}
		enc.Encode(response)
		return
	}
	defer conn.Close()
	query := req.FormValue("query")
	response := make(map[string]interface{})

	var queries []string
	// Clear existing log.
	for {
		select {
		case <-querylog:
			continue
		default:
		}
		break
	}
	execQuery(conn, "result", query, "", "", response)
	// Collect
	time.Sleep(250 * time.Millisecond)
	for {
		select {
		case val := <-querylog:
			queries = append(queries, val)
			continue
		default:
		}
		break
	}
	response["queries"] = queries

	execQuery(conn, "customer0", "select * from customer", "customer", "-80", response)
	execQuery(conn, "customer1", "select * from customer", "customer", "80-", response)
	execQuery(conn, "corder0", "select * from corder", "customer", "-80", response)
	execQuery(conn, "corder1", "select * from corder", "customer", "80-", response)
	execQuery(conn, "corder_event0", "select * from corder_event", "customer", "-80", response)
	execQuery(conn, "corder_event1", "select * from corder_event", "customer", "80-", response)
	execQuery(conn, "oname_keyspace_idx0", "select * from oname_keyspace_idx", "customer", "-80", response)
	execQuery(conn, "oname_keyspace_idx1", "select * from oname_keyspace_idx", "customer", "80-", response)
	execQuery(conn, "product", "select * from product", "product", "0", response)
	execQuery(conn, "customer_seq", "select * from customer_seq", "product", "0", response)
	execQuery(conn, "corder_keyspace_idx", "select * from corder_keyspace_idx", "product", "0", response)
	enc.Encode(response)
}

func execQuery(conn *mysql.Conn, key, query, keyspace, shard string, response map[string]interface{}) {
	if query == "" || query == "undefined" {
		return
	}
	if keyspace != "" {
		_, err := conn.ExecuteFetch(fmt.Sprintf("use `%v:%v`", keyspace, shard), 10000, true)
		if err != nil {
			response[key] = map[string]interface{}{
				"title": key,
				"error": err.Error(),
			}
			return
		}
	}
	title := key
	switch {
	case strings.HasSuffix(key, "0"):
		title = key[:len(key)-1] + ":-80"
	case strings.HasSuffix(key, "1"):
		title = key[:len(key)-1] + ":80-"
	}
	qr, err := conn.ExecuteFetch(query, 10000, true)
	if err != nil {
		if strings.Contains(err.Error(), "doesn't exist") {
			return
		}
		response[key] = map[string]interface{}{
			"title": title,
			"error": err.Error(),
		}
		log.Errorf("error: %v", err)
		return
	}
	response[key] = resultToMap(title, qr)
}

func resultToMap(title string, qr *sqltypes.Result) map[string]interface{} {
	fields := make([]string, 0, len(qr.Fields))
	for _, field := range qr.Fields {
		fields = append(fields, field.Name)
	}
	rows := make([][]string, 0, len(qr.Rows))
	for _, row := range qr.Rows {
		srow := make([]string, 0, len(row))
		for _, value := range row {
			if value.Type() == sqltypes.VarBinary {
				srow = append(srow, hex.EncodeToString(value.ToBytes()))
			} else {
				srow = append(srow, value.ToString())
			}
		}
		rows = append(rows, srow)
	}
	return map[string]interface{}{
		"title":        title,
		"fields":       fields,
		"rows":         rows,
		"rowsaffected": int64(qr.RowsAffected),
		"insertid":     int64(qr.InsertID),
	}
}

func streamQuerylog(port int) (<-chan string, error) {
	request := fmt.Sprintf("http://localhost:%d/debug/querylog", port)
	resp, err := http.Get(request)
	if err != nil {
		log.Errorf("Error reading stream: %v: %v", request, err)
		return nil, err
	}
	ch := make(chan string, 100)
	go func() {
		buffered := bufio.NewReader(resp.Body)
		for {
			str, err := buffered.ReadString('\n')
			if err != nil {
				log.Errorf("Error reading stream: %v: %v", request, err)
				close(ch)
				resp.Body.Close()
				return
			}
			splits := strings.Split(str, "\t")
			if len(splits) < 13 {
				continue
			}
			trimmed := strings.Trim(splits[12], `"`)
			if trimmed == "" {
				continue
			}
			splitQueries := strings.Split(trimmed, ";")
			var final []string
			for _, query := range splitQueries {
				if strings.Contains(query, "1 != 1") {
					continue
				}
				final = append(final, query)
			}
			select {
			case ch <- strings.Join(final, "; "):
			default:
			}
		}
	}()
	return ch, nil
}
