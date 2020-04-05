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
					Name: "user",
					Shards: []*vttestpb.Shard{{
						Name: "-80",
					}, {
						Name: "80-",
					}},
				}, {
					Name: "lookup",
					Shards: []*vttestpb.Shard{{
						Name: "0",
					}},
				}},
			},
			SchemaDir:     path.Join(os.Getenv("VTROOT"), "examples/demo/schema"),
			MySQLBindHost: "0.0.0.0",
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

	execQuery(conn, "user0", "select * from user", "user", "-80", response)
	execQuery(conn, "user1", "select * from user", "user", "80-", response)
	execQuery(conn, "user_extra0", "select * from user_extra", "user", "-80", response)
	execQuery(conn, "user_extra1", "select * from user_extra", "user", "80-", response)
	execQuery(conn, "music0", "select * from music", "user", "-80", response)
	execQuery(conn, "music1", "select * from music", "user", "80-", response)
	execQuery(conn, "music_extra0", "select * from music_extra", "user", "-80", response)
	execQuery(conn, "music_extra1", "select * from music_extra", "user", "80-", response)
	execQuery(conn, "name_info0", "select * from name_info", "user", "-80", response)
	execQuery(conn, "name_info1", "select * from name_info", "user", "80-", response)
	execQuery(conn, "music_keyspace_idx0", "select music_id, hex(keyspace_id) from music_keyspace_idx", "user", "-80", response)
	execQuery(conn, "music_keyspace_idx1", "select music_id, hex(keyspace_id) from music_keyspace_idx", "user", "80-", response)
	execQuery(conn, "user_seq", "select * from user_seq", "lookup", "0", response)
	execQuery(conn, "music_seq", "select * from music_seq", "lookup", "0", response)
	execQuery(conn, "name_keyspace_idx", "select name, hex(keyspace_id) from name_keyspace_idx", "lookup", "0", response)
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
			srow = append(srow, value.ToString())
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
