/*
Copyright 2017 Google Inc.

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

// Package influxdbbackend is useful for publishing metrics to an InfluxDB backend (tested on v0.88).
// It requires a database to already have been created in InfluxDB, and then specified via the
// "--influxdb_database" flag.
//
// It's still a work in progress, as it publishes almost all stats as key-value string pairs,
// instead of better JSON representations. This limitation will hopefully be fixed after the
// release of InfluxDB v0.9, as it has better support for arbitrary metadata dicts in the
// form of tags.
package influxdbbackend

import (
	"expvar"
	"flag"

	log "github.com/golang/glog"
	influxClient "github.com/influxdb/influxdb/client"
	"github.com/youtube/vitess/go/stats"
	"github.com/youtube/vitess/go/vt/servenv"
)

var influxDBHost = flag.String("influxdb_host", "localhost:8086", "the influxdb host (with port)")
var influxDBDatabase = flag.String("influxdb_database", "vitess", "the name of the influxdb database")
var influxDBUsername = flag.String("influxdb_username", "root", "influxdb username")
var influxDBPassword = flag.String("influxdb_password", "root", "influxdb password")

// InfluxDBBackend implements stats.PushBackend
type InfluxDBBackend struct {
	client *influxClient.Client
}

// init attempts to create a singleton InfluxDBBackend and register it as a PushBackend.
// If it fails to create one, this is a noop.
func init() {
	// Needs to happen in servenv.OnRun() instead of init because it requires flag parsing and logging
	servenv.OnRun(func() {
		config := &influxClient.ClientConfig{
			Host:     *influxDBHost,
			Username: *influxDBUsername,
			Password: *influxDBPassword,
			Database: *influxDBDatabase,
		}
		client, err := influxClient.NewClient(config)
		if err != nil {
			log.Errorf("Unable to create an InfluxDB client: %v", err)
			return
		}

		stats.RegisterPushBackend("influxdb", &InfluxDBBackend{
			client: client,
		})
	})
}

// PushAll pushes all expvar stats to InfluxDB
func (backend *InfluxDBBackend) PushAll() error {
	series := []*influxClient.Series{}
	expvar.Do(func(kv expvar.KeyValue) {
		series = append(series, &influxClient.Series{
			Name: "stats",
			// TODO(aaijazi): This would be much better suited to InfluxDB v0.90's tags.
			// Ideally, we'd use some of the expvars as tags, and some as values.
			// However, as of 03/11/2015, InfluxDB v0.90 hasn't proven quite stable enough to use.
			Columns: []string{"key", "value"},
			Points: [][]interface{}{
				{kv.Key, statToValue(kv.Value)},
			},
		})
	})
	err := backend.client.WriteSeries(series)
	return err
}

// statToValue converts from a stats.Stat type to a JSON representable value.
// This is preferred to just calling the String() for things like numbers, so that
// InfluxDB can also represent the metrics as numbers.
// TODO(aaijazi): this needs to be extended to support better serialization of other types..
// It's probably good to do this after InfluxDB 0.9 is released, as it has has better support
// for arbitrary dict values (as tags).
func statToValue(v expvar.Var) interface{} {
	switch v := v.(type) {
	case *stats.Float:
		return v.Get()
	case *stats.Int:
		return v.Get()
	case stats.FloatFunc:
		return v()
	case stats.IntFunc:
		return v()
	default:
		return v.String()
	}
}
