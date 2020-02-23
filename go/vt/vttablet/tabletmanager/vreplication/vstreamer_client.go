/*
Copyright 2019 The Vitess Authors.

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

package vreplication

import (
	"errors"
	"fmt"
	"sync"

	"golang.org/x/net/context"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/dbconfigs"
	"vitess.io/vitess/go/vt/grpcclient"
	"vitess.io/vitess/go/vt/vttablet/queryservice"
	"vitess.io/vitess/go/vt/vttablet/tabletconn"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/connpool"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/schema"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tabletenv"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/vstreamer"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

var (
	_      VStreamerClient = (*TabletVStreamerClient)(nil)
	_      VStreamerClient = (*MySQLVStreamerClient)(nil)
	dbcfgs *dbconfigs.DBConfigs
)

// VStreamerClient exposes the core interface of a vstreamer
type VStreamerClient interface {
	// Open sets up all the environment for a vstream
	Open(ctx context.Context) error
	// Close closes a vstream
	Close(ctx context.Context) error

	// VStream streams VReplication events based on the specified filter.
	VStream(ctx context.Context, startPos string, filter *binlogdatapb.Filter, send func([]*binlogdatapb.VEvent) error) error

	// VStreamRows streams rows of a table from the specified starting point.
	VStreamRows(ctx context.Context, query string, lastpk *querypb.QueryResult, send func(*binlogdatapb.VStreamRowsResponse) error) error
}

// TabletVStreamerClient a vstream client backed by vttablet
type TabletVStreamerClient struct {
	// mu protects isOpen, streamers, streamIdx and kschema.
	mu sync.Mutex

	isOpen bool

	tablet         *topodatapb.Tablet
	target         *querypb.Target
	tsQueryService queryservice.QueryService
}

// MySQLVStreamerClient a vstream client backed by MySQL
type MySQLVStreamerClient struct {
	// mu protects isOpen, streamers, streamIdx and kschema.
	mu sync.Mutex

	isOpen bool

	sourceConnParams *mysql.ConnParams
	sourceSe         *schema.Engine
}

// NewTabletVStreamerClient creates a new TabletVStreamerClient
func NewTabletVStreamerClient(tablet *topodatapb.Tablet) *TabletVStreamerClient {
	return &TabletVStreamerClient{
		tablet: tablet,
		target: &querypb.Target{
			Keyspace:   tablet.Keyspace,
			Shard:      tablet.Shard,
			TabletType: tablet.Type,
		},
	}
}

// Open part of the VStreamerClient interface
func (vsClient *TabletVStreamerClient) Open(ctx context.Context) (err error) {
	vsClient.mu.Lock()
	defer vsClient.mu.Unlock()
	if vsClient.isOpen {
		return nil
	}
	vsClient.isOpen = true

	vsClient.tsQueryService, err = tabletconn.GetDialer()(vsClient.tablet, grpcclient.FailFast(true))
	return err
}

// Close part of the VStreamerClient interface
func (vsClient *TabletVStreamerClient) Close(ctx context.Context) (err error) {
	vsClient.mu.Lock()
	defer vsClient.mu.Unlock()
	if !vsClient.isOpen {
		return nil
	}
	vsClient.isOpen = false
	return vsClient.tsQueryService.Close(ctx)
}

// VStream part of the VStreamerClient interface
func (vsClient *TabletVStreamerClient) VStream(ctx context.Context, startPos string, filter *binlogdatapb.Filter, send func([]*binlogdatapb.VEvent) error) error {
	if !vsClient.isOpen {
		return errors.New("can't VStream without opening client")
	}
	return vsClient.tsQueryService.VStream(ctx, vsClient.target, startPos, filter, send)
}

// VStreamRows part of the VStreamerClient interface
func (vsClient *TabletVStreamerClient) VStreamRows(ctx context.Context, query string, lastpk *querypb.QueryResult, send func(*binlogdatapb.VStreamRowsResponse) error) error {
	if !vsClient.isOpen {
		return errors.New("can't VStreamRows without opening client")
	}
	return vsClient.tsQueryService.VStreamRows(ctx, vsClient.target, query, lastpk, send)
}

// NewMySQLVStreamerClient is a vstream client that allows you to stream directly from MySQL.
// In order to achieve this, the following creates a vstreamer Engine with a dummy in memorytopo.
func NewMySQLVStreamerClient() *MySQLVStreamerClient {
	if dbcfgs == nil {
		panic("can't use MySQLVStreamerClient without calling InitVStreamerClient() ")
	}
	// TODO: For now external mysql streams can only be used with ExternalReplWithDB creds.
	// In the future we will support multiple users.
	externReplWithDBParams, _ := dbcfgs.ExternalReplWithDB().GetConnParams()
	vsClient := &MySQLVStreamerClient{
		sourceConnParams: externReplWithDBParams,
	}
	return vsClient
}

// Open part of the VStreamerClient interface
func (vsClient *MySQLVStreamerClient) Open(ctx context.Context) (err error) {
	vsClient.mu.Lock()
	defer vsClient.mu.Unlock()
	if vsClient.isOpen {
		return nil
	}
	vsClient.isOpen = true

	// Let's create all the required components by vstreamer

	vsClient.sourceSe = schema.NewEngine(checker{}, tabletenv.DefaultQsConfig)
	vsClient.sourceSe.InitDBConfig(vsClient.sourceConnParams)
	err = vsClient.sourceSe.Open()
	if err != nil {
		return err
	}
	return nil
}

// Close part of the VStreamerClient interface
func (vsClient *MySQLVStreamerClient) Close(ctx context.Context) (err error) {
	vsClient.mu.Lock()
	defer vsClient.mu.Unlock()
	if !vsClient.isOpen {
		return nil
	}

	vsClient.isOpen = false
	vsClient.sourceSe.Close()
	return nil
}

// VStream part of the VStreamerClient interface
func (vsClient *MySQLVStreamerClient) VStream(ctx context.Context, startPos string, filter *binlogdatapb.Filter, send func([]*binlogdatapb.VEvent) error) error {
	if !vsClient.isOpen {
		return errors.New("can't VStream without opening client")
	}
	streamer := vstreamer.NewVStreamer(ctx, vsClient.sourceConnParams, vsClient.sourceSe, startPos, filter, send)
	return streamer.Stream()
}

// VStreamRows part of the VStreamerClient interface
func (vsClient *MySQLVStreamerClient) VStreamRows(ctx context.Context, query string, lastpk *querypb.QueryResult, send func(*binlogdatapb.VStreamRowsResponse) error) error {
	if !vsClient.isOpen {
		return errors.New("can't VStreamRows without opening client")
	}
	var row []sqltypes.Value
	if lastpk != nil {
		r := sqltypes.Proto3ToResult(lastpk)
		if len(r.Rows) != 1 {
			return fmt.Errorf("unexpected lastpk input: %v", lastpk)
		}
		row = r.Rows[0]
	}
	streamer := vstreamer.NewRowStreamer(ctx, vsClient.sourceConnParams, vsClient.sourceSe, query, row, send)
	return streamer.Stream()
}

// InitVStreamerClient initializes config for vstreamer client
func InitVStreamerClient(cfg *dbconfigs.DBConfigs) {
	dbcfgs = cfg
}

type checker struct{}

var _ = connpool.MySQLChecker(checker{})

func (checker) CheckMySQL() {}
