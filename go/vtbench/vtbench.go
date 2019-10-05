/*
Copyright 2018 The Vitess Authors

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

package vtbench

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"vitess.io/vitess/go/stats"
	"vitess.io/vitess/go/vt/log"

	querypb "vitess.io/vitess/go/vt/proto/query"
)

// ClientProtocol indicates how to connect
type ClientProtocol int

const (
	// MySQL uses the mysql wire protocol
	MySQL ClientProtocol = iota

	// GRPCVtgate uses the grpc wire protocol to vttablet
	GRPCVtgate

	// GRPCVttablet uses the grpc wire protocol to vttablet
	GRPCVttablet
)

// ProtocolString returns a string representation of the protocol
func (cp ClientProtocol) String() string {
	switch cp {
	case MySQL:
		return "mysql"
	case GRPCVtgate:
		return "grpc-vtgate"
	case GRPCVttablet:
		return "grpc-vttablet"
	default:
		return fmt.Sprintf("unknown-protocol-%d", cp)
	}
}

// ConnParams specifies how to connect to the vtgate(s)
type ConnParams struct {
	Hosts      []string
	Port       int
	DB         string
	Username   string
	Password   string
	UnixSocket string
	Protocol   ClientProtocol
}

// Bench controls the test
type Bench struct {
	ConnParams ConnParams
	Threads    int
	Count      int
	Query      string

	threads []benchThread

	// synchronization waitgroup for startup / completion
	wg sync.WaitGroup

	// starting gate used to block all threads on startup
	lock sync.RWMutex

	Rows    *stats.Counter
	Bytes   *stats.Counter
	Timings *stats.Timings

	TotalTime time.Duration
}

type benchThread struct {
	b        *Bench
	i        int
	conn     clientConn
	query    string
	bindVars map[string]*querypb.BindVariable
}

// NewBench creates a new bench test
func NewBench(threads, count int, cp ConnParams, query string) *Bench {
	bench := Bench{
		Threads:    threads,
		Count:      count,
		ConnParams: cp,
		Query:      query,
		Rows:       stats.NewCounter("", ""),
		Timings:    stats.NewTimings("", "", ""),
	}
	return &bench
}

// Run executes the test
func (b *Bench) Run(ctx context.Context) error {
	err := b.createConns(ctx)
	if err != nil {
		return err
	}

	b.createThreads(ctx)
	if err := b.runTest(ctx); err != nil {
		return err
	}
	return nil
}

func (b *Bench) createConns(ctx context.Context) error {
	log.V(10).Infof("creating %d client connections...", b.Threads)
	start := time.Now()
	reportInterval := 2 * time.Second
	report := start.Add(reportInterval)
	for i := 0; i < b.Threads; i++ {
		host := b.ConnParams.Hosts[i%len(b.ConnParams.Hosts)]
		cp := b.ConnParams
		cp.Hosts = []string{host}

		var conn clientConn
		var err error

		switch b.ConnParams.Protocol {
		case MySQL:
			log.V(5).Infof("connecting to %s using mysql protocol...", host)
			conn = &mysqlClientConn{}
			err = conn.connect(ctx, cp)
		case GRPCVtgate:
			log.V(5).Infof("connecting to %s using grpc vtgate protocol...", host)
			conn = &grpcVtgateConn{}
			err = conn.connect(ctx, cp)
		case GRPCVttablet:
			log.V(5).Infof("connecting to %s using grpc vttablet protocol...", host)
			conn = &grpcVttabletConn{}
			err = conn.connect(ctx, cp)
		default:
			return fmt.Errorf("unimplemented connection protocol %s", b.ConnParams.Protocol.String())
		}

		if err != nil {
			return fmt.Errorf("error connecting to %s using %v protocol: %v", host, cp.Protocol.String(), err)
		}

		// XXX handle normalization and per-thread query templating
		query, bindVars := b.getQuery(i)
		b.threads = append(b.threads, benchThread{
			b:        b,
			i:        i,
			conn:     conn,
			query:    query,
			bindVars: bindVars,
		})

		if time.Now().After(report) {
			fmt.Printf("Created %d/%d connections after %v\n", i, b.Threads, time.Since(start))
			report = time.Now().Add(reportInterval)
		}
	}

	return nil
}

func (b *Bench) getQuery(i int) (string, map[string]*querypb.BindVariable) {
	query := strings.Replace(b.Query, ":thread", fmt.Sprintf("%d", i), -1)
	bindVars := make(map[string]*querypb.BindVariable)
	return query, bindVars
}

func (b *Bench) createThreads(ctx context.Context) {
	// Create a barrier so all the threads start at the same time
	b.lock.Lock()

	log.V(10).Infof("starting %d threads", b.Threads)
	for i := 0; i < b.Threads; i++ {
		b.wg.Add(1)
		go b.threads[i].clientLoop(ctx)
	}

	log.V(10).Infof("waiting for %d threads to start", b.Threads)
	b.wg.Wait()

	b.wg.Add(b.Threads)
}

func (b *Bench) runTest(ctx context.Context) error {
	start := time.Now()
	fmt.Printf("Starting test threads\n")
	b.lock.Unlock()

	// Then wait for them all to finish looping
	log.V(10).Infof("waiting for %d threads to finish", b.Threads)
	b.wg.Wait()
	b.TotalTime = time.Since(start)

	return nil
}

func (bt *benchThread) clientLoop(ctx context.Context) {
	b := bt.b

	// Declare that startup is finished and wait for
	// the barrier
	b.wg.Done()
	log.V(10).Infof("thread %d waiting for startup barrier", bt.i)
	b.lock.RLock()
	log.V(10).Infof("thread %d starting loop", bt.i)

	for i := 0; i < b.Count; i++ {
		start := time.Now()
		result, err := bt.conn.execute(ctx, bt.query, bt.bindVars)
		b.Timings.Record("query", start)
		if err != nil {
			log.Errorf("query error: %v", err)
			break
		} else {
			b.Rows.Add(int64(result.RowsAffected))
		}

	}

	b.wg.Done()
}
