/*
Copyright 2026 The Vitess Authors.

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

package trace

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"net"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	collectortracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/grpc"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/test/endtoend/utils"
)

var (
	clusterInstance *cluster.LocalProcessCluster
	vtParams        mysql.ConnParams
	collector       *otlpCollector
	cell            = "zone1"
	hostname        = "localhost"
	keyspaceName    = "ks"

	schemaSQL = `CREATE TABLE t1 (id BIGINT NOT NULL PRIMARY KEY) ENGINE=InnoDB;`
	vschema   = `{"sharded": false, "tables": {"t1": {}}}`
)

// otlpCollector is a minimal OTLP gRPC trace collector that records
// received spans in memory for test assertions.
type otlpCollector struct {
	collectortracepb.UnimplementedTraceServiceServer

	mu    sync.Mutex
	spans []*tracepb.Span
	srv   *grpc.Server
	port  int
}

func (c *otlpCollector) Export(_ context.Context, req *collectortracepb.ExportTraceServiceRequest) (*collectortracepb.ExportTraceServiceResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, rs := range req.GetResourceSpans() {
		for _, ss := range rs.GetScopeSpans() {
			c.spans = append(c.spans, ss.GetSpans()...)
		}
	}
	return &collectortracepb.ExportTraceServiceResponse{}, nil
}

// spanWithTraceID returns the first span whose trace ID matches (hex-encoded), or nil.
func (c *otlpCollector) spanWithTraceID(traceID string) *tracepb.Span {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, s := range c.spans {
		if hex.EncodeToString(s.GetTraceId()) == traceID {
			return s
		}
	}
	return nil
}

// spanWithAttribute returns the first span that has the given string attribute key=value.
func (c *otlpCollector) spanWithAttribute(key, value string) *tracepb.Span {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, s := range c.spans {
		for _, attr := range s.GetAttributes() {
			if attr.GetKey() == key && attr.GetValue().GetStringValue() == value {
				return s
			}
		}
	}
	return nil
}

// dumpSpans returns a debug string of all collected spans.
func (c *otlpCollector) dumpSpans() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	var b strings.Builder
	for i, s := range c.spans {
		fmt.Fprintf(&b, "  [%d] name=%q traceID=%x attrs=[", i, s.GetName(), s.GetTraceId())
		for _, a := range s.GetAttributes() {
			fmt.Fprintf(&b, "%s=%s ", a.GetKey(), attrValueStr(a.GetValue()))
		}
		fmt.Fprintln(&b, "]")
	}
	return b.String()
}

func attrValueStr(v *commonpb.AnyValue) string {
	if v == nil {
		return "<nil>"
	}
	if sv := v.GetStringValue(); sv != "" {
		return sv
	}
	return fmt.Sprintf("%v", v)
}

func TestMain(m *testing.M) {
	flag.Parse()

	exitCode := func() int {
		clusterInstance = cluster.NewCluster(cell, hostname)
		defer clusterInstance.Teardown()

		if err := clusterInstance.StartTopo(); err != nil {
			fmt.Fprintf(os.Stderr, "StartTopo: %v\n", err)
			return 1
		}

		keyspace := &cluster.Keyspace{
			Name:      keyspaceName,
			SchemaSQL: schemaSQL,
			VSchema:   vschema,
		}
		if err := clusterInstance.StartUnshardedKeyspace(*keyspace, 0, false, cell); err != nil {
			fmt.Fprintf(os.Stderr, "StartUnshardedKeyspace: %v\n", err)
			return 1
		}

		// Start a minimal OTLP collector that records exported spans.
		collectorPort := clusterInstance.GetAndReservePort()
		lis, err := net.Listen("tcp", fmt.Sprintf("localhost:%d", collectorPort))
		if err != nil {
			fmt.Fprintf(os.Stderr, "collector listen on port %d: %v\n", collectorPort, err)
			return 1
		}
		collector = &otlpCollector{
			srv:  grpc.NewServer(),
			port: collectorPort,
		}
		collectortracepb.RegisterTraceServiceServer(collector.srv, collector)
		go func() { _ = collector.srv.Serve(lis) }()
		defer collector.srv.GracefulStop()

		// Start vtgate with OpenTelemetry tracing pointed at our collector.
		clusterInstance.VtGateExtraArgs = append(clusterInstance.VtGateExtraArgs,
			"--tracer", "opentelemetry",
			"--otel-insecure",
			"--otel-endpoint", fmt.Sprintf("localhost:%d", collector.port),
			"--tracing-sampling-rate", "1.0",
		)
		if err := clusterInstance.StartVtgate(); err != nil {
			fmt.Fprintf(os.Stderr, "StartVtgate: %v\n", err)
			return 1
		}

		vtParams = clusterInstance.GetVTParams(keyspaceName)
		return m.Run()
	}()
	os.Exit(exitCode)
}

const warnSubstring = "Unable to parse VT_SPAN_CONTEXT"

// vtgateWarningCount returns the number of VT_SPAN_CONTEXT warnings in vtgate's stderr log.
func vtgateWarningCount(t *testing.T) int {
	t.Helper()
	data, err := os.ReadFile(clusterInstance.VtgateProcess.ErrorLog)
	require.NoError(t, err)
	return strings.Count(string(data), warnSubstring)
}

// buildCarrier creates a valid base64-encoded VT_SPAN_CONTEXT carrier for
// the given W3C traceparent string.
func buildCarrier(traceparent string) string {
	carrier := map[string]string{"traceparent": traceparent}
	jsonBytes, _ := json.Marshal(carrier)
	return base64.StdEncoding.EncodeToString(jsonBytes)
}

// TestVTSpanContextComQuery verifies that VT_SPAN_CONTEXT is parsed when
// a query is sent via COM_QUERY (the standard text protocol path).
func TestVTSpanContextComQuery(t *testing.T) {
	ctx := t.Context()
	conn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	before := vtgateWarningCount(t)

	// Send a query with an invalid VT_SPAN_CONTEXT via COM_QUERY.
	qr := utils.Exec(t, conn, "/*VT_SPAN_CONTEXT=invalid*/SELECT 1")
	require.Len(t, qr.Rows, 1)

	assert.Eventually(t, func() bool {
		return vtgateWarningCount(t) > before
	}, 30*time.Second, 500*time.Millisecond,
		"expected VT_SPAN_CONTEXT warning in vtgate stderr for COM_QUERY path")
}

// TestVTSpanContextPreparedStatement verifies that VT_SPAN_CONTEXT is parsed
// when a query is sent via prepared statements (COM_STMT_PREPARE / COM_STMT_EXECUTE).
//
// Reproduces https://github.com/vitessio/vitess/issues/19942
func TestVTSpanContextPreparedStatement(t *testing.T) {
	connStr := fmt.Sprintf("@tcp(%s:%d)/%s", hostname, clusterInstance.VtgateMySQLPort, keyspaceName)
	db, err := sql.Open("mysql", connStr)
	require.NoError(t, err)
	defer db.Close()

	before := vtgateWarningCount(t)

	var result int
	err = db.QueryRow("/*VT_SPAN_CONTEXT=invalid*/SELECT ?", 1).Scan(&result)
	require.NoError(t, err)
	assert.Equal(t, 1, result)

	assert.Eventually(t, func() bool {
		return vtgateWarningCount(t) > before
	}, 30*time.Second, 500*time.Millisecond,
		"expected VT_SPAN_CONTEXT warning in vtgate stderr for prepared statement path")
}

// TestVTSpanContextValidTraceComQuery sends a valid VT_SPAN_CONTEXT via
// COM_QUERY and verifies that the trace span is exported to the collector
// with the expected trace ID.
func TestVTSpanContextValidTraceComQuery(t *testing.T) {
	const traceID = "4bf92f3577b34da6a3ce929d0e0e4736"
	const spanID = "00f067aa0ba902b7"
	traceparent := fmt.Sprintf("00-%s-%s-01", traceID, spanID)
	carrier := buildCarrier(traceparent)

	ctx := t.Context()
	conn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	before := vtgateWarningCount(t)

	qr := utils.Exec(t, conn, fmt.Sprintf("/*VT_SPAN_CONTEXT=%s*/SELECT 1", carrier))
	require.Len(t, qr.Rows, 1)

	// The OTLP batch exporter flushes on a timer (default 5s).
	// Wait for the span to arrive at our collector.
	require.Eventually(t, func() bool {
		return collector.spanWithTraceID(traceID) != nil
	}, 30*time.Second, 500*time.Millisecond,
		"expected collector to receive a span with traceID %s for COM_QUERY; collected spans:\n%s",
		traceID, collector.dumpSpans())

	// No warning should be logged for a valid carrier.
	assert.Equal(t, before, vtgateWarningCount(t),
		"valid VT_SPAN_CONTEXT should not produce a warning")
}

// TestVTSpanContextValidTracePreparedStatement sends a valid VT_SPAN_CONTEXT
// via prepared statement and verifies that the trace span is exported to the
// collector with the expected trace ID.
func TestVTSpanContextValidTracePreparedStatement(t *testing.T) {
	const traceID = "abcdef1234567890abcdef1234567890"
	const spanID = "1234567890abcdef"
	traceparent := fmt.Sprintf("00-%s-%s-01", traceID, spanID)
	carrier := buildCarrier(traceparent)

	connStr := fmt.Sprintf("@tcp(%s:%d)/%s?interpolateParams=false", hostname, clusterInstance.VtgateMySQLPort, keyspaceName)
	db, err := sql.Open("mysql", connStr)
	require.NoError(t, err)
	defer db.Close()

	before := vtgateWarningCount(t)

	var result int
	err = db.QueryRow(fmt.Sprintf("/*VT_SPAN_CONTEXT=%s*/SELECT ?", carrier), 1).Scan(&result)
	require.NoError(t, err)
	assert.Equal(t, 1, result)

	// Wait for the span to arrive at our collector.
	require.Eventually(t, func() bool {
		return collector.spanWithTraceID(traceID) != nil
	}, 30*time.Second, 500*time.Millisecond,
		"expected collector to receive a span with traceID %s for prepared statement; collected spans:\n%s",
		traceID, collector.dumpSpans())

	// No warning should be logged for a valid carrier.
	assert.Equal(t, before, vtgateWarningCount(t),
		"valid VT_SPAN_CONTEXT should not produce a warning")
}
