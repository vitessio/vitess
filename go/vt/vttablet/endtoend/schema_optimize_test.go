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

package endtoend

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"
	"unsafe"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/mysql/replication"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/vttablet/endtoend/framework"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/schema"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

const (
	gtidExecutedOptimizeThresholdBytes = 128 * 1024 * 1024
	gtidExecutedOptimizeSetupTarget    = gtidExecutedOptimizeThresholdBytes + 32*1024*1024
	gtidExecutedOptimizeBatchRows      = 1000000
	gtidExecutedOptimizeInsertBatches  = 8
)

func TestSchemaEngineOptimizeGtidExecutedOnReplica(t *testing.T) {
	ctx := t.Context()
	client := framework.NewClient()

	conn, err := mysql.Connect(ctx, &connParams)
	require.NoError(t, err)
	t.Cleanup(func() {
		conn.Close()
	})

	require.NoError(t, client.SetServingType(topodatapb.TabletType_REPLICA))
	t.Cleanup(func() {
		require.NoError(t, client.SetServingType(topodatapb.TabletType_PRIMARY))
		setGlobalBoolVar(t, conn, "super_read_only", false)
		setGlobalBoolVar(t, conn, "read_only", false)
	})
	stampSchemaEngineTabletTypeLastChangedAt(t, framework.Server.SchemaEngine(), time.Now().Add(-10*time.Minute))

	createGtidExecutedDataFree(t, conn, gtidExecutedOptimizeSetupTarget)
	dataFreeBefore := waitForGtidExecutedDataFreeAtLeast(t, conn, gtidExecutedOptimizeSetupTarget)
	require.Greater(t, dataFreeBefore, uint64(gtidExecutedOptimizeThresholdBytes))

	setGlobalBoolVar(t, conn, "read_only", true)
	setGlobalBoolVar(t, conn, "super_read_only", true)

	logHandler := &capturingSlogHandler{}
	previousLogger := log.SwapLogger(slog.New(logHandler))
	t.Cleanup(func() {
		log.SwapLogger(previousLogger)
	})

	require.NoError(t, framework.Server.SchemaEngine().ReloadAtEx(ctx, replication.Position{}, true))

	assert.Eventually(t, func() bool {
		return logHandler.Contains("schema engine: OPTIMIZE of mysql.gtid_executed completed")
	}, 2*time.Minute, 100*time.Millisecond)

	assert.Eventually(t, func() bool {
		readOnly, err := getGlobalBoolVar(conn, "read_only")
		if err != nil {
			return false
		}
		superReadOnly, err := getGlobalBoolVar(conn, "super_read_only")
		if err != nil {
			return false
		}
		return readOnly && superReadOnly
	}, 30*time.Second, 100*time.Millisecond)

	assert.True(t, getRequiredGlobalBoolVar(t, conn, "read_only"))
	assert.True(t, getRequiredGlobalBoolVar(t, conn, "super_read_only"))
}

// stampSchemaEngineTabletTypeLastChangedAt sets the Engine's
// unexported tabletTypeLastChangedAt field to a past time so that
// the OPTIMIZE stability cooldown does not delay the test.
//
// We deliberately reach into the field via reflect + unsafe.Pointer
// rather than adding a SetTabletTypeLastChangedAtForTests method to
// the production Engine: we do not want a test-only mutator on a
// production API. The tradeoff is that a future rename of the
// struct field breaks this test at runtime (require.True on
// field.IsValid) rather than at compile time.
func stampSchemaEngineTabletTypeLastChangedAt(t *testing.T, se *schema.Engine, when time.Time) {
	t.Helper()

	field := reflect.ValueOf(se).Elem().FieldByName("tabletTypeLastChangedAt")
	require.True(t, field.IsValid(), "tabletTypeLastChangedAt no longer exists on schema.Engine; update this test")
	reflect.NewAt(field.Type(), unsafe.Pointer(field.UnsafeAddr())).Elem().Set(reflect.ValueOf(when))
}

func createGtidExecutedDataFree(t *testing.T, conn *mysql.Conn, minDataFree uint64) {
	t.Helper()

	requiresGtidTag := gtidExecutedHasTagColumn(t, conn)
	fakeUUIDs := make([]string, 0, gtidExecutedOptimizeInsertBatches)
	for batch := range gtidExecutedOptimizeInsertBatches {
		fakeUUID := fmt.Sprintf("aaaaaaaa-aaaa-aaaa-aaaa-%012x", batch+1)
		fakeUUIDs = append(fakeUUIDs, fakeUUID)
		insertFakeGTIDBatch(t, conn, fakeUUID, requiresGtidTag, gtidExecutedOptimizeBatchRows)
	}
	for _, fakeUUID := range fakeUUIDs {
		deleteFakeGTIDBatch(t, conn, fakeUUID, requiresGtidTag)
	}
	execFetch(t, conn, "ANALYZE TABLE mysql.gtid_executed")
	dataFree := waitForGtidExecutedDataFreeFloor(t, conn)
	require.GreaterOrEqual(t, dataFree, minDataFree, "failed to synthesize enough mysql.gtid_executed DATA_FREE for optimize test")
}

func insertFakeGTIDBatch(t *testing.T, conn *mysql.Conn, fakeUUID string, requiresGtidTag bool, rows int) {
	t.Helper()

	columns := "source_uuid, interval_start, interval_end"
	values := fmt.Sprintf("'%s', seq.n + 1, seq.n + 1", fakeUUID)
	if requiresGtidTag {
		columns += ", gtid_tag"
		values += ", ''"
	}

	query := fmt.Sprintf(`INSERT INTO mysql.gtid_executed (%s)
SELECT %s
FROM (
	SELECT d0.n + 10*d1.n + 100*d2.n + 1000*d3.n + 10000*d4.n + 100000*d5.n AS n
	FROM %s
	CROSS JOIN %s
	CROSS JOIN %s
	CROSS JOIN %s
	CROSS JOIN %s
	CROSS JOIN %s
) AS seq
WHERE seq.n < %d`, columns, values, inlineDigitsTable("d0"), inlineDigitsTable("d1"), inlineDigitsTable("d2"), inlineDigitsTable("d3"), inlineDigitsTable("d4"), inlineDigitsTable("d5"), rows)

	execFetch(t, conn, query)
}

func deleteFakeGTIDBatch(t *testing.T, conn *mysql.Conn, fakeUUID string, requiresGtidTag bool) {
	t.Helper()

	query := fmt.Sprintf("DELETE FROM mysql.gtid_executed WHERE source_uuid = '%s'", fakeUUID)
	if requiresGtidTag {
		query += " AND gtid_tag = ''"
	}
	execFetch(t, conn, query)
}

func waitForGtidExecutedDataFreeAtLeast(t *testing.T, conn *mysql.Conn, minDataFree uint64) uint64 {
	t.Helper()

	var dataFree uint64
	assert.Eventually(t, func() bool {
		var err error
		dataFree, err = getGtidExecutedDataFree(conn)
		return err == nil && dataFree >= minDataFree
	}, 30*time.Second, time.Second, "mysql.gtid_executed DATA_FREE never reached %d bytes", minDataFree)
	return dataFree
}

func waitForGtidExecutedDataFreeFloor(t *testing.T, conn *mysql.Conn) uint64 {
	t.Helper()

	var dataFree uint64
	assert.Eventually(t, func() bool {
		var err error
		dataFree, err = getGtidExecutedDataFree(conn)
		return err == nil
	}, 10*time.Second, 500*time.Millisecond)
	return dataFree
}

func getGtidExecutedDataFree(conn *mysql.Conn) (uint64, error) {
	qr, err := conn.ExecuteFetch("SELECT data_free FROM information_schema.TABLES WHERE table_schema = 'mysql' AND table_name = 'gtid_executed'", 1, false)
	if err != nil {
		return 0, err
	}
	if len(qr.Rows) == 0 || len(qr.Rows[0]) == 0 {
		return 0, errors.New("mysql.gtid_executed row missing from information_schema.TABLES")
	}
	return qr.Rows[0][0].ToCastUint64()
}

func gtidExecutedHasTagColumn(t *testing.T, conn *mysql.Conn) bool {
	t.Helper()

	qr, err := conn.ExecuteFetch("SELECT column_name FROM information_schema.COLUMNS WHERE table_schema = 'mysql' AND table_name = 'gtid_executed' ORDER BY ordinal_position", 10, false)
	require.NoError(t, err)

	for _, row := range qr.Rows {
		if strings.EqualFold(row[0].ToString(), "gtid_tag") {
			return true
		}
	}
	return false
}

func getRequiredGlobalBoolVar(t *testing.T, conn *mysql.Conn, name string) bool {
	t.Helper()

	value, err := getGlobalBoolVar(conn, name)
	require.NoError(t, err)
	return value
}

func getGlobalBoolVar(conn *mysql.Conn, name string) (bool, error) {
	qr, err := conn.ExecuteFetch("SELECT @@global."+name, 1, false)
	if err != nil {
		return false, err
	}
	if len(qr.Rows) == 0 || len(qr.Rows[0]) == 0 {
		return false, fmt.Errorf("missing @@global.%s result", name)
	}
	value := strings.ToUpper(qr.Rows[0][0].ToString())
	return value == "1" || value == "ON", nil
}

func setGlobalBoolVar(t *testing.T, conn *mysql.Conn, name string, enabled bool) {
	t.Helper()

	value := "OFF"
	if enabled {
		value = "ON"
	}
	execFetch(t, conn, fmt.Sprintf("SET GLOBAL %s = '%s'", name, value))
}

func execFetch(t *testing.T, conn *mysql.Conn, query string) {
	t.Helper()

	_, err := conn.ExecuteFetch(query, 1, false)
	require.NoError(t, err)
}

func inlineDigitsTable(alias string) string {
	return "(SELECT 0 AS n UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) AS " + alias
}

type capturingSlogHandler struct {
	mu       sync.Mutex
	messages []string
}

func (h *capturingSlogHandler) Enabled(context.Context, slog.Level) bool {
	return true
}

func (h *capturingSlogHandler) Handle(_ context.Context, record slog.Record) error {
	h.mu.Lock()
	h.messages = append(h.messages, record.Message)
	h.mu.Unlock()
	return nil
}

func (h *capturingSlogHandler) WithAttrs([]slog.Attr) slog.Handler {
	return h
}

func (h *capturingSlogHandler) WithGroup(string) slog.Handler {
	return h
}

func (h *capturingSlogHandler) Contains(message string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, current := range h.messages {
		if strings.Contains(current, message) {
			return true
		}
	}
	return false
}
