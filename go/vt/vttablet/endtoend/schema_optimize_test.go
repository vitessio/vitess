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
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/vttablet/endtoend/framework"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/schema"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

const gtidExecutedOptimizeDataFreeAtSpawn = 129 * 1024 * 1024

// runOptimizeGtidExecuted exposes the worker without a huge DATA_FREE fixture.
//
//go:linkname runOptimizeGtidExecuted vitess.io/vitess/go/vt/vttablet/tabletserver/schema.(*Engine).runOptimizeGtidExecuted
func runOptimizeGtidExecuted(*schema.Engine, uint64)

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

	setGlobalBoolVar(t, conn, "read_only", true)
	setGlobalBoolVar(t, conn, "super_read_only", true)

	logHandler := &capturingSlogHandler{}
	previousLogger := log.SwapLogger(slog.New(logHandler))
	t.Cleanup(func() {
		log.SwapLogger(previousLogger)
	})

	runOptimizeGtidExecuted(framework.Server.SchemaEngine(), gtidExecutedOptimizeDataFreeAtSpawn)

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
