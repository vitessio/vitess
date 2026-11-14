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

package servenv

import (
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"

	"vitess.io/vitess/go/vt/log/logtest"
)

// TestCheckServiceMapLogs checks that each line logs the service-map entry
// that does what its message says: "-grpc-x" disables the service and
// "grpc-x" enables it.
func TestCheckServiceMapLogs(t *testing.T) {
	serviceMap["grpc-registered"] = true
	t.Cleanup(func() { delete(serviceMap, "grpc-registered") })
	logs := logtest.Capture(t, slog.LevelInfo)

	assert.True(t, checkServiceMap("grpc", "registered"))
	assert.False(t, checkServiceMap("grpc", "unregistered"))

	record := logs.Record(t, "Registering service for protocol, disable it with the service_map_entry service-map parameter")
	assert.Equal(t, "registered", record["service"])
	assert.Equal(t, "-grpc-registered", record["service_map_entry"])
	record = logs.Record(t, "Not registering service for protocol, enable it with the service_map_entry service-map parameter")
	assert.Equal(t, "unregistered", record["service"])
	assert.Equal(t, "grpc-unregistered", record["service_map_entry"])
}
