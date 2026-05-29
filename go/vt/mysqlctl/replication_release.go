//go:build !e2etest

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

package mysqlctl

// innoDBLogTable is the table HasRecentInnoDBLongSemaphoreWait queries. In
// release builds it points at the real MySQL log view. The e2etest-build
// variant lives in replication_e2etest.go and swaps in a writable test table.
const innoDBLogTable = "performance_schema.error_log"
