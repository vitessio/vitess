//go:build e2etest

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

// InnoDBLogTable is the table HasRecentInnoDBLongSemaphoreWait queries. Under
// the `e2etest` build tag it points at a writable test table so e2e tests can
// inject synthetic MY-012985 rows. The release variant lives in
// replication_release.go and points at the real MySQL log view.
const InnoDBLogTable = "_vt.fake_innodb_error_log"
