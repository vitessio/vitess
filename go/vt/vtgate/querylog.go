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

package vtgate

import (
	"github.com/youtube/vitess/go/streamlog"
)

var (
	// Handler path for exposing query logs
	queryLogHandler = "/debug/querylog"

	// QueryLogger enables streaming logging of queries
	QueryLogger = streamlog.New("VTGate", 10)
)

func initQueryLogger() {
	QueryLogger.ServeLogs(queryLogHandler, streamlog.GetFormatter(QueryLogger))
}
