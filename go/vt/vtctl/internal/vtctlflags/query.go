/*
Copyright 2021 The Vitess Authors.

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

package vtctlflags

import "flag"

var (
	enableQueries = flag.Bool("enable_queries", false, "if set, allows vtgate and vttablet queries. May have security implications, as the queries will be run from this process.")
)

// AreQueriesEnabled returns whether a vtctl was started with
// the -enable_queries flag.
func AreQueriesEnabled() bool { return *enableQueries }
