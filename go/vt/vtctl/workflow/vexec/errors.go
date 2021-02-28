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

package vexec

import "errors"

var (
	ErrCannotUpdateImmutableColumn = errors.New("cannot update immutable column")
	ErrNoShardPrimary              = errors.New("no primary found for shard")
	ErrNoShardsForKeyspace         = errors.New("no shards found in keyspace")
	ErrUnpreparedQuery             = errors.New("attempted to execute unprepared query")
	ErrUnsupportedQuery            = errors.New("query not supported by vexec")
	ErrUnsupportedQueryConstruct   = errors.New("unsupported query construct")
	ErrUnsupportedTable            = errors.New("table not supported by vexec")
)
