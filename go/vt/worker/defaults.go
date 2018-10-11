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

package worker

import "vitess.io/vitess/go/vt/throttler"

const (
	defaultOnline  = true
	defaultOffline = true
	// defaultChunkCount is the number of chunks in which each table should be
	// divided. One chunk is processed by one chunk pipeline at a time.
	// -source_reader_count defines the number of concurrent chunk pipelines.
	defaultChunkCount = 1000
	// defaultMinRowsPerChunk is the minimum number of rows a chunk should have
	// on average. If this is not guaranteed, --chunk_count will be reduced
	// automatically.
	defaultMinRowsPerChunk   = 10 * 1000
	defaultSourceReaderCount = 10
	// defaultWriteQueryMaxRows aggregates up to 100 rows per INSERT or DELETE
	// query. Higher values are not recommended to avoid overloading MySQL.
	// The actual row count will be less if defaultWriteQueryMaxSize is reached
	// first, but always at least 1 row.
	defaultWriteQueryMaxRows = 100
	// defaultWriteQueryMaxSize caps the write queries which aggregate multiple
	// rows. This limit prevents e.g. that MySQL will OOM.
	defaultWriteQueryMaxSize = 1024 * 1024
	// defaultDestinationPackCount is deprecated in favor of the writeQueryMax*
	// values and currently only used by VerticalSplitClone.
	// defaultDestinationPackCount is the number of StreamExecute responses which
	// will be aggreated into one transaction. See the vttablet flag
	// "-queryserver-config-stream-buffer-size" for the size (in bytes) of a
	// StreamExecute response. As of 06/2015, the default for it was 32 kB.
	// Note that higher values for this flag --destination_pack_count will
	// increase memory consumption in vtworker, vttablet and mysql.
	defaultDestinationPackCount   = 10
	defaultDestinationWriterCount = 20
	defaultMinHealthyTablets      = 2
	defaultDestTabletType         = "RDONLY"
	defaultParallelDiffsCount     = 8
	defaultMaxTPS                 = throttler.MaxRateModuleDisabled
	defaultMaxReplicationLag      = throttler.ReplicationLagModuleDisabled
	defaultUseConsistentSnapshot  = false
	defaultSkipNullRows           = false
)
