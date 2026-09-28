/*
Copyright 2023 The Vitess Authors.

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

package opentsdb

import (
	"time"

	"vitess.io/vitess/go/stats"
)

// backend implements stats.PushBackend
type backend struct {
	// The prefix is the name of the binary (vtgate, vttablet, etc.) and will be
	// prepended to all the stats reported.
	prefix string
	// Tags that should be included with every data point. If there's a tag name
	// collision between the common tags and a single data point's tags, the data
	// point tag will override the common tag.
	commonTags map[string]string
	// writer is used to send data points somewhere (file, http, ...).
	writer writer
}

// PushAll pushes all stats to OpenTSDB
func (b *backend) PushAll() error {
	collector := b.collector()
	collector.collectAll()
	return b.writer.Write(collector.data)
}

// PushOne pushes a single stat to OpenTSDB
func (b *backend) PushOne(name string, v stats.Variable) error {
	collector := b.collector()
	collector.collectOne(name, v)
	return b.writer.Write(collector.data)
}

func (b *backend) collector() *collector {
	return &collector{
		commonTags: b.commonTags,
		prefix:     b.prefix,
		timestamp:  time.Now().Unix(),
	}
}
