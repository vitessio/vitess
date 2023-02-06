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

package workflow

import (
	"fmt"
	"sort"
	"strings"

	"google.golang.org/protobuf/proto"

	"vitess.io/vitess/go/mysql"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
)

// VReplicationStream represents a single stream of a vreplication workflow.
type VReplicationStream struct {
	ID                 uint32
	Workflow           string
	BinlogSource       *binlogdatapb.BinlogSource
	Position           mysql.Position
	WorkflowType       binlogdatapb.VReplicationWorkflowType
	WorkflowSubType    binlogdatapb.VReplicationWorkflowSubType
	DeferSecondaryKeys bool
}

// VReplicationStreams wraps a slice of VReplicationStream objects to provide
// some aggregate functionality.
type VReplicationStreams []*VReplicationStream

// Values returns a string representing the IDs of the VReplicationStreams for
// use in an IN clause.
//
// (TODO|@ajm188) This currently returns the literal ")" if len(streams) == 0.
// We should probably update this function to return the full "IN" clause, and
// then if len(streams) == 0, return "1 != 1" so that we can still execute a
// valid SQL query.
func (streams VReplicationStreams) Values() string {
	buf := &strings.Builder{}
	prefix := "("

	for _, vrs := range streams {
		fmt.Fprintf(buf, "%s%d", prefix, vrs.ID)
		prefix = ", "
	}

	buf.WriteString(")")
	return buf.String()
}

// Workflows returns a list of unique workflow names in the list of vreplication
// streams.
func (streams VReplicationStreams) Workflows() []string {
	set := make(map[string]bool, len(streams))
	for _, vrs := range streams {
		set[vrs.Workflow] = true
	}

	list := make([]string, 0, len(set))
	for k := range set {
		list = append(list, k)
	}

	sort.Strings(list)
	return list
}

// Copy returns a copy of the list of streams. All fields except .Position are
// copied.
func (streams VReplicationStreams) Copy() VReplicationStreams {
	out := make([]*VReplicationStream, len(streams))

	for i, vrs := range streams {
		out[i] = &VReplicationStream{
			ID:           vrs.ID,
			Workflow:     vrs.Workflow,
			BinlogSource: proto.Clone(vrs.BinlogSource).(*binlogdatapb.BinlogSource),
			Position:     vrs.Position,
		}
	}

	return VReplicationStreams(out)
}

// ToSlice unwraps a VReplicationStreams object into the underlying slice of
// VReplicationStream objects.
func (streams VReplicationStreams) ToSlice() []*VReplicationStream {
	return []*VReplicationStream(streams)
}
