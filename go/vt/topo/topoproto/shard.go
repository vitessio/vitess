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

package topoproto

import (
	"encoding/hex"
	"fmt"
	"html/template"
	"strings"

	"vitess.io/vitess/go/vt/key"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

// KeyspaceShardString returns a "keyspace/shard" string taking
// keyspace and shard as separate inputs.
func KeyspaceShardString(keyspace, shard string) string {
	return fmt.Sprintf("%v/%v", keyspace, shard)
}

// ParseKeyspaceShard parse a "keyspace/shard" string and extract
// both keyspace and shard
func ParseKeyspaceShard(param string) (string, string, error) {
	keySpaceShard := strings.Split(param, "/")
	if len(keySpaceShard) != 2 {
		return "", "", fmt.Errorf("invalid shard path: %v", param)
	}
	return keySpaceShard[0], keySpaceShard[1], nil
}

// SourceShardString returns a printable view of a SourceShard.
func SourceShardString(source *topodatapb.Shard_SourceShard) string {
	return fmt.Sprintf("SourceShard(%v,%v/%v)", source.Uid, source.Keyspace, source.Shard)
}

// SourceShardAsHTML returns a HTML version of the object.
func SourceShardAsHTML(source *topodatapb.Shard_SourceShard) template.HTML {
	result := fmt.Sprintf("<b>Uid</b>: %v</br>\n<b>Source</b>: %v/%v</br>\n", source.Uid, source.Keyspace, source.Shard)
	if key.KeyRangeIsPartial(source.KeyRange) {
		result += fmt.Sprintf("<b>KeyRange</b>: %v-%v</br>\n",
			hex.EncodeToString(source.KeyRange.Start),
			hex.EncodeToString(source.KeyRange.End))
	}
	if len(source.Tables) > 0 {
		result += fmt.Sprintf("<b>Tables</b>: %v</br>\n",
			strings.Join(source.Tables, " "))
	}
	return template.HTML(result)
}
