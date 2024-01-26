/*
Copyright 2024 The Vitess Authors.

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

package topo

import (
	"context"
	"reflect"
	"testing"
)

func TestGetServingShards(t *testing.T) {
	type fields struct {
		globalCell         Conn
		globalReadOnlyCell Conn
		factory            Factory
		cellConns          map[string]cellConn
	}
	type args struct {
		ctx      context.Context
		keyspace string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    []*ShardInfo
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ts := &Server{
				globalCell:         tt.fields.globalCell,
				globalReadOnlyCell: tt.fields.globalReadOnlyCell,
				factory:            tt.fields.factory,
				cellConns:          tt.fields.cellConns,
			}
			got, err := ts.GetServingShards(tt.args.ctx, tt.args.keyspace)
			if (err != nil) != tt.wantErr {
				t.Errorf("Server.GetServingShards() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Server.GetServingShards() = %v, want %v", got, tt.want)
			}
		})
	}
}
