// Copyright 2016 Google Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package bttest

import (
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/net/context"
	btapb "google.golang.org/genproto/googleapis/bigtable/admin/v2"
	btpb "google.golang.org/genproto/googleapis/bigtable/v2"
	"google.golang.org/grpc"
	"strconv"
)

func TestConcurrentMutationsReadModifyAndGC(t *testing.T) {
	s := &server{
		tables: make(map[string]*table),
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if _, err := s.CreateTable(
		ctx,
		&btapb.CreateTableRequest{Parent: "cluster", TableId: "t"}); err != nil {
		t.Fatal(err)
	}
	const name = `cluster/tables/t`
	tbl := s.tables[name]
	req := &btapb.ModifyColumnFamiliesRequest{
		Name: name,
		Modifications: []*btapb.ModifyColumnFamiliesRequest_Modification{{
			Id:  "cf",
			Mod: &btapb.ModifyColumnFamiliesRequest_Modification_Create{&btapb.ColumnFamily{}},
		}},
	}
	_, err := s.ModifyColumnFamilies(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	req = &btapb.ModifyColumnFamiliesRequest{
		Name: name,
		Modifications: []*btapb.ModifyColumnFamiliesRequest_Modification{{
			Id: "cf",
			Mod: &btapb.ModifyColumnFamiliesRequest_Modification_Update{&btapb.ColumnFamily{
				GcRule: &btapb.GcRule{Rule: &btapb.GcRule_MaxNumVersions{1}},
			}},
		}},
	}
	if _, err := s.ModifyColumnFamilies(ctx, req); err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	var ts int64
	ms := func() []*btpb.Mutation {
		return []*btpb.Mutation{{
			Mutation: &btpb.Mutation_SetCell_{&btpb.Mutation_SetCell{
				FamilyName:      "cf",
				ColumnQualifier: []byte(`col`),
				TimestampMicros: atomic.AddInt64(&ts, 1000),
			}},
		}}
	}

	rmw := func() *btpb.ReadModifyWriteRowRequest {
		return &btpb.ReadModifyWriteRowRequest{
			TableName: name,
			RowKey:    []byte(fmt.Sprint(rand.Intn(100))),
			Rules: []*btpb.ReadModifyWriteRule{{
				FamilyName:      "cf",
				ColumnQualifier: []byte("col"),
				Rule:            &btpb.ReadModifyWriteRule_IncrementAmount{1},
			}},
		}
	}
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for ctx.Err() == nil {
				req := &btpb.MutateRowRequest{
					TableName: name,
					RowKey:    []byte(fmt.Sprint(rand.Intn(100))),
					Mutations: ms(),
				}
				s.MutateRow(ctx, req)
			}
		}()
		wg.Add(1)
		go func() {
			defer wg.Done()
			for ctx.Err() == nil {
				_, _ = s.ReadModifyWriteRow(ctx, rmw())
			}
		}()

		wg.Add(1)
		go func() {
			defer wg.Done()
			tbl.gc()
		}()
	}
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(1 * time.Second):
		t.Error("Concurrent mutations and GCs haven't completed after 1s")
	}
}

func TestCreateTableWithFamily(t *testing.T) {
	// The Go client currently doesn't support creating a table with column families
	// in one operation but it is allowed by the API. This must still be supported by the
	// fake server so this test lives here instead of in the main bigtable
	// integration test.
	s := &server{
		tables: make(map[string]*table),
	}
	ctx := context.Background()
	newTbl := btapb.Table{
		ColumnFamilies: map[string]*btapb.ColumnFamily{
			"cf1": {GcRule: &btapb.GcRule{Rule: &btapb.GcRule_MaxNumVersions{123}}},
			"cf2": {GcRule: &btapb.GcRule{Rule: &btapb.GcRule_MaxNumVersions{456}}},
		},
	}
	cTbl, err := s.CreateTable(ctx, &btapb.CreateTableRequest{Parent: "cluster", TableId: "t", Table: &newTbl})
	if err != nil {
		t.Fatalf("Creating table: %v", err)
	}
	tbl, err := s.GetTable(ctx, &btapb.GetTableRequest{Name: cTbl.Name})
	if err != nil {
		t.Fatalf("Getting table: %v", err)
	}
	cf := tbl.ColumnFamilies["cf1"]
	if cf == nil {
		t.Fatalf("Missing col family cf1")
	}
	if got, want := cf.GcRule.GetMaxNumVersions(), int32(123); got != want {
		t.Errorf("Invalid MaxNumVersions: wanted:%d, got:%d", want, got)
	}
	cf = tbl.ColumnFamilies["cf2"]
	if cf == nil {
		t.Fatalf("Missing col family cf2")
	}
	if got, want := cf.GcRule.GetMaxNumVersions(), int32(456); got != want {
		t.Errorf("Invalid MaxNumVersions: wanted:%d, got:%d", want, got)
	}
}

type MockSampleRowKeysServer struct {
	responses []*btpb.SampleRowKeysResponse
	grpc.ServerStream
}

func (s *MockSampleRowKeysServer) Send(resp *btpb.SampleRowKeysResponse) error {
	s.responses = append(s.responses, resp)
	return nil
}

func TestSampleRowKeys(t *testing.T) {
	s := &server{
		tables: make(map[string]*table),
	}
	ctx := context.Background()
	newTbl := btapb.Table{
		ColumnFamilies: map[string]*btapb.ColumnFamily{
			"cf": {GcRule: &btapb.GcRule{Rule: &btapb.GcRule_MaxNumVersions{1}}},
		},
	}
	tbl, err := s.CreateTable(ctx, &btapb.CreateTableRequest{Parent: "cluster", TableId: "t", Table: &newTbl})
	if err != nil {
		t.Fatalf("Creating table: %v", err)
	}

	// Populate the table
	val := []byte("value")
	rowCount := 1000
	for i := 0; i < rowCount; i++ {
		req := &btpb.MutateRowRequest{
			TableName: tbl.Name,
			RowKey:    []byte("row-" + strconv.Itoa(i)),
			Mutations: []*btpb.Mutation{{
				Mutation: &btpb.Mutation_SetCell_{&btpb.Mutation_SetCell{
					FamilyName:      "cf",
					ColumnQualifier: []byte("col"),
					TimestampMicros: 0,
					Value:           val,
				}},
			}},
		}
		if _, err := s.MutateRow(ctx, req); err != nil {
			t.Fatalf("Populating table: %v", err)
		}
	}

	mock := &MockSampleRowKeysServer{}
	if err := s.SampleRowKeys(&btpb.SampleRowKeysRequest{TableName: tbl.Name}, mock); err != nil {
		t.Errorf("SampleRowKeys error: %v", err)
	}
	if len(mock.responses) == 0 {
		t.Fatal("Response count: got 0, want > 0")
	}
	// Make sure the offset of the final response is the offset of the final row
	got := mock.responses[len(mock.responses)-1].OffsetBytes
	want := int64((rowCount - 1) * len(val))
	if got != want {
		t.Errorf("Invalid offset: got %d, want %d", got, want)
	}
}

func TestDropRowRange(t *testing.T) {
	s := &server{
		tables: make(map[string]*table),
	}
	ctx := context.Background()
	newTbl := btapb.Table{
		ColumnFamilies: map[string]*btapb.ColumnFamily{
			"cf": {GcRule: &btapb.GcRule{Rule: &btapb.GcRule_MaxNumVersions{1}}},
		},
	}
	tblInfo, err := s.CreateTable(ctx, &btapb.CreateTableRequest{Parent: "cluster", TableId: "t", Table: &newTbl})
	if err != nil {
		t.Fatalf("Creating table: %v", err)
	}

	tbl := s.tables[tblInfo.Name]

	// Populate the table
	prefixes := []string{"AAA", "BBB", "CCC", "DDD"}
	count := 3

	for _, prefix := range prefixes {
		for i := 0; i < count; i++ {
			req := &btpb.MutateRowRequest{
				TableName: tblInfo.Name,
				RowKey:    []byte(prefix + strconv.Itoa(i)),
				Mutations: []*btpb.Mutation{{
					Mutation: &btpb.Mutation_SetCell_{&btpb.Mutation_SetCell{
						FamilyName:      "cf",
						ColumnQualifier: []byte("col"),
						TimestampMicros: 0,
						Value:           []byte{},
					}},
				}},
			}
			if _, err := s.MutateRow(ctx, req); err != nil {
				t.Fatalf("Populating table: %v", err)
			}
		}
	}

	tblSize := len(tbl.rows)
	req := &btapb.DropRowRangeRequest{
		Name:tblInfo.Name,
		Target: &btapb.DropRowRangeRequest_RowKeyPrefix{[]byte("AAA")},
	}
	if _, err = s.DropRowRange(ctx, req); err != nil {
		t.Fatalf("Dropping first range: %v", err)
	}
	got, want := len(tbl.rows), tblSize - count
	if got != want {
		t.Errorf("Row count after first drop: got %d (%v), want %d", got, tbl.rows, want)
	}

	req = &btapb.DropRowRangeRequest{
		Name:tblInfo.Name,
		Target: &btapb.DropRowRangeRequest_RowKeyPrefix{[]byte("DDD")},
	}
	if _, err = s.DropRowRange(ctx, req); err != nil {
		t.Fatalf("Dropping second range: %v", err)
	}
	got, want = len(tbl.rows), tblSize - (2 * count)
	if got != want  {
		t.Errorf("Row count after second drop: got %d (%v), want %d", got, tbl.rows, want)
	}

	req = &btapb.DropRowRangeRequest{
		Name:tblInfo.Name,
		Target: &btapb.DropRowRangeRequest_RowKeyPrefix{[]byte("XXX")},
	}
	if _, err = s.DropRowRange(ctx, req); err != nil {
		t.Fatalf("Dropping invalid range: %v", err)
	}
	got, want = len(tbl.rows), tblSize - (2 * count)
	if got != want  {
		t.Errorf("Row count after invalid drop: got %d (%v), want %d", got, tbl.rows, want)
	}

	req = &btapb.DropRowRangeRequest{
		Name:tblInfo.Name,
		Target: &btapb.DropRowRangeRequest_DeleteAllDataFromTable{true},
	}
	if _, err = s.DropRowRange(ctx, req); err != nil {
		t.Fatalf("Dropping all data: %v", err)
	}
	got, want = len(tbl.rows), 0
	if got != want  {
		t.Errorf("Row count after drop all: got %d, want %d", got, want)
	}
}