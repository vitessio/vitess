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

package binlogplayer

import "github.com/youtube/vitess/go/sqltypes"

// VtClient is a high level interface to the database.
type VtClient interface {
	Connect() error
	Begin() error
	Commit() error
	Rollback() error
	Close()
	ExecuteFetch(query string, maxrows int) (qr *sqltypes.Result, err error)
}

// VtClientMock is a VtClient that writes to a writer instead of executing
// anything.
// It allows to mock out query results for queries. See AddResult().
type VtClientMock struct {
	Stdout        []string
	results       []*sqltypes.Result
	CommitChannel chan []string
	currentResult int
}

// NewVtClientMock returns a new VtClientMock
func NewVtClientMock() *VtClientMock {
	return &VtClientMock{
		results:       make([]*sqltypes.Result, 0),
		currentResult: -1,
	}
}

// AddResult appends a mocked query result to the end of the list.
// It will be returned exactly once to a client when it's up.
func (dc *VtClientMock) AddResult(result *sqltypes.Result) {
	dc.results = append(dc.results, result)
}

// Connect is part of the VtClient interface
func (dc *VtClientMock) Connect() error {
	return nil
}

// Begin is part of the VtClient interface
func (dc *VtClientMock) Begin() error {
	dc.Stdout = append(dc.Stdout, "BEGIN")
	return nil
}

// Commit is part of the VtClient interface
func (dc *VtClientMock) Commit() error {
	dc.Stdout = append(dc.Stdout, "COMMIT")
	if dc.CommitChannel != nil {
		dc.CommitChannel <- dc.Stdout
		dc.Stdout = nil
	}
	return nil
}

// Rollback is part of the VtClient interface
func (dc *VtClientMock) Rollback() error {
	dc.Stdout = append(dc.Stdout, "ROLLBACK")
	return nil
}

// Close is part of the VtClient interface
func (dc *VtClientMock) Close() {
	return
}

// ExecuteFetch is part of the VtClient interface
func (dc *VtClientMock) ExecuteFetch(query string, maxrows int) (qr *sqltypes.Result, err error) {
	dc.Stdout = append(dc.Stdout, query)
	if dc.currentResult+1 < len(dc.results) {
		dc.currentResult++
	}
	result := dc.results[dc.currentResult]
	return result, nil
}
