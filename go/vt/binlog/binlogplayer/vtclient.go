// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package binlogplayer

import "github.com/youtube/vitess/go/sqltypes"

// VtClient is a high level interface to the database
type VtClient interface {
	Connect() error
	Begin() error
	Commit() error
	Rollback() error
	Close()
	ExecuteFetch(query string, maxrows int, wantfields bool) (qr *sqltypes.Result, err error)
}

// VtClientMock is a VtClient that writes to a writer instead of executing
// anything
type VtClientMock struct {
	Stdout        []string
	Result        *sqltypes.Result
	CommitChannel chan []string
}

// NewVtClientMock returns a new VtClientMock
func NewVtClientMock() *VtClientMock {
	return &VtClientMock{
		Result: &sqltypes.Result{
			Fields:       nil,
			RowsAffected: 1,
			InsertID:     0,
			Rows:         nil,
		},
	}
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
func (dc *VtClientMock) ExecuteFetch(query string, maxrows int, wantfields bool) (qr *sqltypes.Result, err error) {
	dc.Stdout = append(dc.Stdout, query)
	return dc.Result, nil
}
