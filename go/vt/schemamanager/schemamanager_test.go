// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package schemamanager

import (
	"errors"
	"testing"
	"time"

	fakevtgateconn "github.com/youtube/vitess/go/vt/vtgate/fakerpcvtgateconn"
	"golang.org/x/net/context"
)

var (
	errDataSourcerOpen  = errors.New("Open Fail")
	errDataSourcerRead  = errors.New("Read Fail")
	errDataSourcerClose = errors.New("Close Fail")
)

func TestRunSchemaChangesDataSourcerOpenFail(t *testing.T) {
	dataSourcer := newFakeDataSourcer([]string{"select * from test_db"}, true, false, false)
	handler := newFakeHandler()
	fakeConn := newFakeVtGateConn()
	exec := newFakeVtGateExecutor(fakeConn)
	err := Run(dataSourcer, exec, handler, []string{"0", "1", "2"})
	if err != errDataSourcerOpen {
		t.Fatalf("data sourcer open fail, shoud get error: %v, but get error: %v",
			errDataSourcerOpen, err)
	}
}

func TestRunSchemaChangesDataSourcerReadFail(t *testing.T) {
	dataSourcer := newFakeDataSourcer([]string{"select * from test_db"}, false, true, false)
	handler := newFakeHandler()
	fakeConn := newFakeVtGateConn()
	exec := newFakeVtGateExecutor(fakeConn)
	err := Run(dataSourcer, exec, handler, []string{"0", "1", "2"})
	if err != errDataSourcerRead {
		t.Fatalf("data sourcer read fail, shoud get error: %v, but get error: %v",
			errDataSourcerRead, err)
	}
	if !handler.onDataSourcerReadFailTriggered {
		t.Fatalf("event handler should call OnDataSourcerReadFail but it didn't")
	}
}

func TestRunSchemaChangesValidationFail(t *testing.T) {
	dataSourcer := newFakeDataSourcer([]string{"invalid sql"}, false, false, false)
	handler := newFakeHandler()
	fakeConn := newFakeVtGateConn()
	exec := newFakeVtGateExecutor(fakeConn)
	err := Run(dataSourcer, exec, handler, []string{"0", "1", "2"})
	if err == nil {
		t.Fatalf("run schema change should fail due to executor.Open fail")
	}
}

func TestRunSchemaChanges(t *testing.T) {
	dataSourcer := NewSimepleDataSourcer("select * from test_db;")
	handler := newFakeHandler()
	fakeConn := newFakeVtGateConn()
	exec := newFakeVtGateExecutor(fakeConn)
	err := Run(dataSourcer, exec, handler, []string{"0", "1", "2"})
	if err != nil {
		t.Fatalf("schema change should success but get error: %v", err)
	}
	if !handler.onDataSourcerReadSuccessTriggered {
		t.Fatalf("event handler should call OnDataSourcerReadSuccess but it didn't")
	}
	if handler.onDataSourcerReadFailTriggered {
		t.Fatalf("event handler should not call OnDataSourcerReadFail but it did")
	}
	if !handler.onValidationSuccessTriggered {
		t.Fatalf("event handler should call OnDataSourcerValidateSuccess but it didn't")
	}
	if handler.onValidationFailTriggered {
		t.Fatalf("event handler should not call OnValidationFail but it did")
	}
	if !handler.onExecutorCompleteTriggered {
		t.Fatalf("event handler should call OnExecutorComplete but it didn't")
	}
}

func newFakeVtGateConn() *fakevtgateconn.FakeVTGateConn {
	return fakevtgateconn.NewFakeVTGateConn(context.Background(), "", 1*time.Second)
}

func newFakeVtGateExecutor(conn *fakevtgateconn.FakeVTGateConn) *VtGateExecutor {
	return NewVtGateExecutor(
		"test_keyspace",
		conn,
		1*time.Second)
}

type fakeDataSourcer struct {
	sqls      []string
	openFail  bool
	readFail  bool
	closeFail bool
}

func newFakeDataSourcer(sqls []string, openFail bool, readFail bool, closeFail bool) *fakeDataSourcer {
	return &fakeDataSourcer{sqls, openFail, readFail, closeFail}
}

func (sourcer fakeDataSourcer) Open() error {
	if sourcer.openFail {
		return errDataSourcerOpen
	}
	return nil
}

func (sourcer fakeDataSourcer) Read() ([]string, error) {
	if sourcer.readFail {
		return nil, errDataSourcerRead
	}
	return sourcer.sqls, nil
}

func (sourcer fakeDataSourcer) Close() error {
	if sourcer.closeFail {
		return errDataSourcerClose
	}
	return nil
}

type fakeEventHandler struct {
	onDataSourcerReadSuccessTriggered bool
	onDataSourcerReadFailTriggered    bool
	onValidationSuccessTriggered      bool
	onValidationFailTriggered         bool
	onExecutorCompleteTriggered       bool
}

func newFakeHandler() *fakeEventHandler {
	return &fakeEventHandler{}
}

func (handler *fakeEventHandler) OnDataSourcerReadSuccess([]string) error {
	handler.onDataSourcerReadSuccessTriggered = true
	return nil
}

func (handler *fakeEventHandler) OnDataSourcerReadFail(err error) error {
	handler.onDataSourcerReadFailTriggered = true
	return err
}

func (handler *fakeEventHandler) OnValidationSuccess([]string) error {
	handler.onValidationSuccessTriggered = true
	return nil
}

func (handler *fakeEventHandler) OnValidationFail(err error) error {
	handler.onValidationFailTriggered = true
	return err
}

func (handler *fakeEventHandler) OnExecutorComplete(*ExecuteResult) error {
	handler.onExecutorCompleteTriggered = true
	return nil
}

var _ EventHandler = (*fakeEventHandler)(nil)
var _ DataSourcer = (*fakeDataSourcer)(nil)
