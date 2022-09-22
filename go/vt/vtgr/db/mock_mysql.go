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

package db

import (
	reflect "reflect"
	"strconv"

	gomock "github.com/golang/mock/gomock"

	mysql "vitess.io/vitess/go/mysql"
	inst "vitess.io/vitess/go/vt/vtorc/inst"
)

// MockAgent is a mock of Agent interface
type MockAgent struct {
	ctrl     *gomock.Controller
	recorder *MockAgentMockRecorder
}

// MockAgentMockRecorder is the mock recorder for MockAgent
type MockAgentMockRecorder struct {
	mock *MockAgent
}

// NewMockAgent creates a new mock instance
func NewMockAgent(ctrl *gomock.Controller) *MockAgent {
	mock := &MockAgent{ctrl: ctrl}
	mock.recorder = &MockAgentMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use
func (m *MockAgent) EXPECT() *MockAgentMockRecorder {
	return m.recorder
}

// BootstrapGroupLocked mocks base method
func (m *MockAgent) BootstrapGroupLocked(instanceKey *inst.InstanceKey) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "BootstrapGroupLocked", instanceKey)
	ret0, _ := ret[0].(error)
	return ret0
}

// BootstrapGroupLocked indicates an expected call of BootstrapGroupLocked
func (mr *MockAgentMockRecorder) BootstrapGroupLocked(instanceKey any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "BootstrapGroupLocked", reflect.TypeOf((*MockAgent)(nil).BootstrapGroupLocked), instanceKey)
}

// RebootstrapGroupLocked mocks base method
func (m *MockAgent) RebootstrapGroupLocked(instanceKey *inst.InstanceKey, name string) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "RebootstrapGroupLocked", instanceKey, name)
	ret0, _ := ret[0].(error)
	return ret0
}

// RebootstrapGroupLocked indicates an expected call of RebootstrapGroupLocked
func (mr *MockAgentMockRecorder) RebootstrapGroupLocked(instanceKey, name any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "RebootstrapGroupLocked", reflect.TypeOf((*MockAgent)(nil).RebootstrapGroupLocked), instanceKey, name)
}

// StopGroupLocked mocks base method
func (m *MockAgent) StopGroupLocked(instanceKey *inst.InstanceKey) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "StopGroupLocked", instanceKey)
	ret0, _ := ret[0].(error)
	return ret0
}

// StopGroupLocked indicates an expected call of StopGroupLocked
func (mr *MockAgentMockRecorder) StopGroupLocked(instanceKey any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "StopGroupLocked", reflect.TypeOf((*MockAgent)(nil).StopGroupLocked), instanceKey)
}

// JoinGroupLocked mocks base method
func (m *MockAgent) JoinGroupLocked(instanceKey, primaryKey *inst.InstanceKey) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "JoinGroupLocked", instanceKey, primaryKey)
	ret0, _ := ret[0].(error)
	return ret0
}

// JoinGroupLocked indicates an expected call of JoinGroupLocked
func (mr *MockAgentMockRecorder) JoinGroupLocked(instanceKey, primaryKey any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "JoinGroupLocked", reflect.TypeOf((*MockAgent)(nil).JoinGroupLocked), instanceKey, primaryKey)
}

// SetReadOnly mocks base method
func (m *MockAgent) SetReadOnly(instanceKey *inst.InstanceKey, readOnly bool) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "SetReadOnly", instanceKey, readOnly)
	ret0, _ := ret[0].(error)
	return ret0
}

// SetReadOnly indicates an expected call of SetReadOnly
func (mr *MockAgentMockRecorder) SetReadOnly(instanceKey, readOnly any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "SetReadOnly", reflect.TypeOf((*MockAgent)(nil).SetReadOnly), instanceKey, readOnly)
}

// FetchApplierGTIDSet mocks base method
func (m *MockAgent) FetchApplierGTIDSet(instanceKey *inst.InstanceKey) (mysql.GTIDSet, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "FetchApplierGTIDSet", instanceKey)
	ret0, _ := ret[0].(mysql.GTIDSet)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// FetchApplierGTIDSet indicates an expected call of FetchApplierGTIDSet
func (mr *MockAgentMockRecorder) FetchApplierGTIDSet(instanceKey any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "FetchApplierGTIDSet", reflect.TypeOf((*MockAgent)(nil).FetchApplierGTIDSet), instanceKey)
}

// Failover mocks base method
func (m *MockAgent) Failover(instance *inst.InstanceKey) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Failover", instance)
	ret0, _ := ret[0].(error)
	return ret0
}

// Failover indicates an expected call of Failover
func (mr *MockAgentMockRecorder) Failover(instance any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Failover", reflect.TypeOf((*MockAgent)(nil).Failover), instance)
}

// FetchGroupView mocks base method
func (m *MockAgent) FetchGroupView(alias string, instanceKey *inst.InstanceKey) (*GroupView, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "FetchGroupView", alias, instanceKey)
	ret0, _ := ret[0].(*GroupView)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// FetchGroupView indicates an expected call of FetchGroupView
func (mr *MockAgentMockRecorder) FetchGroupView(alias, instanceKey any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "FetchGroupView", reflect.TypeOf((*MockAgent)(nil).FetchGroupView), alias, instanceKey)
}

// TestGroupState mocks a row from mysql
type TestGroupState struct {
	MemberHost, MemberPort, MemberState, MemberRole string
}

// BuildGroupView builds gruop view from input
func BuildGroupView(alias, groupName, host string, port int, readOnly bool, stalenessResult int, inputs []TestGroupState) *GroupView {
	view := NewGroupView(alias, host, port)
	view.GroupName = groupName
	// group_name, member_host, member_port, member_state, member_role, is_local
	for _, row := range inputs {
		memberPort, _ := strconv.Atoi(row.MemberPort)
		member := NewGroupMember(
			row.MemberState,
			row.MemberRole,
			row.MemberHost,
			memberPort,
			false)
		if host == row.MemberHost && port == memberPort {
			member.ReadOnly = readOnly
		}
		view.UnresolvedMembers = append(view.UnresolvedMembers, member)
		view.HeartbeatStaleness = stalenessResult
	}
	return view
}
