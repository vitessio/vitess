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

package sync2

// FakeConsolidator satisies the Consolidator interface and can be used to mock
// how Vitess interacts with the Consolidator.
type FakeConsolidator struct {
	// CreateCalls can be used to inspect Create calls.
	CreateCalls []string
	// CreateReturns can be used to inspect Create return values.
	CreateReturns []PendingResult
	// CreateReturnCreated pre-configures the "created" bool returned in Create
	// calls.
	CreateReturnCreated bool
	// CreateReturnPendingResult pre-configures the PendingResult returned in
	// Create calls.
	CreateReturnPendingResult PendingResult
	// RecordCalls can be usd to inspect Record calls.
	RecordCalls []string
}

// FakePendingResult satisies the PendingResult interface and can be used to
// mock how Vitess interacts with the Consolidator.
type FakePendingResult struct {
	// BroadcastCalls can be used to inspect Broadcast calls.
	BroadcastCalls int
	// WaitCalls can be used to inspect Wait calls.
	WaitCalls int
	err       error
	result    any
}

var (
	_ Consolidator  = &FakeConsolidator{}
	_ PendingResult = &FakePendingResult{}
)

// NewFakeConsolidator creates a new FakeConsolidator.
func NewFakeConsolidator() *FakeConsolidator {
	return &FakeConsolidator{}
}

// Create records the Create call for later verification, and returns a
// pre-configured PendingResult and "created" bool.
func (fc *FakeConsolidator) Create(sql string) (PendingResult, bool) {
	fc.CreateCalls = append(fc.CreateCalls, sql)
	fc.CreateReturns = append(fc.CreateReturns, fc.CreateReturnPendingResult)
	return fc.CreateReturnPendingResult, fc.CreateReturnCreated
}

// Record records the Record call for later verification.
func (fc *FakeConsolidator) Record(sql string) {
	fc.RecordCalls = append(fc.RecordCalls, sql)
}

// Items is currently a no-op.
func (fc *FakeConsolidator) Items() []ConsolidatorCacheItem {
	return nil
}

// Broadcast records the Broadcast call for later verification.
func (fr *FakePendingResult) Broadcast() {
	fr.BroadcastCalls++
}

// Err returns the pre-configured error.
func (fr *FakePendingResult) Err() error {
	return fr.err
}

// Err returns the pre-configured Result.
func (fr *FakePendingResult) Result() any {
	return fr.result
}

// SetErr stores the err, which can be retrieved with Err.
func (fr *FakePendingResult) SetErr(err error) {
	fr.err = err
}

// SetResult stores the result, which can be retrieved with Result.
func (fr *FakePendingResult) SetResult(result any) {
	fr.result = result
}

// Wait records the Wait call for later verification.
func (fr *FakePendingResult) Wait() {
	fr.WaitCalls++
}
