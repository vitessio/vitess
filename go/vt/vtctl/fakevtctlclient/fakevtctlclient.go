/*
Copyright 2019 The Vitess Authors.

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

// Package fakevtctlclient contains a fake for the vtctlclient interface.
package fakevtctlclient

import (
	"time"

	"context"

	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/vtctl/vtctlclient"
)

// FakeVtctlClient is a fake which implements the vtctlclient interface.
// The fake can be used to return a specific result for a given command.
// If the command is not registered, an error will be thrown.
type FakeVtctlClient struct {
	*FakeLoggerEventStreamingClient
}

// NewFakeVtctlClient creates a FakeVtctlClient struct.
func NewFakeVtctlClient() *FakeVtctlClient {
	return &FakeVtctlClient{NewFakeLoggerEventStreamingClient()}
}

// FakeVtctlClientFactory always returns the current instance.
func (f *FakeVtctlClient) FakeVtctlClientFactory(addr string) (vtctlclient.VtctlClient, error) {
	return f, nil
}

// ExecuteVtctlCommand is part of the vtctlclient interface.
func (f *FakeVtctlClient) ExecuteVtctlCommand(ctx context.Context, args []string, actionTimeout time.Duration) (logutil.EventStream, error) {
	return f.FakeLoggerEventStreamingClient.StreamResult("" /* addr */, args)
}

// Close is part of the vtctlclient interface.
func (f *FakeVtctlClient) Close() {}
