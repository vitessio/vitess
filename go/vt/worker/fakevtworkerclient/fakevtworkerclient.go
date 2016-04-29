// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package fakevtworkerclient contains a fake for the vtworkerclient interface.
package fakevtworkerclient

import (
	"time"

	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/vt/logutil"
	"github.com/youtube/vitess/go/vt/vtctl/fakevtctlclient"
	"github.com/youtube/vitess/go/vt/worker/vtworkerclient"
)

// FakeVtworkerClient is a fake which implements the vtworkerclient interface.
// The fake can be used to return a specific result for a given command.
// If the command is not registered, an error will be thrown.
type FakeVtworkerClient struct {
	*fakevtctlclient.FakeLoggerEventStreamingClient
}

// NewFakeVtworkerClient creates a FakeVtworkerClient struct.
func NewFakeVtworkerClient() *FakeVtworkerClient {
	return &FakeVtworkerClient{fakevtctlclient.NewFakeLoggerEventStreamingClient()}
}

// FakeVtworkerClientFactory always returns the current instance.
func (f *FakeVtworkerClient) FakeVtworkerClientFactory(addr string, dialTimeout time.Duration) (vtworkerclient.Client, error) {
	return f, nil
}

// ExecuteVtworkerCommand is part of the vtworkerclient interface.
func (f *FakeVtworkerClient) ExecuteVtworkerCommand(ctx context.Context, args []string) (logutil.EventStream, error) {
	return f.FakeLoggerEventStreamingClient.StreamResult(args)
}

// Close is part of the vtworkerclient interface.
func (f *FakeVtworkerClient) Close() {}
