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

package logutil

import (
	logutilpb "vitess.io/vitess/go/vt/proto/logutil"
)

// This file contains a few functions to help with proto3.

// EventStream is an interface used by RPC clients when the streaming
// RPC returns a stream of log events.
type EventStream interface {
	// Recv returns the next event in the logs.
	// If there are no more, it will return io.EOF.
	Recv() (*logutilpb.Event, error)
}
