/*
Copyright 2025 The Vitess Authors.

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

package main

// Imports and registers the gRPC tabletmanager client.
// This is needed when --server=internal as the vtctldclient
// binary will then not only need to talk to the topo server
// directly but it will also need to talk to tablets directly
// via tmclient.

import (
	_ "vitess.io/vitess/go/vt/vttablet/grpctmclient"
	_ "vitess.io/vitess/go/vt/vttablet/tmclient"
)
