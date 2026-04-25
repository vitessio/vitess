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

package cli

// Imports and register the gRPC tabletmanager client. Also wires gossip
// TLS so the gossip dialer reuses the tablet-manager gRPC client's
// transport security configuration.

import (
	"vitess.io/vitess/go/vt/vttablet/grpctmclient"
	"vitess.io/vitess/go/vt/vttablet/tabletmanager"
)

// init wires the tabletmanager gossip dialer to the grpctmclient's
// SecureDialOption so gossip picks up the same TLS configuration as the
// rest of the tablet-manager gRPC traffic. Done here rather than in
// tabletmanager itself to avoid an import cycle with grpctmclient's
// tests (which import tabletmanager).
func init() {
	tabletmanager.GossipSecureDialOption = grpctmclient.SecureDialOption
}
