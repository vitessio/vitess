// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package vtctlclient

import (
	"flag"
	"fmt"
	"time"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/vt/logutil"
)

var vtctlClientProtocol = flag.String("vtctl_client_protocol", "gorpc", "the protocol to use to talk to the vtctl server")

// VtctlClient defines the interface used to send remote vtctl commands
type VtctlClient interface {
	// ExecuteVtctlCommand will execute the command remotely
	ExecuteVtctlCommand(args []string, actionTimeout, lockTimeout time.Duration, sendReply func(*logutil.LoggerEvent) error) error
}

type VtctlClientFactory func(addr string) (VtctlClient, error)

var vtctlClientFactories = make(map[string]VtctlClientFactory)

func RegisterVtctlClientFactory(name string, factory VtctlClientFactory) {
	if _, ok := vtctlClientFactories[name]; ok {
		log.Fatalf("RegisterVtctlClientFactory %s already exists", name)
	}
	vtctlClientFactories[name] = factory
}

func NewVtctlClient(addr string) (VtctlClient, error) {
	factory, ok := vtctlClientFactories[*vtctlClientProtocol]
	if !ok {
		return nil, fmt.Errorf("unknown vtctl client protocol: %v", *vtctlClientProtocol)
	}
	return factory(addr)
}
