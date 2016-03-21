// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package servenv

import (
	"flag"

	log "github.com/golang/glog"

	"github.com/youtube/vitess/go/flagutil"
)

var (
	serviceMapFlag flagutil.StringListValue

	// serviceMap is the used version of the service map.
	// init() functions can add default values to it (using InitServiceMap).
	// service_map command line parameter will alter the map.
	// Can only be used after servenv.Init has been called.
	serviceMap = make(map[string]bool)
)

func init() {
	flag.Var(&serviceMapFlag, "service_map", "comma separated list of services to enable (or disable if prefixed with '-') Example: grpc-vtworker")
	onInit(func() {
		updateServiceMap()
	})
}

// InitServiceMap will set the default value for a protocol/name to be served.
func InitServiceMap(protocol, name string) {
	serviceMap[protocol+"-"+name] = true
}

// updateServiceMap takes the command line parameter, and updates the
// ServiceMap accordingly
func updateServiceMap() {
	for _, s := range serviceMapFlag {
		if s[0] == '-' {
			delete(serviceMap, s[1:])
		} else {
			serviceMap[s] = true
		}
	}
}

// CheckServiceMap returns if we should register a RPC service
// (and also logs how to enable / disable it)
func CheckServiceMap(protocol, name string) bool {
	if serviceMap[protocol+"-"+name] {
		log.Infof("Registering %v for %v, disable it with -%v-%v service_map parameter", name, protocol, protocol, name)
		return true
	}
	log.Infof("Not registering %v for %v, enable it with %v-%v service_map parameter", name, protocol, protocol, name)
	return false
}
