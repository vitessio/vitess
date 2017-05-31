/*
Copyright 2017 Google Inc.

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
