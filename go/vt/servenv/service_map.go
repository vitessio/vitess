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

package servenv

import (
	"github.com/spf13/pflag"

	"vitess.io/vitess/go/vt/log"
)

var (
	serviceMapFlag []string

	// serviceMap is the used version of the service map.
	// init() functions can add default values to it (using InitServiceMap).
	// service_map command line parameter will alter the map.
	// Can only be used after servenv.Init has been called.
	serviceMap = make(map[string]bool)
)

// RegisterServiceMapFlag registers an OnParse hook to install the
// `--service_map` flag for a given cmd. It must be called before ParseFlags or
// ParseFlagsWithArgs.
func RegisterServiceMapFlag() {
	OnParse(func(fs *pflag.FlagSet) {
		fs.StringSliceVar(&serviceMapFlag, "service_map", serviceMapFlag, "comma separated list of services to enable (or disable if prefixed with '-') Example: grpc-queryservice")
	})
	OnInit(updateServiceMap)
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

// checkServiceMap returns if we should register a RPC service
// (and also logs how to enable / disable it)
func checkServiceMap(protocol, name string) bool {
	if serviceMap[protocol+"-"+name] {
		log.Infof("Registering %v for %v, disable it with -%v-%v service_map parameter", name, protocol, protocol, name)
		return true
	}
	log.Infof("Not registering %v for %v, enable it with %v-%v service_map parameter", name, protocol, protocol, name)
	return false
}
