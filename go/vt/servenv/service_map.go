// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package servenv

import (
	"flag"

	"github.com/henryanand/vitess/go/flagutil"
)

var (
	serviceMapFlag flagutil.StringListValue

	// ServiceMap is the exported version of the service map.
	// init() functions will add default values to it.
	// service_map command line parameter will alter the map.
	// Can only be used after servenv.Init has been called.
	ServiceMap = make(map[string]bool)
)

func init() {
	flag.Var(&serviceMapFlag, "service_map", "services to enable / disable")
	onInit(func() {
		updateServiceMap()
	})
}

// InitServiceMapForBsonRpcService will set the default entries for a
// bson rpc to serve the service.
func InitServiceMapForBsonRpcService(name string) {
	ServiceMap["bsonrpc-vt-"+name] = true
	ServiceMap["bsonrpc-auth-vt-"+name] = true
	ServiceMap["bsonrpc-vts-"+name] = true
	ServiceMap["bsonrpc-auth-vts-"+name] = true
	ServiceMap["bsonrpc-unix-"+name] = true
	ServiceMap["bsonrpc-auth-unix-"+name] = true
}

func updateServiceMap() {
	for _, s := range serviceMapFlag {
		if s[0] == '-' {
			delete(ServiceMap, s[1:])
		} else {
			ServiceMap[s] = true
		}
	}
}
