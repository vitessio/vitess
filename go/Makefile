# Copyright 2012, Google Inc. All rights reserved.
# Use of this source code is governed by a BSD-style license that can
# be found in the LICENSE file.

MAKEFLAGS = -s

all:
	cd cmd/mysqlctl; go build
	cd cmd/normalizer; go build
	cd cmd/vtaction; go build
	cd cmd/vtclient2; go build
	cd cmd/vtctl; go build
	cd cmd/vtocc; go build
	cd cmd/vttablet; go build
	cd cmd/zk; go build
	cd cmd/zkctl; go build

# alphabetically ordered tests
# the ones that are commented out don't pass
test:
	cd bson; go test
	cd bytes2; go test
	cd cache; go test
	cd hack; go test
#	cd logfile; go test
	if [ -e "/usr/bin/memcached" ]; then \
		cd memcache; exec go test ; \
	fi
	cd pools; go test
	cd rpcplus; go test
	cd rpcplus/jsonrpc; go test
	cd rpcwrap/auth; go test
	cd timer; go test
	cd umgmt; go test
#	cd vt/client2; go test
#	cd vt/mysqlctl; go test
	cd vt/sqlparser; go test
	cd vt/tabletmanager; go test
	cd vt/tabletserver; go test
#	cd vt/wrangler; go test
#	cd zk; go test
#	cd zk/zkctl; go test

clean:
	cd cmd/mysqlctl; go clean
	cd cmd/normalizer; go clean
	cd cmd/vtaction; go clean
	cd cmd/vtclient2; go clean
	cd cmd/vtctl; go clean
	cd cmd/vtocc; go clean
	cd cmd/vttablet; go clean
	cd cmd/zk; go clean
	cd cmd/zkctl; go clean
