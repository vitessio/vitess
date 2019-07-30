package main

// This plugin imports clientcert to register the client certificate implementation of AuthServer.

import (
	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/vt/vtgate"
)

func init() {
	vtgate.RegisterPluginInitializer(func() { mysql.InitAuthServerClientCert() })
}
