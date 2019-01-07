package main

// This plugin imports clientcert to register the client certificate implementation of AuthServer.

import (
	"vitess.io/vitess/go/mysql/clientcert"
	"vitess.io/vitess/go/vt/vtgate"
)

func init() {
	vtgate.RegisterPluginInitializer(func() { clientcert.Init() })
}
