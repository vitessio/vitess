package main

// This plugin imports staticauthserver to register the flat-file implementation of AuthServer.

import (
	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/vt/vtgate"
)

func init() {
	vtgate.RegisterPluginInitializer(func() { mysql.InitAuthServerStatic() })
}
