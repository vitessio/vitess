package main

// This plugin imports staticauthserver to register the flat-file implementation of AuthServer.

import (
	"github.com/youtube/vitess/go/mysqlconn"
	"github.com/youtube/vitess/go/vt/vtgate"
)

func init() {
	vtgate.RegisterPluginInitializer(func() { mysqlconn.InitAuthServerStatic() })
}
