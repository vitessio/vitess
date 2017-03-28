package main

// This plugin imports ldapauthserver to register the LDAP implementation of AuthServer.

import (
	"github.com/youtube/vitess/go/mysqlconn/ldapauthserver"
	"github.com/youtube/vitess/go/vt/vtgate"
)

func init() {
	vtgate.RegisterPluginInitializer(func() { ldapauthserver.Init() })
}
