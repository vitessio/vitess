package main

// This plugin imports ldapauthserver to register the LDAP implementation of AuthServer.

import (
	"vitess.io/vitess/go/mysql/ldapauthserver"
	"vitess.io/vitess/go/vt/vtgate"
)

func init() {
	vtgate.RegisterPluginInitializer(func() { ldapauthserver.Init() })
}
