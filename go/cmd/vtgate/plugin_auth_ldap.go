package main

// This plugin imports ldapauthserver to register the LDAP implementation of AuthServer.

import (
	_ "github.com/youtube/vitess/go/mysqlconn/ldapauthserver"
)
