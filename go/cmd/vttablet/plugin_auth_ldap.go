package main

// This plugin imports ldapacl to register the LDAP implementation of Table ACLs

import (
	_ "github.com/youtube/vitess/go/vt/tableacl/ldapacl"
)
