package main

// This plugin imports InitAuthServerVault to register the HashiCorp Vault implementation of AuthServer.

import (
	"vitess.io/vitess/go/mysql/vault"
	"vitess.io/vitess/go/vt/vtgate"
)

func init() {
	vtgate.RegisterPluginInitializer(func() { vault.InitAuthServerVault() })
}
