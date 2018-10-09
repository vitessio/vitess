package main

import (
	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/vt/vtgate"
)

func init() {
	vtgate.RegisterPluginInitializer(func() { mysql.InitAuthServerK8s() })
}
