package sidecar

import (
	"sync/atomic"
)

const (
	DefaultName = "_vt"
)

var (
	// This should be accessed via GetName()
	sidecarDBName atomic.Value
)

func init() {
	sidecarDBName.Store(DefaultName)
}

func SetName(name string) {
	sidecarDBName.Store(name)
}

func GetName() string {
	return sidecarDBName.Load().(string)
}
