package etcdtopo

import (
	"fmt"

	"github.com/youtube/vitess/go/vt/topo"
)

// EtcdVersion is etcd's idea of a version.
// It implements topo.Version.
// We use the native etcd version type, uint64.
type EtcdVersion uint64

// String is part of the topo.Version interface.
func (v EtcdVersion) String() string {
	return fmt.Sprintf("%v", uint64(v))
}

var _ topo.Version = (EtcdVersion)(0) // compile-time interface check
