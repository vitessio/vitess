// getsrv is a tiny utility that just calls net.LookupSRV().
// It's used in the etcd pod for the Vitess on Kubernetes example,
// to avoid having to install extra packages in etcd-lite.
package main

import (
	"fmt"
	"net"
	"os"
)

func main() {
	_, addrs, err := net.LookupSRV(os.Args[1], os.Args[2], os.Args[3])
	if err != nil {
		return
	}

	for _, addr := range addrs {
		fmt.Printf("%v:%v\n", addr.Target, addr.Port)
	}
}
