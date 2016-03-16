package main

import (
	"fmt"
	"log"

	"launchpad.net/gozk/zookeeper"
)

func main() {
	zk, session, err := zookeeper.Dial("localhost:2181", 5e9)
	if err != nil {
		log.Fatalf("Can't connect: %v", err)
	}
	defer zk.Close()

	// Wait for connection.
	event := <-session
	if event.State != zookeeper.STATE_CONNECTED {
		log.Fatalf("Can't connect: %v", event)
	}

	_, err = zk.Create("/counter", "0", 0, zookeeper.WorldACL(zookeeper.PERM_ALL))
	if err != nil {
		log.Fatalf("Can't create counter: %v", err)
	} else {
		fmt.Println("Counter created!")
	}
}
