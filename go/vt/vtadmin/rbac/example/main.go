package main

import (
	"encoding/json"
	"flag"
	"fmt"

	"vitess.io/vitess/go/vt/vtadmin/rbac"
)

func main() {
	flag.Parse()

	cfg, err := rbac.LoadConfig("config.yaml")
	if err != nil {
		panic(err)
	}

	data, err := json.Marshal(cfg)
	if err != nil {
		panic(err)
	}

	fmt.Printf("%s\n", data)
}
