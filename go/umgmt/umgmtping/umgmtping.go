package main

import (
	"code.google.com/p/vitess/go/umgmt"
	"flag"
)

var sockPath = flag.String("sock-path", "", "")

func main() {
	flag.Parse()
	println("sock path: ", *sockPath)
	uc, err := umgmt.Dial(*sockPath)
	if err != nil {
		panic(err)
	}
	msg, err := uc.Ping()
	if err != nil {
		panic(err)
	}
	println("msg: ", msg)
}
