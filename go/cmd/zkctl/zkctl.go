/*
Copyright 2019 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// zkctl initializes and controls ZooKeeper with Vitess-specific configuration.
package main

import (
	"github.com/spf13/pflag"

	"vitess.io/vitess/go/exit"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/zkctl"
)

var usage = `
Commands:

	init | start | shutdown | teardown
`

var (
	zkCfg = "6@<hostname>:3801:3802:3803"
	myID  uint
)

func registerZkctlFlags(fs *pflag.FlagSet) {
	fs.StringVar(&zkCfg, "zk.cfg", zkCfg,
		"zkid@server1:leaderPort1:electionPort1:clientPort1,...)")
	fs.UintVar(&myID, "zk.myid", myID,
		"which server do you want to be? only needed when running multiple instance on one box, otherwise myid is implied by hostname")

}
func init() {
	servenv.OnParse(registerZkctlFlags)
}

func main() {
	defer exit.Recover()
	defer logutil.Flush()

	fs := pflag.NewFlagSet("zkctl", pflag.ExitOnError)
	log.RegisterFlags(fs)
	logutil.RegisterFlags(fs)
	args := servenv.ParseFlagsWithArgs("zkctl")

	zkConfig := zkctl.MakeZkConfigFromString(zkCfg, uint32(myID))
	zkd := zkctl.NewZkd(zkConfig)

	action := args[0]
	var err error
	switch action {
	case "init":
		err = zkd.Init()
	case "shutdown":
		err = zkd.Shutdown()
	case "start":
		err = zkd.Start()
	case "teardown":
		err = zkd.Teardown()
	default:
		log.Errorf("invalid action: %v", action)
		log.Errorf(usage)
		exit.Return(1)
	}
	if err != nil {
		log.Errorf("failed %v: %v", action, err)
		exit.Return(1)
	}
}
