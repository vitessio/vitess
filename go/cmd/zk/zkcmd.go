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

package main

import (
	"context"
	"os"
	"os/signal"

	"vitess.io/vitess/go/cmd/zk/command"
	"vitess.io/vitess/go/exit"
	"vitess.io/vitess/go/vt/log"
)

func main() {
	defer exit.Recover()

	// Create a context for the command, cancel it if we get a signal.
	ctx, cancel := context.WithCancel(context.Background())
	sigRecv := make(chan os.Signal, 1)
	signal.Notify(sigRecv, os.Interrupt)
	go func() {
		<-sigRecv
		cancel()
	}()

	// Run the command.
	if err := command.Root.ExecuteContext(ctx); err != nil {
		log.Error(err)
		exit.Return(1)
	}
}
