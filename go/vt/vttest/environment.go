/*
Copyright 2017 Google Inc.

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

package vttest

import (
	"errors"
	"math/rand"
	"os"
	"path"
	"time"

	// we use gRPC everywhere, so import the vtgate client.
	_ "github.com/youtube/vitess/go/vt/vtgate/grpcvtgateconn"
)

func launcherPath() (string, error) {
	vttop := os.Getenv("VTTOP")
	if vttop == "" {
		return "", errors.New("VTTOP not set")
	}
	return path.Join(vttop, "py/vttest/run_local_database.py"), nil
}

func vtgateProtocol() string {
	return "grpc"
}

// randomPort returns a random number between 10k & 30k.
func randomPort() int {
	v := rand.Int31n(20000)
	return int(v + 10000)
}

func init() {
	rand.Seed(time.Now().UnixNano())
}
