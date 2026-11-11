/*
Copyright 2026 The Vitess Authors.

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

package servenv

import "google.golang.org/grpc"

// SetupGRPCTestListener sets the globals createGRPCServer reads and creates
// the process gRPC server. authMode "" leaves grpc-auth-mode unset. The
// returned server is nil when the listener was not created. restore puts the
// globals back and must run after the server is stopped.
func SetupGRPCTestListener(port int, authMode, credsFilePath string) (restore func(), server *grpc.Server) {
	savedPort := gRPCPort
	savedBind := gRPCBindAddress
	savedAuth := gRPCAuth
	savedCert := gRPCCert
	savedKey := gRPCKey
	savedCA := gRPCCA
	savedCRL := gRPCCRL
	savedServerCA := gRPCServerCA
	savedOptionalTLS := gRPCEnableOptionalTLS
	savedOrca := gRPCEnableOrcaMetrics
	savedSocket := socketFile
	savedPlugin := authPlugin
	savedCreds := credsFile
	savedServer := GRPCServer
	prevService, hadService := serviceMap["grpc-vtgateservice"]

	gRPCPort = port
	gRPCBindAddress = "127.0.0.1"
	gRPCAuth = authMode
	gRPCCert = ""
	gRPCKey = ""
	gRPCCA = ""
	gRPCCRL = ""
	gRPCServerCA = ""
	gRPCEnableOptionalTLS = false
	gRPCEnableOrcaMetrics = false
	socketFile = ""
	authPlugin = nil
	credsFile = credsFilePath
	GRPCServer = nil
	serviceMap["grpc-vtgateservice"] = true

	createGRPCServer()
	server = GRPCServer

	restore = func() {
		gRPCPort = savedPort
		gRPCBindAddress = savedBind
		gRPCAuth = savedAuth
		gRPCCert = savedCert
		gRPCKey = savedKey
		gRPCCA = savedCA
		gRPCCRL = savedCRL
		gRPCServerCA = savedServerCA
		gRPCEnableOptionalTLS = savedOptionalTLS
		gRPCEnableOrcaMetrics = savedOrca
		socketFile = savedSocket
		authPlugin = savedPlugin
		credsFile = savedCreds
		GRPCServer = savedServer
		if hadService {
			serviceMap["grpc-vtgateservice"] = prevService
		} else {
			delete(serviceMap, "grpc-vtgateservice")
		}
	}
	return restore, server
}

// ServeGRPCTestServer listens with the server createGRPCServer just built.
func ServeGRPCTestServer() (stop func()) {
	return serveGRPC()
}
