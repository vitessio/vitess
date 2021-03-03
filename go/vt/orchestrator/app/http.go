/*
   Copyright 2014 Outbrain Inc.

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

package app

import (
	"flag"
	"net"
	nethttp "net/http"
	"path"
	"strings"
	"time"

	"vitess.io/vitess/go/vt/orchestrator/agent"
	"vitess.io/vitess/go/vt/orchestrator/collection"
	"vitess.io/vitess/go/vt/orchestrator/config"
	"vitess.io/vitess/go/vt/orchestrator/http"
	"vitess.io/vitess/go/vt/orchestrator/inst"
	"vitess.io/vitess/go/vt/orchestrator/logic"
	"vitess.io/vitess/go/vt/orchestrator/process"
	"vitess.io/vitess/go/vt/orchestrator/ssl"

	"github.com/go-martini/martini"
	"github.com/martini-contrib/auth"
	"github.com/martini-contrib/gzip"
	"github.com/martini-contrib/render"

	"vitess.io/vitess/go/vt/orchestrator/external/golib/log"
)

const discoveryMetricsName = "DISCOVERY_METRICS"

// TODO(sougou): see if this can be rice-boxed.
var webDir = flag.String("orc_web_dir", "web/orchestrator", "Orchestrator http file location")

var sslPEMPassword []byte
var agentSSLPEMPassword []byte
var discoveryMetrics *collection.Collection

// Http starts serving
func Http(continuousDiscovery bool) {
	promptForSSLPasswords()
	process.ContinuousRegistration(string(process.OrchestratorExecutionHttpMode), "")

	martini.Env = martini.Prod
	if config.Config.ServeAgentsHttp {
		go agentsHttp()
	}
	standardHttp(continuousDiscovery)
}

// Iterate over the private keys and get passwords for them
// Don't prompt for a password a second time if the files are the same
func promptForSSLPasswords() {
	if ssl.IsEncryptedPEM(config.Config.SSLPrivateKeyFile) {
		sslPEMPassword = ssl.GetPEMPassword(config.Config.SSLPrivateKeyFile)
	}
	if ssl.IsEncryptedPEM(config.Config.AgentSSLPrivateKeyFile) {
		if config.Config.AgentSSLPrivateKeyFile == config.Config.SSLPrivateKeyFile {
			agentSSLPEMPassword = sslPEMPassword
		} else {
			agentSSLPEMPassword = ssl.GetPEMPassword(config.Config.AgentSSLPrivateKeyFile)
		}
	}
}

// standardHttp starts serving HTTP or HTTPS (api/web) requests, to be used by normal clients
func standardHttp(continuousDiscovery bool) {
	m := martini.Classic()

	switch strings.ToLower(config.Config.AuthenticationMethod) {
	case "basic":
		{
			if config.Config.HTTPAuthUser == "" {
				// Still allowed; may be disallowed in future versions
				log.Warning("AuthenticationMethod is configured as 'basic' but HTTPAuthUser undefined. Running without authentication.")
			}
			m.Use(auth.Basic(config.Config.HTTPAuthUser, config.Config.HTTPAuthPassword))
		}
	case "multi":
		{
			if config.Config.HTTPAuthUser == "" {
				// Still allowed; may be disallowed in future versions
				log.Fatal("AuthenticationMethod is configured as 'multi' but HTTPAuthUser undefined")
			}

			m.Use(auth.BasicFunc(func(username, password string) bool {
				if username == "readonly" {
					// Will be treated as "read-only"
					return true
				}
				return auth.SecureCompare(username, config.Config.HTTPAuthUser) && auth.SecureCompare(password, config.Config.HTTPAuthPassword)
			}))
		}
	default:
		{
			// We inject a dummy User object because we have function signatures with User argument in api.go
			m.Map(auth.User(""))
		}
	}

	m.Use(gzip.All())
	// Render html templates from templates directory
	m.Use(render.Renderer(render.Options{
		Directory:       *webDir,
		Layout:          "templates/layout",
		HTMLContentType: "text/html",
	}))
	m.Use(martini.Static(path.Join(*webDir, "public"), martini.StaticOptions{Prefix: config.Config.URLPrefix}))
	if config.Config.UseMutualTLS {
		m.Use(ssl.VerifyOUs(config.Config.SSLValidOUs))
	}

	inst.SetMaintenanceOwner(process.ThisHostname)

	if continuousDiscovery {
		// start to expire metric collection info
		discoveryMetrics = collection.CreateOrReturnCollection(discoveryMetricsName)
		discoveryMetrics.SetExpirePeriod(time.Duration(config.Config.DiscoveryCollectionRetentionSeconds) * time.Second)

		log.Info("Starting Discovery")
		go logic.ContinuousDiscovery()
	}

	log.Info("Registering endpoints")
	http.API.URLPrefix = config.Config.URLPrefix
	http.Web.URLPrefix = config.Config.URLPrefix
	http.API.RegisterRequests(m)
	http.Web.RegisterRequests(m)

	// Serve
	if config.Config.ListenSocket != "" {
		log.Infof("Starting HTTP listener on unix socket %v", config.Config.ListenSocket)
		unixListener, err := net.Listen("unix", config.Config.ListenSocket)
		if err != nil {
			log.Fatale(err)
		}
		defer unixListener.Close()
		if err := nethttp.Serve(unixListener, m); err != nil {
			log.Fatale(err)
		}
	} else if config.Config.UseSSL {
		log.Info("Starting HTTPS listener")
		tlsConfig, err := ssl.NewTLSConfig(config.Config.SSLCAFile, config.Config.UseMutualTLS)
		if err != nil {
			log.Fatale(err)
		}
		tlsConfig.InsecureSkipVerify = config.Config.SSLSkipVerify
		if err = ssl.AppendKeyPairWithPassword(tlsConfig, config.Config.SSLCertFile, config.Config.SSLPrivateKeyFile, sslPEMPassword); err != nil {
			log.Fatale(err)
		}
		if err = ssl.ListenAndServeTLS(config.Config.ListenAddress, m, tlsConfig); err != nil {
			log.Fatale(err)
		}
	} else {
		log.Infof("Starting HTTP listener on %+v", config.Config.ListenAddress)
		if err := nethttp.ListenAndServe(config.Config.ListenAddress, m); err != nil {
			log.Fatale(err)
		}
	}
	log.Info("Web server started")
}

// agentsHttp startes serving agents HTTP or HTTPS API requests
func agentsHttp() {
	m := martini.Classic()
	m.Use(gzip.All())
	m.Use(render.Renderer())
	if config.Config.AgentsUseMutualTLS {
		m.Use(ssl.VerifyOUs(config.Config.AgentSSLValidOUs))
	}

	log.Info("Starting agents listener")

	agent.InitHttpClient()
	go logic.ContinuousAgentsPoll()

	http.AgentsAPI.URLPrefix = config.Config.URLPrefix
	http.AgentsAPI.RegisterRequests(m)

	// Serve
	if config.Config.AgentsUseSSL {
		log.Info("Starting agent HTTPS listener")
		tlsConfig, err := ssl.NewTLSConfig(config.Config.AgentSSLCAFile, config.Config.AgentsUseMutualTLS)
		if err != nil {
			log.Fatale(err)
		}
		tlsConfig.InsecureSkipVerify = config.Config.AgentSSLSkipVerify
		if err = ssl.AppendKeyPairWithPassword(tlsConfig, config.Config.AgentSSLCertFile, config.Config.AgentSSLPrivateKeyFile, agentSSLPEMPassword); err != nil {
			log.Fatale(err)
		}
		if err = ssl.ListenAndServeTLS(config.Config.AgentsServerPort, m, tlsConfig); err != nil {
			log.Fatale(err)
		}
	} else {
		log.Info("Starting agent HTTP listener")
		if err := nethttp.ListenAndServe(config.Config.AgentsServerPort, m); err != nil {
			log.Fatale(err)
		}
	}
	log.Info("Agent server started")
}
