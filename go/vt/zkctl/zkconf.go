//go:build !codeanalysis
// +build !codeanalysis

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

/*
  Generate zoo.conf files from templates.
*/

package zkctl

import (
	"bytes"
	"fmt"
	"os"
	"path"
	"strconv"
	"strings"
	"text/template"

	"vitess.io/vitess/go/netutil"
	"vitess.io/vitess/go/vt/env"
	"vitess.io/vitess/go/vt/log"
)

type zkServerAddr struct {
	ServerId     uint32 // nolint:revive
	Hostname     string
	LeaderPort   int
	ElectionPort int
	ClientPort   int
}

type ZkConfig struct {
	ServerId   uint32 // nolint:revive
	ClientPort int
	Servers    []zkServerAddr
	Global     bool
}

/* ServerId is a unique id for a server - must be 1-255
 */
func NewZkConfig() *ZkConfig {
	return &ZkConfig{
		ClientPort: 2181,
		Servers:    make([]zkServerAddr, 0, 16),
	}
}

func (cnf *ZkConfig) DataDir() string {
	baseDir := env.VtDataRoot()
	if cnf.Global {
		return fmt.Sprintf("%v/zk_global_%03d", baseDir, cnf.ServerId)
	}
	return fmt.Sprintf("%v/zk_%03d", baseDir, cnf.ServerId)
}

func (cnf *ZkConfig) DirectoryList() []string {
	return []string{
		cnf.DataDir(),
		cnf.LogDir(),
	}
}

func (cnf *ZkConfig) LogDir() string {
	return path.Join(cnf.DataDir(), "logs")
}

func (cnf *ZkConfig) ConfigFile() string {
	return path.Join(cnf.DataDir(), "zoo.cfg")
}

func (cnf *ZkConfig) PidFile() string {
	return path.Join(cnf.DataDir(), "zk.pid")
}

func (cnf *ZkConfig) MyidFile() string {
	return path.Join(cnf.DataDir(), "myid")
}

func (cnf *ZkConfig) WriteMyid() error {
	return os.WriteFile(cnf.MyidFile(), []byte(fmt.Sprintf("%v", cnf.ServerId)), 0664)
}

/*
Search for first existing file in cnfFiles and subsitute in the right values.
*/
func MakeZooCfg(cnfFiles []string, cnf *ZkConfig, header string) (string, error) {
	myTemplateSource := new(bytes.Buffer)
	for _, line := range strings.Split(header, "\n") {
		fmt.Fprintf(myTemplateSource, "## %v\n", strings.TrimSpace(line))
	}
	var dataErr error
	for _, path := range cnfFiles {
		data, dataErr := os.ReadFile(path)
		if dataErr != nil {
			continue
		}
		myTemplateSource.WriteString("## " + path + "\n")
		myTemplateSource.Write(data)
	}
	if dataErr != nil {
		return "", dataErr
	}

	myTemplate, err := template.New("foo").Parse(myTemplateSource.String())
	if err != nil {
		return "", err
	}
	cnfData := new(bytes.Buffer)
	err = myTemplate.Execute(cnfData, cnf)
	if err != nil {
		return "", err
	}
	return cnfData.String(), nil
}

const GuessMyID = 0

/*
Create a config for this instance.

<server_id>@<hostname>:<leader_port>:<election_port>:<client_port>

If server_id > 1000, then we assume this is a global quorum.
server_id's must be 1-255, global id's are 1001-1255 mod 1000.
*/
func MakeZkConfigFromString(cmdLine string, myID uint32) *ZkConfig {
	zkConfig := NewZkConfig()
	for _, zki := range strings.Split(cmdLine, ",") {
		zkiParts := strings.SplitN(zki, "@", 2)
		if len(zkiParts) != 2 {
			panic("bad command line format for zk config")
		}
		zkID := zkiParts[0]
		zkAddrParts := strings.Split(zkiParts[1], ":")
		serverId, _ := strconv.ParseUint(zkID, 10, 0) // nolint:revive
		if serverId > 1000 {
			serverId = serverId % 1000
			zkConfig.Global = true
		}
		myID = myID % 1000

		zkServer := zkServerAddr{ServerId: uint32(serverId), ClientPort: 2181,
			LeaderPort: 2888, ElectionPort: 3888}
		switch len(zkAddrParts) {
		case 4:
			zkServer.ClientPort, _ = strconv.Atoi(zkAddrParts[3])
			fallthrough
		case 3:
			zkServer.ElectionPort, _ = strconv.Atoi(zkAddrParts[2])
			fallthrough
		case 2:
			zkServer.LeaderPort, _ = strconv.Atoi(zkAddrParts[1])
			fallthrough
		case 1:
			zkServer.Hostname = zkAddrParts[0]
			// if !strings.Contains(zkServer.Hostname, ".") {
			// 	panic(fmt.Errorf("expected fully qualified hostname: %v", zkServer.Hostname))
			// }
		default:
			panic(fmt.Errorf("bad command line format for zk config"))
		}
		zkConfig.Servers = append(zkConfig.Servers, zkServer)
	}
	hostname := netutil.FullyQualifiedHostnameOrPanic()
	log.Infof("Fully qualified machine hostname was detected as: %v", hostname)
	for _, zkServer := range zkConfig.Servers {
		if (myID > 0 && myID == zkServer.ServerId) || (myID == 0 && zkServer.Hostname == hostname) {
			zkConfig.ServerId = zkServer.ServerId
			zkConfig.ClientPort = zkServer.ClientPort
			break
		}
	}
	if zkConfig.ServerId == 0 {
		panic(fmt.Errorf("no zk server found for host %v in config %v", hostname, cmdLine))
	}
	return zkConfig
}
