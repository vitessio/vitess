// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

/*
  Generate zoo.conf files from templates.
*/

package zkctl

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"path"
	"strconv"
	"strings"
	"text/template"

	"code.google.com/p/vitess/go/netutil"
	"code.google.com/p/vitess/go/vt/env"
)

type zkServerAddr struct {
	ServerId     uint32
	Hostname     string
	LeaderPort   int
	ElectionPort int
	ClientPort   int
}

type ZkConfig struct {
	ServerId   uint32
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
	return ioutil.WriteFile(cnf.MyidFile(), []byte(fmt.Sprintf("%v", cnf.ServerId)), 0664)
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
		data, dataErr := ioutil.ReadFile(path)
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

const GUESS_MYID = 0

/*
  Create a config for this instance.

  <server_id>@<hostname>:<leader_port>:<election_port>:<client_port>

  If server_id > 1000, then we assume this is a global quorum.
  server_id's must be 1-255, global id's are 1001-1255 mod 1000.
*/
func MakeZkConfigFromString(cmdLine string, myId uint32) *ZkConfig {
	zkConfig := NewZkConfig()
	for _, zki := range strings.Split(cmdLine, ",") {
		zkiParts := strings.SplitN(zki, "@", 2)
		if len(zkiParts) != 2 {
			panic("bad command line format for zk config")
		}
		zkId := zkiParts[0]
		zkAddrParts := strings.Split(zkiParts[1], ":")
		serverId, _ := strconv.ParseUint(zkId, 10, 0)
		if serverId > 1000 {
			serverId = serverId % 1000
			zkConfig.Global = true
		}
		myId = myId % 1000

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
	for _, zkServer := range zkConfig.Servers {
		if (myId > 0 && myId == zkServer.ServerId) || (myId == 0 && zkServer.Hostname == hostname) {
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
