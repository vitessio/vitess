// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main

import (
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/golang/protobuf/proto"
	"golang.org/x/net/context"
	"google.golang.org/grpc"

	pb "github.com/youtube/vitess/go/vt/proto/automation"
	pbs "github.com/youtube/vitess/go/vt/proto/automationservice"
)

var (
	automationServer = flag.String("server", "", "Endpoint to Automation Server e.g. localhost:1234.")
	task             = flag.String("task", "", "Task which should be run.")
)

// cmdParams implements flag.Value to store key=value pairs.
type cmdParams struct {
	parameters map[string]string
}

// String implements flag.Value to return the default value.
func (*cmdParams) String() string { return "\"key=value\"" }

func (p *cmdParams) Set(v string) error {
	if v != "" {
		keyAndValue := strings.SplitN(v, "=", 2)
		if len(keyAndValue) < 2 {
			return fmt.Errorf("No key specified: '%v' Expected format: key=value.", v)
		}
		if p.parameters == nil {
			p.parameters = make(map[string]string)
		}
		p.parameters[keyAndValue[0]] = keyAndValue[1]
	}
	return nil
}

func main() {
	var params cmdParams
	flag.Var(&params, "param", "Task Parameter of the form key=value. May be repeated.")
	flag.Parse()

	if *task == "" {
		fmt.Println("Please specify a task using the --task parameter.")
		os.Exit(1)
	}
	if *automationServer == "" {
		fmt.Println("Please specify the automation server address using the --server parameter.")
		os.Exit(2)
	}

	fmt.Println("Connecting to Automation Server:", *automationServer)

	conn, err := grpc.Dial(*automationServer)
	if err != nil {
		fmt.Println("Cannot create connection:", err)
		os.Exit(3)
	}
	defer conn.Close()
	client := pbs.NewAutomationClient(conn)

	enqueueRequest := &pb.EnqueueClusterOperationRequest{
		Name:       *task,
		Parameters: params.parameters,
	}
	fmt.Printf("Sending request:\n%v", proto.MarshalTextString(enqueueRequest))
	enqueueResponse, err := client.EnqueueClusterOperation(context.Background(), enqueueRequest)
	if err != nil {
		fmt.Println("Failed to enqueue ClusterOperation. Error:", err)
		os.Exit(4)
	}
	fmt.Println("Operation was enqueued. Details:", enqueueResponse)
	errWait := waitForClusterOp(client, enqueueResponse.Id)
	if errWait != nil {
		fmt.Println("ERROR:", errWait)
		os.Exit(5)
	}
	fmt.Println("SUCCESS: ClusterOperation finished.")
}

// waitForClusterOp polls and blocks until the ClusterOperation invocation specified by "id" has finished. If an error occured, it will be returned.
func waitForClusterOp(client pbs.AutomationClient, id string) error {
	for {
		req := &pb.GetClusterOperationDetailsRequest{
			Id: id,
		}

		resp, err := client.GetClusterOperationDetails(context.Background(), req)
		if err != nil {
			return fmt.Errorf("Failed to get ClusterOperation Details. Request: %v Error: %v", req, err)
		}

		switch resp.ClusterOp.State {
		case pb.ClusterOperationState_UNKNOWN_CLUSTER_OPERATION_STATE:
			return fmt.Errorf("ClusterOperation is in an unknown state. Details: %v", resp)
		case pb.ClusterOperationState_CLUSTER_OPERATION_DONE:
			if resp.ClusterOp.Error != "" {
				return fmt.Errorf("ClusterOperation failed. Details:\n%v", proto.MarshalTextString(resp))
			}
			return nil
		}

		time.Sleep(50 * time.Millisecond)
	}
}
