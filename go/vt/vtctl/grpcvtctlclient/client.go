// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package grpcvtctlclient contains the gRPC version of the vtctl client protocol
package grpcvtctlclient

import (
	"io"
	"time"

	"github.com/youtube/vitess/go/vt/logutil"
	"github.com/youtube/vitess/go/vt/vtctl/vtctlclient"
	"golang.org/x/net/context"
	"google.golang.org/grpc"

	pb "github.com/youtube/vitess/go/vt/proto/vtctl"
)

type gRPCVtctlClient struct {
	cc *grpc.ClientConn
	c  pb.VtctlClient
}

func gRPCVtctlClientFactory(addr string, dialTimeout time.Duration) (vtctlclient.VtctlClient, error) {
	// create the RPC client
	cc, err := grpc.Dial(addr)
	if err != nil {
		return nil, err
	}
	c := pb.NewVtctlClient(cc)

	return &gRPCVtctlClient{
		cc: cc,
		c:  c,
	}, nil
}

// ExecuteVtctlCommand is part of the VtctlClient interface
func (client *gRPCVtctlClient) ExecuteVtctlCommand(args []string, actionTimeout, lockTimeout time.Duration) (<-chan *logutil.LoggerEvent, vtctlclient.ErrFunc) {
	ctx, cancel := context.WithTimeout(context.TODO(), actionTimeout)
	query := &pb.ExecuteVtctlCommandArgs{
		Args:          args,
		ActionTimeout: int64(actionTimeout.Nanoseconds()),
		LockTimeout:   int64(lockTimeout.Nanoseconds()),
	}

	stream, err := client.c.ExecuteVtctlCommand(ctx, query)
	if err != nil {
		return nil, func() error { return err }
	}

	results := make(chan *logutil.LoggerEvent, 1)
	var finalError error
	go func() {
		for {
			le, err := stream.Recv()
			if err != nil {
				cancel()
				if err != io.EOF {
					finalError = err
				}
				close(results)
				return
			}
			results <- &logutil.LoggerEvent{
				Time:  time.Unix(le.Time.Seconds, le.Time.Nanoseconds),
				Level: int(le.Level),
				File:  le.File,
				Line:  int(le.Line),
				Value: le.Value,
			}
		}
	}()
	return results, func() error {
		return finalError
	}
}

// Close is part of the VtctlClient interface
func (client *gRPCVtctlClient) Close() {
	client.cc.Close()
}

func init() {
	vtctlclient.RegisterVtctlClientFactory("grpc", gRPCVtctlClientFactory)
}
