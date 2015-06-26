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

	pb "github.com/youtube/vitess/go/vt/proto/vtctldata"
	pbs "github.com/youtube/vitess/go/vt/proto/vtctlservice"
)

type gRPCVtctlClient struct {
	cc *grpc.ClientConn
	c  pbs.VtctlClient
}

func gRPCVtctlClientFactory(addr string, dialTimeout time.Duration) (vtctlclient.VtctlClient, error) {
	// create the RPC client
	cc, err := grpc.Dial(addr, grpc.WithBlock(), grpc.WithTimeout(dialTimeout))
	if err != nil {
		return nil, err
	}
	c := pbs.NewVtctlClient(cc)

	return &gRPCVtctlClient{
		cc: cc,
		c:  c,
	}, nil
}

// ExecuteVtctlCommand is part of the VtctlClient interface
func (client *gRPCVtctlClient) ExecuteVtctlCommand(ctx context.Context, args []string, actionTimeout, lockTimeout time.Duration) (<-chan *logutil.LoggerEvent, vtctlclient.ErrFunc) {
	query := &pb.ExecuteVtctlCommandRequest{
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
				if err != io.EOF {
					finalError = err
				}
				close(results)
				return
			}
			results <- logutil.ProtoToLoggerEvent(le.Event)
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
	vtctlclient.RegisterFactory("grpc", gRPCVtctlClientFactory)
}
