// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package grpcvtworkerclient contains the gRPC version of the vtworker client protocol.
package grpcvtworkerclient

import (
	"io"
	"time"

	"github.com/youtube/vitess/go/vt/logutil"
	"github.com/youtube/vitess/go/vt/worker/vtworkerclient"
	"golang.org/x/net/context"
	"google.golang.org/grpc"

	pb "github.com/youtube/vitess/go/vt/proto/vtworkerdata"
	pbs "github.com/youtube/vitess/go/vt/proto/vtworkerservice"
)

type gRPCVtworkerClient struct {
	cc *grpc.ClientConn
	c  pbs.VtworkerClient
}

func gRPCVtworkerClientFactory(addr string, dialTimeout time.Duration) (vtworkerclient.VtworkerClient, error) {
	// create the RPC client
	cc, err := grpc.Dial(addr)
	if err != nil {
		return nil, err
	}
	c := pbs.NewVtworkerClient(cc)

	return &gRPCVtworkerClient{
		cc: cc,
		c:  c,
	}, nil
}

// ExecuteVtworkerCommand is part of the VtworkerClient interface.
func (client *gRPCVtworkerClient) ExecuteVtworkerCommand(ctx context.Context, args []string) (<-chan *logutil.LoggerEvent, vtworkerclient.ErrFunc) {
	query := &pb.ExecuteVtworkerCommandRequest{
		Args: args,
	}

	stream, err := client.c.ExecuteVtworkerCommand(ctx, query)
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
			results <- &logutil.LoggerEvent{
				Time:  time.Unix(le.Event.Time.Seconds, le.Event.Time.Nanoseconds),
				Level: int(le.Event.Level),
				File:  le.Event.File,
				Line:  int(le.Event.Line),
				Value: le.Event.Value,
			}
		}
	}()
	return results, func() error {
		return finalError
	}
}

// Close is part of the VtworkerClient interface.
func (client *gRPCVtworkerClient) Close() {
	client.cc.Close()
}

func init() {
	vtworkerclient.RegisterFactory("grpc", gRPCVtworkerClientFactory)
}
