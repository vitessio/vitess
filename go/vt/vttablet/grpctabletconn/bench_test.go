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

package grpctabletconn

import (
	"context"
	"fmt"
	"io"
	"math"
	"net"
	"slices"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"vitess.io/vitess/go/mysql"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	queryservicepb "vitess.io/vitess/go/vt/proto/queryservice"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/vttablet/grpcqueryservice"
	"vitess.io/vitess/go/vt/vttablet/queryservice"
	"vitess.io/vitess/go/vt/vttablet/tabletconntest"
)

// benchBinlogService overrides BinlogDumpGTID to stream pre-generated packets.
type benchBinlogService struct {
	*tabletconntest.FakeQueryService
	packets []*binlogdatapb.BinlogDumpResponse
}

func (s *benchBinlogService) BinlogDumpGTID(_ context.Context, _ *binlogdatapb.BinlogDumpGTIDRequest, send func(*binlogdatapb.BinlogDumpResponse) error) error {
	for _, pkt := range s.packets {
		if err := send(pkt); err != nil {
			return err
		}
	}
	return nil
}

// setupBenchGRPC starts a gRPC server with the given service and returns a
// client connection. maxMsgSize sets the maximum gRPC message size for both
// server and client.
func setupBenchGRPC(b *testing.B, service queryservice.QueryService, maxMsgSize int) queryservice.QueryService {
	b.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		b.Fatalf("Cannot listen: %v", err)
	}

	host := listener.Addr().(*net.TCPAddr).IP.String()
	port := listener.Addr().(*net.TCPAddr).Port

	server := grpc.NewServer(
		grpc.MaxRecvMsgSize(maxMsgSize),
		grpc.MaxSendMsgSize(maxMsgSize),
	)
	grpcqueryservice.Register(server, service)
	go server.Serve(listener)
	b.Cleanup(server.Stop)

	addr := fmt.Sprintf("%s:%d", host, port)
	cc, err := grpc.NewClient(addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(maxMsgSize),
			grpc.MaxCallSendMsgSize(maxMsgSize),
		),
	)
	if err != nil {
		b.Fatalf("grpc.NewClient failed: %v", err)
	}
	b.Cleanup(func() { cc.Close() })

	tablet := &topodatapb.Tablet{
		Keyspace: "bench",
		Shard:    "0",
		Type:     topodatapb.TabletType_REPLICA,
		Alias:    &topodatapb.TabletAlias{Cell: "zone1", Uid: 1},
		Hostname: host,
		PortMap:  map[string]int32{"grpc": int32(port)},
	}
	conn := &gRPCQueryClient{
		tablet: tablet,
		cc:     cc,
		c:      queryservicepb.NewQueryClient(cc),
	}
	return conn
}

// ---------- HOL blocking infrastructure ----------

// holPingServerIface is the interface for the unary ping service.
type holPingServerIface interface {
	Ping(ctx context.Context, req *binlogdatapb.BinlogDumpGTIDRequest) (*binlogdatapb.BinlogDumpResponse, error)
}

type holPingServerImpl struct{}

func (holPingServerImpl) Ping(_ context.Context, _ *binlogdatapb.BinlogDumpGTIDRequest) (*binlogdatapb.BinlogDumpResponse, error) {
	return &binlogdatapb.BinlogDumpResponse{}, nil
}

func holPingHandler(srv any, ctx context.Context, dec func(any) error, _ grpc.UnaryServerInterceptor) (any, error) {
	req := new(binlogdatapb.BinlogDumpGTIDRequest)
	if err := dec(req); err != nil {
		return nil, err
	}
	return srv.(holPingServerIface).Ping(ctx, req)
}

var holPingServiceDesc = grpc.ServiceDesc{
	ServiceName: "bench.Ping",
	HandlerType: (*holPingServerIface)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "Ping",
			Handler:    holPingHandler,
		},
	},
	Streams: []grpc.StreamDesc{},
}

// holStreamServerIface is the interface for the streaming service used in HOL tests.
type holStreamServerIface interface {
	Stream(*binlogdatapb.BinlogDumpGTIDRequest, grpc.ServerStream) error
}

type holStreamServerImpl struct {
	packets []*binlogdatapb.BinlogDumpResponse
}

func (s *holStreamServerImpl) Stream(_ *binlogdatapb.BinlogDumpGTIDRequest, stream grpc.ServerStream) error {
	for _, pkt := range s.packets {
		if err := stream.SendMsg(pkt); err != nil {
			return err
		}
	}
	return nil
}

func holStreamHandler(srv any, stream grpc.ServerStream) error {
	m := new(binlogdatapb.BinlogDumpGTIDRequest)
	if err := stream.RecvMsg(m); err != nil {
		return err
	}
	return srv.(holStreamServerIface).Stream(m, stream)
}

var holStreamServiceDesc = grpc.ServiceDesc{
	ServiceName: "bench.Stream",
	HandlerType: (*holStreamServerIface)(nil),
	Methods:     []grpc.MethodDesc{},
	Streams: []grpc.StreamDesc{
		{
			StreamName:    "Stream",
			Handler:       holStreamHandler,
			ServerStreams: true,
		},
	},
}

// setupHOLServer creates a gRPC server with both stream and ping services,
// returning a single client connection that multiplexes both.
func setupHOLServer(t *testing.T, streamSvc holStreamServerIface) *grpc.ClientConn {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Cannot listen: %v", err)
	}

	server := grpc.NewServer(
		grpc.MaxRecvMsgSize(64<<20),
		grpc.MaxSendMsgSize(64<<20),
	)
	server.RegisterService(&holStreamServiceDesc, streamSvc)
	server.RegisterService(&holPingServiceDesc, holPingServerImpl{})
	go server.Serve(listener)
	t.Cleanup(server.Stop)

	cc, err := grpc.NewClient(
		listener.Addr().String(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(64<<20),
			grpc.MaxCallSendMsgSize(64<<20),
		),
	)
	if err != nil {
		t.Fatalf("grpc.NewClient failed: %v", err)
	}
	t.Cleanup(func() { cc.Close() })

	return cc
}

// doPing performs one unary Ping RPC on cc and returns the latency.
func doPing(cc *grpc.ClientConn) (time.Duration, error) {
	start := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var resp binlogdatapb.BinlogDumpResponse
	err := cc.Invoke(ctx, "/bench.Ping/Ping", &binlogdatapb.BinlogDumpGTIDRequest{}, &resp)
	return time.Since(start), err
}

// consumeStreamWithCallback reads all messages from the streaming RPC and
// passes each to the callback for processing.
func consumeStreamWithCallback(cc *grpc.ClientConn, callback func(*binlogdatapb.BinlogDumpResponse) error) error {
	ctx := context.Background()
	stream, err := cc.NewStream(ctx, &holStreamServiceDesc.Streams[0], "/bench.Stream/Stream")
	if err != nil {
		return fmt.Errorf("NewStream: %w", err)
	}
	if err := stream.SendMsg(&binlogdatapb.BinlogDumpGTIDRequest{}); err != nil {
		return fmt.Errorf("SendMsg: %w", err)
	}
	if err := stream.CloseSend(); err != nil {
		return fmt.Errorf("CloseSend: %w", err)
	}
	for {
		var resp binlogdatapb.BinlogDumpResponse
		err := stream.RecvMsg(&resp)
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return fmt.Errorf("RecvMsg: %w", err)
		}
		if err := callback(&resp); err != nil {
			return fmt.Errorf("callback: %w", err)
		}
	}
}

type latencyStats struct {
	p50, p95, p99, max time.Duration
}

func computeLatencyStats(latencies []time.Duration) latencyStats {
	slices.Sort(latencies)
	n := len(latencies)
	return latencyStats{
		p50: latencies[n/2],
		p95: latencies[int(math.Ceil(float64(n)*0.95))-1],
		p99: latencies[int(math.Ceil(float64(n)*0.99))-1],
		max: latencies[n-1],
	}
}

// ---------- E2E benchmark helpers ----------

// discardConn implements net.Conn with writes going to a discard sink.
type discardConn struct{}

func (discardConn) Read([]byte) (int, error)         { select {} }
func (discardConn) Write(b []byte) (int, error)      { return len(b), nil }
func (discardConn) Close() error                     { return nil }
func (discardConn) LocalAddr() net.Addr              { return &net.TCPAddr{} }
func (discardConn) RemoteAddr() net.Addr             { return &net.TCPAddr{} }
func (discardConn) SetDeadline(time.Time) error      { return nil }
func (discardConn) SetReadDeadline(time.Time) error  { return nil }
func (discardConn) SetWriteDeadline(time.Time) error { return nil }

// buildSinglePacketResponses builds one gRPC response per MySQL packet,
// each containing just the payload (no MySQL header). This matches the
// approach from b5c4a099b5 where each gRPC message carried exactly one
// complete MySQL packet payload.
func buildSinglePacketResponses(payloadSizes []int) []*binlogdatapb.BinlogDumpResponse {
	responses := make([]*binlogdatapb.BinlogDumpResponse, len(payloadSizes))
	for i, size := range payloadSizes {
		responses[i] = &binlogdatapb.BinlogDumpResponse{
			Raw: make([]byte, size),
		}
	}
	return responses
}

// buildE2EResponses builds a raw MySQL packet stream from the given payload
// sizes, then splits it into fixed-size chunks simulating gRPC response
// delivery. Packets larger than responseSize naturally span multiple responses.
func buildE2EResponses(payloadSizes []int, responseSize int) []*binlogdatapb.BinlogDumpResponse {
	var stream []byte
	seq := uint8(1)
	for _, size := range payloadSizes {
		stream = append(stream, byte(size), byte(size>>8), byte(size>>16), seq)
		seq++
		stream = append(stream, make([]byte, size)...)
	}
	var responses []*binlogdatapb.BinlogDumpResponse
	for i := 0; i < len(stream); i += responseSize {
		end := min(i+responseSize, len(stream))
		responses = append(responses, &binlogdatapb.BinlogDumpResponse{
			Raw: stream[i:end],
		})
	}
	return responses
}

// BenchmarkE2ECallback measures the full production path: gRPC server streams
// BinlogDumpResponse messages → gRPC client receives → callback parses MySQL
// packet headers → handles spanning packets → writes to downstream mysql.Conn.
func BenchmarkE2ECallback(b *testing.B) {
	type testCase struct {
		name        string
		packetSizes []int
	}

	cases := []testCase{
		{
			name: "no_spanning",
			packetSizes: func() []int {
				s := make([]int, 1000)
				for i := range s {
					s[i] = 200
				}
				return s
			}(),
		},
		{
			name: "mixed_500KB",
			packetSizes: func() []int {
				s := make([]int, 100)
				for i := range s {
					if i%10 == 0 {
						s[i] = 500 * 1024 // spans ~8 responses
					} else {
						s[i] = 200
					}
				}
				return s
			}(),
		},
		{
			name: "mixed_16MB",
			packetSizes: func() []int {
				s := make([]int, 100)
				for i := range s {
					if i%10 == 0 {
						s[i] = mysql.MaxPacketSize // spans ~256 responses
					} else {
						s[i] = 200
					}
				}
				return s
			}(),
		},
		{
			name: "all_spanning_500KB",
			packetSizes: func() []int {
				s := make([]int, 20)
				for i := range s {
					s[i] = 500 * 1024
				}
				return s
			}(),
		},
		{
			name: "all_spanning_16MB",
			packetSizes: func() []int {
				s := make([]int, 5)
				for i := range s {
					s[i] = mysql.MaxPacketSize
				}
				return s
			}(),
		},
	}

	responseSizes := []struct {
		name string
		size int
	}{
		{"64KB", 64 * 1024},
		{"256KB", 256 * 1024},
	}

	// Allow messages up to 32MB to accommodate 16MB MySQL packets plus
	// protobuf framing overhead.
	const e2eMaxMsgSize = 32 << 20

	for _, tc := range cases {
		numPackets := len(tc.packetSizes)

		// SinglePacket: one gRPC message per MySQL packet, payload only
		// (no MySQL headers). Matches the approach from b5c4a099b5.
		// Chunk size is irrelevant here (one message per packet).
		singleResponses := buildSinglePacketResponses(tc.packetSizes)
		var singleTotalBytes int64
		for _, r := range singleResponses {
			singleTotalBytes += int64(len(r.Raw))
		}

		b.Run("SinglePacket/"+tc.name, func(b *testing.B) {
			service := &benchBinlogService{
				FakeQueryService: tabletconntest.CreateFakeServer(b),
				packets:          singleResponses,
			}
			client := setupBenchGRPC(b, service, e2eMaxMsgSize)
			conn := mysql.NewConnForTest(discardConn{})

			b.SetBytes(singleTotalBytes)
			b.ReportAllocs()
			b.ResetTimer()

			for b.Loop() {
				err := client.BinlogDumpGTID(context.Background(), &binlogdatapb.BinlogDumpGTIDRequest{}, func(response *binlogdatapb.BinlogDumpResponse) error {
					return conn.WritePacketDirect(response.Raw)
				})
				if err != nil {
					b.Fatalf("BinlogDumpGTID failed: %v", err)
				}
			}

			b.ReportMetric(float64(numPackets), "packets/op")
		})

		for _, rs := range responseSizes {
			responses := buildE2EResponses(tc.packetSizes, rs.size)
			var totalBytes int64
			for _, r := range responses {
				totalBytes += int64(len(r.Raw))
			}

			b.Run("AllocCopy/"+rs.name+"/"+tc.name, func(b *testing.B) {
				service := &benchBinlogService{
					FakeQueryService: tabletconntest.CreateFakeServer(b),
					packets:          responses,
				}
				client := setupBenchGRPC(b, service, e2eMaxMsgSize)
				conn := mysql.NewConnForTest(discardConn{})

				b.SetBytes(totalBytes)
				b.ReportAllocs()
				b.ResetTimer()

				for b.Loop() {
					var packetData []byte
					var packetDataOffset int
					var packetDataLength int

					err := client.BinlogDumpGTID(context.Background(), &binlogdatapb.BinlogDumpGTIDRequest{}, func(response *binlogdatapb.BinlogDumpResponse) error {
						buf := response.Raw
						bufOffset := 0

						if packetData != nil {
							remaining := packetDataLength - packetDataOffset
							if len(buf) < remaining {
								copy(packetData[packetDataOffset:], buf)
								packetDataOffset += len(buf)
								return nil
							}
							copy(packetData[packetDataOffset:], buf[:remaining])
							bufOffset = remaining
							conn.WritePacketDirect(packetData)
							packetData = nil
							packetDataOffset = 0
							packetDataLength = 0
							if bufOffset == len(buf) {
								return nil
							}
						}

						for len(buf)-bufOffset > 0 {
							header := buf[bufOffset : bufOffset+mysql.PacketHeaderSize]
							bufOffset += mysql.PacketHeaderSize
							pktLen := int(uint32(header[0]) | uint32(header[1])<<8 | uint32(header[2])<<16)

							if pktLen <= len(buf[bufOffset:]) {
								conn.WritePacketDirect(buf[bufOffset : bufOffset+pktLen])
								bufOffset += pktLen
							} else {
								packetData = make([]byte, pktLen)
								copy(packetData, buf[bufOffset:])
								packetDataOffset = len(buf[bufOffset:])
								packetDataLength = pktLen
								bufOffset = len(buf)
							}
						}
						return nil
					})
					if err != nil {
						b.Fatalf("BinlogDumpGTID failed: %v", err)
					}
				}

				b.ReportMetric(float64(numPackets), "packets/op")
			})

			b.Run("ZeroCopy/"+rs.name+"/"+tc.name, func(b *testing.B) {
				service := &benchBinlogService{
					FakeQueryService: tabletconntest.CreateFakeServer(b),
					packets:          responses,
				}
				client := setupBenchGRPC(b, service, e2eMaxMsgSize)
				conn := mysql.NewConnForTest(discardConn{})

				b.SetBytes(totalBytes)
				b.ReportAllocs()
				b.ResetTimer()

				for b.Loop() {
					var packetLength int
					var written int

					err := client.BinlogDumpGTID(context.Background(), &binlogdatapb.BinlogDumpGTIDRequest{}, func(response *binlogdatapb.BinlogDumpResponse) error {
						buf := response.Raw
						bufOffset := 0

						if packetLength > 0 {
							remaining := packetLength - written
							if len(buf) < remaining {
								conn.WritePacketRaw(buf)
								written += len(buf)
								return nil
							}
							conn.WritePacketRaw(buf[:remaining])
							bufOffset = remaining
							packetLength = 0
							written = 0
							if bufOffset == len(buf) {
								return nil
							}
						}

						for len(buf)-bufOffset > 0 {
							header := buf[bufOffset : bufOffset+mysql.PacketHeaderSize]
							bufOffset += mysql.PacketHeaderSize
							pktLen := int(uint32(header[0]) | uint32(header[1])<<8 | uint32(header[2])<<16)

							if pktLen <= len(buf[bufOffset:]) {
								conn.WritePacketDirect(buf[bufOffset : bufOffset+pktLen])
								bufOffset += pktLen
							} else {
								packetLength = pktLen
								conn.WritePacketHeader(pktLen)
								conn.WritePacketRaw(buf[bufOffset:])
								written = len(buf) - bufOffset
								bufOffset = len(buf)
							}
						}
						return nil
					})
					if err != nil {
						b.Fatalf("BinlogDumpGTID failed: %v", err)
					}
				}

				b.ReportMetric(float64(numPackets), "packets/op")
			})
		}
	}
}

// TestE2ECallbackHOL measures how the three callback approaches affect
// head-of-line blocking on a shared gRPC connection. A streaming RPC delivers
// binlog data while concurrent unary pings measure transport-level latency.
//
// SinglePacket sends one 16MB gRPC message per MySQL packet.
// AllocCopy/ZeroCopy receive the same data chunked into 64KB or 256KB messages.
// Smaller messages allow the gRPC transport to interleave ping responses
// between stream frames, reducing HOL blocking.
func TestE2ECallbackHOL(t *testing.T) {
	const (
		packetPayload = mysql.MaxPacketSize // 16MB per MySQL packet
		numPackets    = 40                  // packets per round
		rounds        = 1                   // ~640MB total
		pingInterval  = 2 * time.Millisecond
	)

	// Build packet sizes for one round.
	roundSizes := make([]int, numPackets)
	for i := range roundSizes {
		roundSizes[i] = packetPayload
	}

	// Build responses for SinglePacket (one message per packet, repeated).
	var singleMessages []*binlogdatapb.BinlogDumpResponse
	for range rounds {
		singleMessages = append(singleMessages, buildSinglePacketResponses(roundSizes)...)
	}

	type holCase struct {
		name     string
		messages []*binlogdatapb.BinlogDumpResponse
		consume  func(*grpc.ClientConn) error
	}

	chunkSizes := []struct {
		name string
		size int
	}{
		{"64KB", 64 * 1024},
		{"256KB", 256 * 1024},
	}

	var cases []holCase

	// SinglePacket: one gRPC message per MySQL packet (chunk size irrelevant).
	cases = append(cases, holCase{
		name:     "SinglePacket",
		messages: singleMessages,
		consume: func(cc *grpc.ClientConn) error {
			conn := mysql.NewConnForTest(discardConn{})
			return consumeStreamWithCallback(cc, func(resp *binlogdatapb.BinlogDumpResponse) error {
				return conn.WritePacketDirect(resp.Raw)
			})
		},
	})

	for _, cs := range chunkSizes {
		var chunkedMessages []*binlogdatapb.BinlogDumpResponse
		for range rounds {
			chunkedMessages = append(chunkedMessages, buildE2EResponses(roundSizes, cs.size)...)
		}

		msgs := chunkedMessages // capture for closures
		cases = append(cases, holCase{
			name:     "AllocCopy/" + cs.name,
			messages: msgs,
			consume: func(cc *grpc.ClientConn) error {
				conn := mysql.NewConnForTest(discardConn{})
				var packetData []byte
				var packetDataOffset, packetDataLength int

				return consumeStreamWithCallback(cc, func(response *binlogdatapb.BinlogDumpResponse) error {
					buf := response.Raw
					bufOffset := 0

					if packetData != nil {
						remaining := packetDataLength - packetDataOffset
						if len(buf) < remaining {
							copy(packetData[packetDataOffset:], buf)
							packetDataOffset += len(buf)
							return nil
						}
						copy(packetData[packetDataOffset:], buf[:remaining])
						bufOffset = remaining
						conn.WritePacketDirect(packetData)
						packetData = nil
						packetDataOffset = 0
						packetDataLength = 0
						if bufOffset == len(buf) {
							return nil
						}
					}

					for len(buf)-bufOffset > 0 {
						header := buf[bufOffset : bufOffset+mysql.PacketHeaderSize]
						bufOffset += mysql.PacketHeaderSize
						pktLen := int(uint32(header[0]) | uint32(header[1])<<8 | uint32(header[2])<<16)

						if pktLen <= len(buf[bufOffset:]) {
							conn.WritePacketDirect(buf[bufOffset : bufOffset+pktLen])
							bufOffset += pktLen
						} else {
							packetData = make([]byte, pktLen)
							copy(packetData, buf[bufOffset:])
							packetDataOffset = len(buf[bufOffset:])
							packetDataLength = pktLen
							bufOffset = len(buf)
						}
					}
					return nil
				})
			},
		})

		cases = append(cases, holCase{
			name:     "ZeroCopy/" + cs.name,
			messages: msgs,
			consume: func(cc *grpc.ClientConn) error {
				conn := mysql.NewConnForTest(discardConn{})
				var packetLength, written int

				return consumeStreamWithCallback(cc, func(response *binlogdatapb.BinlogDumpResponse) error {
					buf := response.Raw
					bufOffset := 0

					if packetLength > 0 {
						remaining := packetLength - written
						if len(buf) < remaining {
							conn.WritePacketRaw(buf)
							written += len(buf)
							return nil
						}
						conn.WritePacketRaw(buf[:remaining])
						bufOffset = remaining
						packetLength = 0
						written = 0
						if bufOffset == len(buf) {
							return nil
						}
					}

					for len(buf)-bufOffset > 0 {
						header := buf[bufOffset : bufOffset+mysql.PacketHeaderSize]
						bufOffset += mysql.PacketHeaderSize
						pktLen := int(uint32(header[0]) | uint32(header[1])<<8 | uint32(header[2])<<16)

						if pktLen <= len(buf[bufOffset:]) {
							conn.WritePacketDirect(buf[bufOffset : bufOffset+pktLen])
							bufOffset += pktLen
						} else {
							packetLength = pktLen
							conn.WritePacketHeader(pktLen)
							conn.WritePacketRaw(buf[bufOffset:])
							written = len(buf) - bufOffset
							bufOffset = len(buf)
						}
					}
					return nil
				})
			},
		})
	}

	// Baseline: ping with no competing stream.
	baselineCC := setupHOLServer(t, &holStreamServerImpl{packets: nil})
	var baselineLatencies []time.Duration
	for range 100 {
		lat, err := doPing(baselineCC)
		if err != nil {
			t.Fatalf("baseline ping failed: %v", err)
		}
		baselineLatencies = append(baselineLatencies, lat)
	}
	baseline := computeLatencyStats(baselineLatencies)
	t.Logf("Baseline (no stream):  p50=%v  p95=%v  p99=%v  max=%v",
		baseline.p50, baseline.p95, baseline.p99, baseline.max)

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			svc := &holStreamServerImpl{packets: tc.messages}
			cc := setupHOLServer(t, svc)

			// Warm up the connection.
			if _, err := doPing(cc); err != nil {
				t.Fatalf("warmup ping failed: %v", err)
			}

			var (
				mu        sync.Mutex
				latencies []time.Duration
				stopPing  = make(chan struct{})
				pingDone  = make(chan struct{})
			)

			// Start pinging in the background.
			go func() {
				defer close(pingDone)
				ticker := time.NewTicker(pingInterval)
				defer ticker.Stop()
				for {
					select {
					case <-stopPing:
						return
					case <-ticker.C:
						lat, err := doPing(cc)
						if err != nil {
							return
						}
						mu.Lock()
						latencies = append(latencies, lat)
						mu.Unlock()
					}
				}
			}()

			// Let pings establish a rhythm.
			time.Sleep(50 * time.Millisecond)

			// Run the stream with the callback.
			if err := tc.consume(cc); err != nil {
				t.Fatalf("stream failed: %v", err)
			}

			// Let a few more pings land after the stream finishes.
			time.Sleep(50 * time.Millisecond)
			close(stopPing)
			<-pingDone

			mu.Lock()
			collected := latencies
			mu.Unlock()

			if len(collected) < 10 {
				t.Fatalf("only collected %d pings, need at least 10", len(collected))
			}

			stats := computeLatencyStats(collected)
			t.Logf("%s (%d msgs, %d pings):  p50=%v  p95=%v  p99=%v  max=%v",
				tc.name, len(tc.messages), len(collected),
				stats.p50, stats.p95, stats.p99, stats.max)
		})
	}
}
