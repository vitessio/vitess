package fakes

import (
	"golang.org/x/net/context"

	"github.com/golang/protobuf/proto"

	querypb "github.com/youtube/vitess/go/vt/proto/query"
	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
	"github.com/youtube/vitess/go/vt/vttablet/queryservice"
)

const (
	// DefaultSecondsBehindMaster is the default MySQL replication lag which is
	// reported in all faked stream health responses.
	DefaultSecondsBehindMaster uint32 = 1
)

// StreamHealthQueryService is a QueryService implementation which allows to
// send custom StreamHealthResponse messages by adding them to a channel.
// Note that it only works with one connected client because messages going
// into "healthResponses" are not duplicated to all clients.
//
// If you want to override other QueryService methods, embed this struct
// as anonymous field in your own QueryService fake.
type StreamHealthQueryService struct {
	queryservice.QueryService
	healthResponses chan *querypb.StreamHealthResponse
	target          querypb.Target
}

// NewStreamHealthQueryService creates a new fake query service for the target.
func NewStreamHealthQueryService(target querypb.Target) *StreamHealthQueryService {
	return &StreamHealthQueryService{
		QueryService:    ErrorQueryService,
		healthResponses: make(chan *querypb.StreamHealthResponse, 1000),
		target:          target,
	}
}

// StreamHealth implements the QueryService interface.
// It sends all queued and future healthResponses to the connected client e.g.
// the healthcheck module.
func (q *StreamHealthQueryService) StreamHealth(ctx context.Context, callback func(*querypb.StreamHealthResponse) error) error {
	for shr := range q.healthResponses {
		callback(shr)
	}
	return nil
}

// AddDefaultHealthResponse adds a faked health response to the buffer channel.
// The response will have default values typical for a healthy tablet.
func (q *StreamHealthQueryService) AddDefaultHealthResponse() {
	q.healthResponses <- &querypb.StreamHealthResponse{
		Target:  proto.Clone(&q.target).(*querypb.Target),
		Serving: true,
		RealtimeStats: &querypb.RealtimeStats{
			SecondsBehindMaster: DefaultSecondsBehindMaster,
		},
	}
}

// AddHealthResponseWithQPS adds a faked health response to the buffer channel.
// Only "qps" is different in this message.
func (q *StreamHealthQueryService) AddHealthResponseWithQPS(qps float64) {
	q.healthResponses <- &querypb.StreamHealthResponse{
		Target:  proto.Clone(&q.target).(*querypb.Target),
		Serving: true,
		RealtimeStats: &querypb.RealtimeStats{
			Qps:                 qps,
			SecondsBehindMaster: DefaultSecondsBehindMaster,
		},
	}
}

// AddHealthResponseWithSecondsBehindMaster adds a faked health response to the
// buffer channel. Only "seconds_behind_master" is different in this message.
func (q *StreamHealthQueryService) AddHealthResponseWithSecondsBehindMaster(replicationLag uint32) {
	q.healthResponses <- &querypb.StreamHealthResponse{
		Target:  proto.Clone(&q.target).(*querypb.Target),
		Serving: true,
		RealtimeStats: &querypb.RealtimeStats{
			SecondsBehindMaster: replicationLag,
		},
	}
}

// AddHealthResponseWithNotServing adds a faked health response to the
// buffer channel. Only "Serving" is different in this message.
func (q *StreamHealthQueryService) AddHealthResponseWithNotServing() {
	q.healthResponses <- &querypb.StreamHealthResponse{
		Target:  proto.Clone(&q.target).(*querypb.Target),
		Serving: false,
		RealtimeStats: &querypb.RealtimeStats{
			SecondsBehindMaster: DefaultSecondsBehindMaster,
		},
	}
}

// UpdateType changes the type of the query service.
// Only newly sent health messages will use the new type.
func (q *StreamHealthQueryService) UpdateType(tabletType topodatapb.TabletType) {
	q.target.TabletType = tabletType
}
