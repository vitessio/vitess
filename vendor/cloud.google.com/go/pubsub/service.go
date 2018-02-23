// Copyright 2016 Google Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package pubsub

import (
	"fmt"
	"math"
	"time"

	"cloud.google.com/go/iam"
	vkit "cloud.google.com/go/pubsub/apiv1"
	"golang.org/x/net/context"
	"google.golang.org/api/option"
	pb "google.golang.org/genproto/googleapis/pubsub/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
)

const version = "0.2.0"

type nextStringFunc func() (string, error)

// service provides an internal abstraction to isolate the generated
// PubSub API; most of this package uses this interface instead.
// The single implementation, *apiService, contains all the knowledge
// of the generated PubSub API (except for that present in legacy code).
type service interface {
	createSubscription(ctx context.Context, topicName, subName string, ackDeadline time.Duration, pushConfig *PushConfig) error
	getSubscriptionConfig(ctx context.Context, subName string) (*SubscriptionConfig, string, error)
	listProjectSubscriptions(ctx context.Context, projName string) nextStringFunc
	deleteSubscription(ctx context.Context, name string) error
	subscriptionExists(ctx context.Context, name string) (bool, error)
	modifyPushConfig(ctx context.Context, subName string, conf *PushConfig) error

	createTopic(ctx context.Context, name string) error
	deleteTopic(ctx context.Context, name string) error
	topicExists(ctx context.Context, name string) (bool, error)
	listProjectTopics(ctx context.Context, projName string) nextStringFunc
	listTopicSubscriptions(ctx context.Context, topicName string) nextStringFunc

	modifyAckDeadline(ctx context.Context, subName string, deadline time.Duration, ackIDs []string) error
	fetchMessages(ctx context.Context, subName string, maxMessages int32) ([]*Message, error)
	publishMessages(ctx context.Context, topicName string, msgs []*Message) ([]string, error)

	// splitAckIDs divides ackIDs into
	//  * a batch of a size which is suitable for passing to acknowledge or
	//    modifyAckDeadline, and
	//  * the rest.
	splitAckIDs(ackIDs []string) ([]string, []string)

	// acknowledge ACKs the IDs in ackIDs.
	acknowledge(ctx context.Context, subName string, ackIDs []string) error

	iamHandle(resourceName string) *iam.Handle

	close() error
}

type apiService struct {
	pubc *vkit.PublisherClient
	subc *vkit.SubscriberClient
}

func newPubSubService(ctx context.Context, opts []option.ClientOption) (*apiService, error) {
	pubc, err := vkit.NewPublisherClient(ctx, opts...)
	if err != nil {
		return nil, err
	}
	subc, err := vkit.NewSubscriberClient(ctx, option.WithGRPCConn(pubc.Connection()))
	if err != nil {
		_ = pubc.Close() // ignore error
		return nil, err
	}
	pubc.SetGoogleClientInfo("pubsub", version)
	subc.SetGoogleClientInfo("pubsub", version)
	return &apiService{pubc: pubc, subc: subc}, nil
}

func (s *apiService) close() error {
	// Return the first error, because the first call closes the connection.
	err := s.pubc.Close()
	_ = s.subc.Close()
	return err
}

func (s *apiService) createSubscription(ctx context.Context, topicName, subName string, ackDeadline time.Duration, pushConfig *PushConfig) error {
	var rawPushConfig *pb.PushConfig
	if pushConfig != nil {
		rawPushConfig = &pb.PushConfig{
			Attributes:   pushConfig.Attributes,
			PushEndpoint: pushConfig.Endpoint,
		}
	}
	_, err := s.subc.CreateSubscription(ctx, &pb.Subscription{
		Name:               subName,
		Topic:              topicName,
		PushConfig:         rawPushConfig,
		AckDeadlineSeconds: trunc32(int64(ackDeadline.Seconds())),
	})
	return err
}

func (s *apiService) getSubscriptionConfig(ctx context.Context, subName string) (*SubscriptionConfig, string, error) {
	rawSub, err := s.subc.GetSubscription(ctx, &pb.GetSubscriptionRequest{Subscription: subName})
	if err != nil {
		return nil, "", err
	}
	sub := &SubscriptionConfig{
		AckDeadline: time.Second * time.Duration(rawSub.AckDeadlineSeconds),
		PushConfig: PushConfig{
			Endpoint:   rawSub.PushConfig.PushEndpoint,
			Attributes: rawSub.PushConfig.Attributes,
		},
	}
	return sub, rawSub.Topic, nil
}

// stringsPage contains a list of strings and a token for fetching the next page.
type stringsPage struct {
	strings []string
	tok     string
}

func (s *apiService) listProjectSubscriptions(ctx context.Context, projName string) nextStringFunc {
	it := s.subc.ListSubscriptions(ctx, &pb.ListSubscriptionsRequest{
		Project: projName,
	})
	return func() (string, error) {
		sub, err := it.Next()
		if err != nil {
			return "", err
		}
		return sub.Name, nil
	}
}

func (s *apiService) deleteSubscription(ctx context.Context, name string) error {
	return s.subc.DeleteSubscription(ctx, &pb.DeleteSubscriptionRequest{Subscription: name})
}

func (s *apiService) subscriptionExists(ctx context.Context, name string) (bool, error) {
	_, err := s.subc.GetSubscription(ctx, &pb.GetSubscriptionRequest{Subscription: name})
	if err == nil {
		return true, nil
	}
	if grpc.Code(err) == codes.NotFound {
		return false, nil
	}
	return false, err
}

func (s *apiService) createTopic(ctx context.Context, name string) error {
	_, err := s.pubc.CreateTopic(ctx, &pb.Topic{Name: name})
	return err
}

func (s *apiService) listProjectTopics(ctx context.Context, projName string) nextStringFunc {
	it := s.pubc.ListTopics(ctx, &pb.ListTopicsRequest{
		Project: projName,
	})
	return func() (string, error) {
		topic, err := it.Next()
		if err != nil {
			return "", err
		}
		return topic.Name, nil
	}
}

func (s *apiService) deleteTopic(ctx context.Context, name string) error {
	return s.pubc.DeleteTopic(ctx, &pb.DeleteTopicRequest{Topic: name})
}

func (s *apiService) topicExists(ctx context.Context, name string) (bool, error) {
	_, err := s.pubc.GetTopic(ctx, &pb.GetTopicRequest{Topic: name})
	if err == nil {
		return true, nil
	}
	if grpc.Code(err) == codes.NotFound {
		return false, nil
	}
	return false, err
}

func (s *apiService) listTopicSubscriptions(ctx context.Context, topicName string) nextStringFunc {
	it := s.pubc.ListTopicSubscriptions(ctx, &pb.ListTopicSubscriptionsRequest{
		Topic: topicName,
	})
	return it.Next
}

func (s *apiService) modifyAckDeadline(ctx context.Context, subName string, deadline time.Duration, ackIDs []string) error {
	return s.subc.ModifyAckDeadline(ctx, &pb.ModifyAckDeadlineRequest{
		Subscription:       subName,
		AckIds:             ackIDs,
		AckDeadlineSeconds: trunc32(int64(deadline.Seconds())),
	})
}

// maxPayload is the maximum number of bytes to devote to actual ids in
// acknowledgement or modifyAckDeadline requests. A serialized
// AcknowledgeRequest proto has a small constant overhead, plus the size of the
// subscription name, plus 3 bytes per ID (a tag byte and two size bytes). A
// ModifyAckDeadlineRequest has an additional few bytes for the deadline. We
// don't know the subscription name here, so we just assume the size exclusive
// of ids is 100 bytes.
//
// With gRPC there is no way for the client to know the server's max message size (it is
// configurable on the server). We know from experience that it
// it 512K.
const (
	maxPayload       = 512 * 1024
	ackFixedOverhead = 100
	overheadPerID    = 3
)

// splitAckIDs splits ids into two slices, the first of which contains at most maxPayload bytes of ackID data.
func (s *apiService) splitAckIDs(ids []string) ([]string, []string) {
	total := ackFixedOverhead
	for i, id := range ids {
		total += len(id) + overheadPerID
		if total > maxPayload {
			return ids[:i], ids[i:]
		}
	}
	return ids, nil
}

func (s *apiService) acknowledge(ctx context.Context, subName string, ackIDs []string) error {
	return s.subc.Acknowledge(ctx, &pb.AcknowledgeRequest{
		Subscription: subName,
		AckIds:       ackIDs,
	})
}

func (s *apiService) fetchMessages(ctx context.Context, subName string, maxMessages int32) ([]*Message, error) {
	resp, err := s.subc.Pull(ctx, &pb.PullRequest{
		Subscription: subName,
		MaxMessages:  maxMessages,
	})
	if err != nil {
		return nil, err
	}
	msgs := make([]*Message, 0, len(resp.ReceivedMessages))
	for i, m := range resp.ReceivedMessages {
		msg, err := toMessage(m)
		if err != nil {
			return nil, fmt.Errorf("pubsub: cannot decode the retrieved message at index: %d, message: %+v", i, m)
		}
		msgs = append(msgs, msg)
	}
	return msgs, nil
}

func (s *apiService) publishMessages(ctx context.Context, topicName string, msgs []*Message) ([]string, error) {
	rawMsgs := make([]*pb.PubsubMessage, len(msgs))
	for i, msg := range msgs {
		rawMsgs[i] = &pb.PubsubMessage{
			Data:       msg.Data,
			Attributes: msg.Attributes,
		}
	}
	resp, err := s.pubc.Publish(ctx, &pb.PublishRequest{
		Topic:    topicName,
		Messages: rawMsgs,
	})
	if err != nil {
		return nil, err
	}
	return resp.MessageIds, nil
}

func (s *apiService) modifyPushConfig(ctx context.Context, subName string, conf *PushConfig) error {
	return s.subc.ModifyPushConfig(ctx, &pb.ModifyPushConfigRequest{
		Subscription: subName,
		PushConfig: &pb.PushConfig{
			Attributes:   conf.Attributes,
			PushEndpoint: conf.Endpoint,
		},
	})
}

func (s *apiService) iamHandle(resourceName string) *iam.Handle {
	return iam.InternalNewHandle(s.pubc.Connection(), resourceName)
}

func trunc32(i int64) int32 {
	if i > math.MaxInt32 {
		i = math.MaxInt32
	}
	return int32(i)
}
