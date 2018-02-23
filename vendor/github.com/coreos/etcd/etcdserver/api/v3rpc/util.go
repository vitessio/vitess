// Copyright 2016 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package v3rpc

import (
	"github.com/coreos/etcd/auth"
	"github.com/coreos/etcd/etcdserver"
	"github.com/coreos/etcd/etcdserver/api/v3rpc/rpctypes"
	"github.com/coreos/etcd/etcdserver/membership"
	"github.com/coreos/etcd/lease"
	"github.com/coreos/etcd/mvcc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
)

var toGRPCErrorMap = map[error]error{
	membership.ErrIDRemoved:               rpctypes.ErrGRPCMemberNotFound,
	membership.ErrIDNotFound:              rpctypes.ErrGRPCMemberNotFound,
	membership.ErrIDExists:                rpctypes.ErrGRPCMemberExist,
	membership.ErrPeerURLexists:           rpctypes.ErrGRPCPeerURLExist,
	etcdserver.ErrNotEnoughStartedMembers: rpctypes.ErrMemberNotEnoughStarted,

	mvcc.ErrCompacted:             rpctypes.ErrGRPCCompacted,
	mvcc.ErrFutureRev:             rpctypes.ErrGRPCFutureRev,
	etcdserver.ErrRequestTooLarge: rpctypes.ErrGRPCRequestTooLarge,
	etcdserver.ErrNoSpace:         rpctypes.ErrGRPCNoSpace,
	etcdserver.ErrTooManyRequests: rpctypes.ErrTooManyRequests,

	etcdserver.ErrNoLeader:                   rpctypes.ErrGRPCNoLeader,
	etcdserver.ErrStopped:                    rpctypes.ErrGRPCStopped,
	etcdserver.ErrTimeout:                    rpctypes.ErrGRPCTimeout,
	etcdserver.ErrTimeoutDueToLeaderFail:     rpctypes.ErrGRPCTimeoutDueToLeaderFail,
	etcdserver.ErrTimeoutDueToConnectionLost: rpctypes.ErrGRPCTimeoutDueToConnectionLost,
	etcdserver.ErrUnhealthy:                  rpctypes.ErrGRPCUnhealthy,
	etcdserver.ErrKeyNotFound:                rpctypes.ErrGRPCKeyNotFound,

	lease.ErrLeaseNotFound: rpctypes.ErrGRPCLeaseNotFound,
	lease.ErrLeaseExists:   rpctypes.ErrGRPCLeaseExist,

	auth.ErrRootUserNotExist:     rpctypes.ErrGRPCRootUserNotExist,
	auth.ErrRootRoleNotExist:     rpctypes.ErrGRPCRootRoleNotExist,
	auth.ErrUserAlreadyExist:     rpctypes.ErrGRPCUserAlreadyExist,
	auth.ErrUserEmpty:            rpctypes.ErrGRPCUserEmpty,
	auth.ErrUserNotFound:         rpctypes.ErrGRPCUserNotFound,
	auth.ErrRoleAlreadyExist:     rpctypes.ErrGRPCRoleAlreadyExist,
	auth.ErrRoleNotFound:         rpctypes.ErrGRPCRoleNotFound,
	auth.ErrAuthFailed:           rpctypes.ErrGRPCAuthFailed,
	auth.ErrPermissionDenied:     rpctypes.ErrGRPCPermissionDenied,
	auth.ErrRoleNotGranted:       rpctypes.ErrGRPCRoleNotGranted,
	auth.ErrPermissionNotGranted: rpctypes.ErrGRPCPermissionNotGranted,
	auth.ErrAuthNotEnabled:       rpctypes.ErrGRPCAuthNotEnabled,
	auth.ErrInvalidAuthToken:     rpctypes.ErrGRPCInvalidAuthToken,
	auth.ErrInvalidAuthMgmt:      rpctypes.ErrGRPCInvalidAuthMgmt,
}

func togRPCError(err error) error {
	grpcErr, ok := toGRPCErrorMap[err]
	if !ok {
		return grpc.Errorf(codes.Unknown, err.Error())
	}
	return grpcErr
}
