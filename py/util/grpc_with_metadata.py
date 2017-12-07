# Copyright 2017 Google Inc.
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#     http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""This file contains the grpc channel decorator to have metadata set per call
"""

class GRPCWithMetadataCallable:
    def __init__(self, callable, getMetadata):
        """Constructor.
        Args:
        callable: Underlying channel
        getMetadata: method to call to get metadata for the grpc request.
        """
        self.callable = callable
        self.getMetadata = getMetadata

    def __call__(self,
                 request,
                 timeout=None,
                 metadata=None,
                 credentials=None):
      call_metadata = self.getMetadata()
      if metadata is not None and call_metadata is not None:
        call_metadata = metadata + call_metadata
      return self.callable(request, timeout, metadata=call_metadata, credentials=credentials)

class GRPCWithMetadataChannel:
    """This class provides a decorator for grpc channels where the caller can set up
       metadata to be attached to all calls to the underlying stub.
    """
    def __init__(self, channel, getMetadata):
      self.channel = channel
      self.getMetadata = getMetadata

    def unary_unary(self,
                    method,
                    request_serializer=None,
                    response_deserializer=None):
      return GRPCWithMetadataCallable(
          self.channel.unary_unary(method, request_serializer, response_deserializer),
          self.getMetadata)

    def unary_stream(self,
                     method,
                     request_serializer=None,
                     response_deserializer=None):
      return GRPCWithMetadataCallable(
          self.channel.unary_stream(method, request_serializer, response_deserializer),
          self.getMetadata)

    def stream_unary(self,
                     method,
                     request_serializer=None,
                     response_deserializer=None):
      return GRPCWithMetadataCallable(
          self.channel.stream_unary(method, request_serializer, response_deserializer),
          self.getMetadata)

    def stream_stream(self,
                      method,
                      request_serializer=None,
                      response_deserializer=None):
      return GRPCWithMetadataCallable(
          self.channel.stream_stream(method, request_serializer, response_deserializer),
          self.getMetadata)
