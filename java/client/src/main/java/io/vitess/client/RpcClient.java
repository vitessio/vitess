/*
 * Copyright 2019 The Vitess Authors.

 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at

 *     http://www.apache.org/licenses/LICENSE-2.0

 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.vitess.client;

import com.google.common.util.concurrent.ListenableFuture;

import io.vitess.proto.Query.QueryResult;
import io.vitess.proto.Vtgate;
import io.vitess.proto.Vtgate.ExecuteRequest;
import io.vitess.proto.Vtgate.ExecuteResponse;
import io.vitess.proto.Vtgate.StreamExecuteRequest;
import io.vitess.proto.Vtgate.VStreamRequest;
import io.vitess.proto.Vtgate.VStreamResponse;

import java.io.Closeable;
import java.sql.SQLException;

/**
 * RpcClient defines a set of methods to communicate with VTGates.
 */
public interface RpcClient extends Closeable {

  /**
   * Sends a single query using the VTGate V3 API.
   *
   * <p>See the
   * <a href="https://github.com/vitessio/vitess/blob/master/proto/vtgateservice.proto">proto</a>
   * definition for canonical documentation on this VTGate API.
   */
  ListenableFuture<ExecuteResponse> execute(Context ctx, ExecuteRequest request)
      throws SQLException;

  /**
   * Sends a list of queries using the VTGate V3 API.
   *
   * <p>See the
   * <a href="https://github.com/vitessio/vitess/blob/master/proto/vtgateservice.proto">proto</a>
   * definition for canonical documentation on this VTGate API.
   */
  ListenableFuture<Vtgate.ExecuteBatchResponse> executeBatch(Context ctx,
      Vtgate.ExecuteBatchRequest request)
      throws SQLException;

  /**
   * Starts stream queries with the VTGate V3 API.
   *
   * <p>Note: Streaming queries are not asynchronous, because they typically shouldn't
   * be used from a latency-critical serving path anyway. This method will return as soon as the
   * request is initiated, but StreamIterator methods will block until the next chunk of results is
   * received from the server.
   *
   * <p>See the
   * <a href="https://github.com/vitessio/vitess/blob/master/proto/vtgateservice.proto">proto</a>
   * definition for canonical documentation on this VTGate API.
   */
  StreamIterator<QueryResult> streamExecute(Context ctx, StreamExecuteRequest request)
      throws SQLException;

  /**
   * Starts streaming the vstream binlog events.
   *
   * Stream begins at the specified VGTID.
   *
   * <p>See the
   * <a href="https://github.com/vitessio/vitess/blob/master/proto/vtgateservice.proto">proto</a>
   * definition for canonical documentation on this VTGate API.
   */
  StreamIterator<VStreamResponse> getVStream(
      Context ctx, VStreamRequest vstreamRequest) throws SQLException;
}
