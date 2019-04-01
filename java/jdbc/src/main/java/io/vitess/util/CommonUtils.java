/*
 * Copyright 2017 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.vitess.util;

import io.vitess.client.Context;
import io.vitess.proto.Vtrpc;

import org.joda.time.Duration;

/**
 * Created by naveen.nahata on 24/02/16.
 */
public class CommonUtils {

  /**
   * Create context used to create grpc client and executing query.
   */
  public static Context createContext(String username, long timeout) {
    Context context = Context.getDefault();
    Vtrpc.CallerID callerID = null;
    if (null != username) {
      callerID = Vtrpc.CallerID.newBuilder().setPrincipal(username).build();
    }

    if (null != callerID) {
      context = context.withCallerId(callerID);
    }
    if (timeout > 0) {
      context = context.withDeadlineAfter(Duration.millis(timeout));
    }

    return context;
  }
}

