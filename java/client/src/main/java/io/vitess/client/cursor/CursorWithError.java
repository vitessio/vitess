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

package io.vitess.client.cursor;

import io.vitess.proto.Query;
import io.vitess.proto.Vtrpc;

/**
 * Created by harshit.gangal on 22/12/16.
 */
public class CursorWithError {

  private final Cursor cursor;
  private final Vtrpc.RPCError error;

  public CursorWithError(Query.ResultWithError resultWithError) {
    if (!resultWithError.hasError()
        || Vtrpc.Code.OK == resultWithError.getError().getCode()) {
      this.cursor = new SimpleCursor(resultWithError.getResult());
      this.error = null;
    } else {
      this.cursor = null;
      this.error = resultWithError.getError();
    }
  }

  public Cursor getCursor() {
    return cursor;
  }

  public Vtrpc.RPCError getError() {
    return error;
  }
}
