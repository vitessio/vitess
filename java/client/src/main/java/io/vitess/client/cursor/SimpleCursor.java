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
import io.vitess.proto.Query.Field;
import io.vitess.proto.Query.QueryResult;

import java.sql.SQLException;
import java.util.Iterator;
import java.util.List;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * A {@link Cursor} that serves records from a single {@link QueryResult} object.
 */
@NotThreadSafe
public class SimpleCursor extends Cursor {

  private final QueryResult queryResult;
  private final Iterator<Query.Row> rowIterator;

  public SimpleCursor(QueryResult queryResult) {
    this.queryResult = queryResult;
    rowIterator = queryResult.getRowsList().iterator();
  }

  @Override
  public long getRowsAffected() throws SQLException {
    return queryResult.getRowsAffected();
  }

  @Override
  public long getInsertId() throws SQLException {
    return queryResult.getInsertId();
  }

  @Override
  public List<Field> getFields() throws SQLException {
    return queryResult.getFieldsList();
  }

  @Override
  public void close() throws Exception {
    // SimpleCursor doesn't need to do anything.
  }

  @Override
  public Row next() throws SQLException {
    if (rowIterator.hasNext()) {
      return new Row(getFieldMap(), rowIterator.next());
    }
    return null;
  }
}
