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

package io.vitess.hadoop;

import com.google.common.collect.Lists;
import com.google.common.io.BaseEncoding;
import com.google.gson.Gson;

import io.vitess.client.cursor.Row;
import io.vitess.proto.Query;
import io.vitess.proto.Query.Field;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RowWritable implements Writable {

  private Row row;

  public RowWritable() {
  }

  public RowWritable(Row row) {
    this.row = row;
  }

  public Row get() {
    return row;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(row.getFields().size());
    for (Field field : row.getFields()) {
      out.writeUTF(BaseEncoding.base64().encode(field.toByteArray()));
    }
    out.writeUTF(BaseEncoding.base64().encode(row.getRowProto().toByteArray()));
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    int numFields = in.readInt();
    List<Field> fields = Lists.newArrayListWithCapacity(numFields);
    for (int i = 0; i < numFields; i++) {
      fields.add(Field.parseFrom(BaseEncoding.base64().decode(in.readUTF())));
    }
    Query.Row rowProto = Query.Row.parseFrom(BaseEncoding.base64().decode(in.readUTF()));
    row = new Row(fields, rowProto);
  }

  @Override
  public String toString() {
    List<Field> fields = row.getFields();

    Map<String, String> map = new HashMap<>();
    for (int i = 0; i < fields.size(); i++) {
      String key = fields.get(i).getName();
      // Prefer the first column if there are duplicate names.
      // This matches JDBC ResultSet behavior.
      if (!map.containsKey(key)) {
        try {
          map.put(key, row.getRawValue(i + 1).toStringUtf8());
        } catch (SQLException exc) {
          map.put(key, exc.toString());
        }
      }
    }

    return new Gson().toJson(map);
  }
}
