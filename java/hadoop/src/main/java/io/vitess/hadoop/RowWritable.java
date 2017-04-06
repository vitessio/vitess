package io.vitess.hadoop;

import com.google.common.collect.Lists;
import com.google.common.io.BaseEncoding;
import com.google.gson.Gson;
import io.vitess.client.cursor.Row;
import io.vitess.proto.Query;
import io.vitess.proto.Query.Field;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.io.Writable;

public class RowWritable implements Writable {
  private Row row;

  public RowWritable() {}

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
        } catch (SQLException e) {
          map.put(key, e.toString());
        }
      }
    }

    return new Gson().toJson(map);
  }
}
