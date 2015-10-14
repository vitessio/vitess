package com.youtube.vitess.hadoop;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.io.BaseEncoding;
import com.youtube.vitess.client.cursor.Row;
import com.youtube.vitess.proto.Query;
import com.youtube.vitess.proto.Query.Field;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import java.util.Map;

public class RowWritable implements Writable {

  private Row row;

  public RowWritable(Row row) {
    this.row = row;
  }

  public Row get() { return row; }

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
    int fieldLen = in.readInt();
    List<Field> fields = Lists.newArrayListWithCapacity(fieldLen);
    for (int i=0; i < fieldLen; i++) {
      fields.add(Field.parseFrom(BaseEncoding.base64().decode(in.readUTF())));
    }
    Query.Row rowProto = Query.Row.parseFrom(BaseEncoding.base64().decode(in.readUTF()));

    ImmutableMap.Builder<String, Integer> builder = new ImmutableMap.Builder<>();
    for (int i = 0; i < fields.size(); ++i) {
      builder.put(fields.get(i).getName(), i);
    }
    Map<String, Integer> fieldMap = builder.build();

    row = new Row(fields, rowProto, fieldMap);
  }
}
