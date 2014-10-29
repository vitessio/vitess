package com.youtube.vitess.vtgate.hadoop.writables;

import com.google.common.primitives.UnsignedLong;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import com.youtube.vitess.vtgate.KeyspaceId;
import com.youtube.vitess.vtgate.utils.GsonAdapters;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Serializable version of {@link KeyspaceId}
 */
public class KeyspaceIdWritable implements WritableComparable<KeyspaceIdWritable> {
  private KeyspaceId keyspaceId;
  // KeyspaceId can be UnsignedLong which needs a custom adapter
  private Gson gson = new GsonBuilder().registerTypeAdapter(UnsignedLong.class,
      GsonAdapters.UNSIGNED_LONG).create();

  public KeyspaceIdWritable() {}

  public KeyspaceIdWritable(KeyspaceId keyspaceId) {
    this.keyspaceId = keyspaceId;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    if (keyspaceId.getId() instanceof UnsignedLong) {
      out.writeUTF("UnsignedLong");
      out.writeUTF(((UnsignedLong) (keyspaceId.getId())).toString());
    } else {
      out.writeUTF("String");
      out.writeUTF((String) (keyspaceId.getId()));
    }
  }

  public KeyspaceId getKeyspaceId() {
    return keyspaceId;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    String type = in.readUTF();
    Object id;
    if ("UnsignedLong".equals(type)) {
      id = UnsignedLong.valueOf(in.readUTF());
    } else {
      id = in.readUTF();
    }
    keyspaceId = KeyspaceId.valueOf(id);
  }

  @Override
  public String toString() {
    return toJson();
  }

  public String toJson() {
    return gson.toJson(keyspaceId, KeyspaceId.class);
  }

  @Override
  public int compareTo(KeyspaceIdWritable o) {
    if (o == null) {
      throw new NullPointerException();
    }

    return this.keyspaceId.compareTo(o.keyspaceId);
  }

  @Override
  public boolean equals(Object o) {
    if (o instanceof KeyspaceIdWritable) {
      return this.compareTo((KeyspaceIdWritable) o) == 0;
    }
    return false;
  }

  @Override
  public int hashCode() {
    return keyspaceId.hashCode();
  }

}
