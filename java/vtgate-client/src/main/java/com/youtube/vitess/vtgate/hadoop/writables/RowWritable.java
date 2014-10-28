package com.youtube.vitess.vtgate.hadoop.writables;

import com.google.common.primitives.UnsignedLong;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import com.youtube.vitess.vtgate.Row;
import com.youtube.vitess.vtgate.Row.Cell;
import com.youtube.vitess.vtgate.utils.GsonAdapters;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Date;
import java.util.LinkedList;

/**
 * Serializable version of {@link Row}. Only implements {@link Writable} and not
 * {@link WritableComparable} since this is not meant to be used as a key.
 *
 */
public class RowWritable implements Writable {
  private Row row;

  // Row contains UnsignedLong and Class objects which need custom adapters
  private Gson gson = new GsonBuilder()
      .registerTypeAdapter(UnsignedLong.class, GsonAdapters.UNSIGNED_LONG)
      .registerTypeAdapter(Class.class, GsonAdapters.CLASS).create();

  public RowWritable() {

  }

  public RowWritable(Row row) {
    this.row = row;
  }

  public Row getRow() {
    return row;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(row.size());
    for (Cell cell : row) {
      writeCell(out, cell);
    }
  }

  public void writeCell(DataOutput out, Cell cell) throws IOException {
    out.writeUTF(cell.getName());
    out.writeUTF(cell.getType().getName());
    out.writeUTF(cell.getValue().toString());
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    int size = in.readInt();
    LinkedList<Cell> cells = new LinkedList<>();
    for (int i = 0; i < size; i++) {
      cells.add(readCell(in));
    }
    row = new Row(cells);
  }

  public Cell readCell(DataInput in) throws IOException {
    String name = in.readUTF();
    String type = in.readUTF();
    String value = in.readUTF();
    Object val = null;
    Class clazz;
    try {
      clazz = Class.forName(type);
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
    if (clazz.equals(Double.class)) {
      val = Double.valueOf(value);
    }
    if (clazz.equals(Integer.class)) {
      val = Integer.valueOf(value);
    }
    if (clazz.equals(Long.class)) {
      val = Long.valueOf(value);
    }
    if (clazz.equals(Float.class)) {
      val = Float.valueOf(value);
    }
    if (clazz.equals(UnsignedLong.class)) {
      val = UnsignedLong.valueOf(value);
    }
    if (clazz.equals(Date.class)) {
      val = Date.parse(value);
    }
    if (clazz.equals(String.class)) {
      val = value;
    }
    if (val == null) {
      throw new RuntimeException("unknown type in RowWritable: " + clazz);
    }
    return new Cell(name, val, clazz);
  }

  @Override
  public String toString() {
    return toJson();
  }

  public String toJson() {
    return gson.toJson(row, Row.class);
  }
}
