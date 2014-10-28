package com.youtube.vitess.vtgate;

import java.math.BigDecimal;
import java.util.Iterator;
import java.util.LinkedList;

import org.joda.time.DateTime;

import com.google.common.collect.ImmutableMap;
import com.google.common.primitives.UnsignedLong;
import com.youtube.vitess.vtgate.Exceptions.InvalidFieldException;
import com.youtube.vitess.vtgate.Row.Cell;

public class Row implements Iterator<Cell>, Iterable<Cell> {
  private ImmutableMap<String, Cell> contents;
  private Iterator<String> iterator;

  public Row(LinkedList<Cell> cells) {
    ImmutableMap.Builder<String, Cell> builder = new ImmutableMap.Builder<>();
    for (Cell cell : cells) {
      builder.put(cell.getName(), cell);
    }
    contents = builder.build();
    iterator = contents.keySet().iterator();
  }

  public int size() {
    return contents.keySet().size();
  }

  public Object getObject(int index) throws InvalidFieldException {
    if (index >= size()) {
      throw new InvalidFieldException("invalid field index " + index);
    }
    return getObject(contents.keySet().asList().get(index));
  }

  public Object getObject(String fieldName) throws InvalidFieldException {
    if (!contents.containsKey(fieldName)) {
      throw new InvalidFieldException("invalid field name " + fieldName);
    }
    return contents.get(fieldName).getValue();
  }

  public Integer getInt(String fieldName) throws InvalidFieldException {
    return (Integer) getAndCheckType(fieldName, Integer.class);
  }

  public Integer getInt(int index) throws InvalidFieldException {
    return (Integer) getAndCheckType(index, Integer.class);
  }

  public UnsignedLong getULong(String fieldName) throws InvalidFieldException {
    return (UnsignedLong) getAndCheckType(fieldName, UnsignedLong.class);
  }

  public UnsignedLong getULong(int index) throws InvalidFieldException {
    return (UnsignedLong) getAndCheckType(index, UnsignedLong.class);
  }

  public String getString(String fieldName) throws InvalidFieldException {
    return (String) getAndCheckType(fieldName, String.class);
  }

  public String getString(int index) throws InvalidFieldException {
    return (String) getAndCheckType(index, String.class);
  }

  public Long getLong(String fieldName) throws InvalidFieldException {
    return (Long) getAndCheckType(fieldName, Long.class);
  }

  public Long getLong(int index) throws InvalidFieldException {
    return (Long) getAndCheckType(index, Long.class);
  }

  public Double getDouble(String fieldName) throws InvalidFieldException {
    return (Double) getAndCheckType(fieldName, Double.class);
  }

  public Double getDouble(int index) throws InvalidFieldException {
    return (Double) getAndCheckType(index, Double.class);
  }

  public Float getFloat(String fieldName) throws InvalidFieldException {
    return (Float) getAndCheckType(fieldName, Float.class);
  }

  public Float getFloat(int index) throws InvalidFieldException {
    return (Float) getAndCheckType(index, Float.class);
  }

  public DateTime getDateTime(String fieldName) throws InvalidFieldException {
    return (DateTime) getAndCheckType(fieldName, DateTime.class);
  }

  public DateTime getDateTime(int index) throws InvalidFieldException {
    return (DateTime) getAndCheckType(index, DateTime.class);
  }

  public byte[] getBytes(String fieldName) throws InvalidFieldException {
    return (byte[]) getAndCheckType(fieldName, byte[].class);
  }

  public byte[] getBytes(int index) throws InvalidFieldException {
    return (byte[]) getAndCheckType(index, byte[].class);
  }

  public BigDecimal getBigDecimal(String fieldName) throws InvalidFieldException {
    return (BigDecimal) getAndCheckType(fieldName, BigDecimal.class);
  }

  public BigDecimal getBigDecimal(int index) throws InvalidFieldException {
    return (BigDecimal) getAndCheckType(index, BigDecimal.class);
  }

  public Short getShort(String fieldName) throws InvalidFieldException {
    return (Short) getAndCheckType(fieldName, Short.class);
  }

  public Short getShort(int index) throws InvalidFieldException {
    return (Short) getAndCheckType(index, Short.class);
  }

  private Object getAndCheckType(String fieldName, Class clazz) throws InvalidFieldException {
    Object o = getObject(fieldName);
    if (o != null && !clazz.isInstance(o)) {
      throw new InvalidFieldException("type mismatch expected:" + clazz.getName() + "actual: "
          + o.getClass().getName());
    }
    return o;
  }

  private Object getAndCheckType(int index, Class clazz) throws InvalidFieldException {
    return getAndCheckType(contents.keySet().asList().get(index), clazz);
  }

  @Override
  public boolean hasNext() {
    return iterator.hasNext();
  }

  @Override
  public Cell next() {
    return contents.get(iterator.next());
  }

  @Override
  public void remove() {
    throw new UnsupportedOperationException("can't remove from row");
  }

  @Override
  public Iterator<Cell> iterator() {
    return this;
  }

  public static class Cell {
    private String name;
    private Object value;
    private Class type;

    public Cell(String name, Object value, Class type) {
      this.name = name;
      this.value = value;
      this.type = type;
    }

    public String getName() {
      return name;
    }

    public Object getValue() {
      return value;
    }

    public Class getType() {
      return type;
    }
  }
}
