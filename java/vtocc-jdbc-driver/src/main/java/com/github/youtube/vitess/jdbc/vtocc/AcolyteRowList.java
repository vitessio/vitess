package com.github.youtube.vitess.jdbc.vtocc;


import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.protobuf.ByteString;

import com.github.youtube.vitess.jdbc.vtocc.QueryService.QueryResult;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import javax.annotation.Nullable;

import acolyte.Row;
import acolyte.RowList;

/**
 * Provides {@link acolyte.RowList} instances that wrap vtocc's {@link com.github.youtube.vitess.jdbc.vtocc.QueryService.QueryResult}.
 *
 * Instances returned are read-only.
 *
 * Work is done in two phases: by using {@link QueryService.Field.Type} provided by vtocc Java class
 * for data storage is selected, and byte array provided by MySQL through vtocc is converted into
 * the type selected.
 *
 * Instances are only called from within {@link acolyte.Driver}, contract is not well defined
 * therefore there are no unit tests. This code is tested as a part of integration tests
 * running SQL queries through JDBC.
 */
public class AcolyteRowList extends RowList {

  private final QueryResult queryResult;

  private AcolyteRowList(QueryResult queryResult) {
    this.queryResult = queryResult;
  }

  /**
   * Extracts data from protobuf, which contains UTF-8 strings as data. Uses {@link
   * #getColumnClasses()} to determine column class by its MySQL type.
   */
  @Override
  public List<Row> getRows() {
    return Lists.transform(queryResult.getRowsList(), new Function<QueryService.Row, Row>() {
      @Override
      public Row apply(final QueryService.Row row) {
        return new Row() {
          @Override
          public List<Object> cells() {
            List<Object> res = Lists.newArrayList();
            for (int i = 0; i < row.getValuesList().size(); i++) {
              if (!row.getValues(i).hasValue()) {
                res.add(null);
                continue;
              }
              Class<?> aClass = getColumnClasses().get(i);
              ByteString rawValue = row.getValues(i).getValue();
              if (byte[].class.isAssignableFrom(aClass)) {
                res.add(rawValue.toByteArray());
                continue;
              }
              String data = rawValue.toStringUtf8();
              if (String.class.isAssignableFrom(aClass)) {
                res.add(data);
              } else if (Boolean.class.isAssignableFrom(aClass)) {
                res.add(!data.isEmpty() && data.charAt(0) != '0');
              } else if (Integer.class.isAssignableFrom(aClass)) {
                res.add(Integer.parseInt(data));
              } else if (Long.class.isAssignableFrom(aClass)) {
                res.add(Long.parseLong(data));
              } else if (Float.class.isAssignableFrom(aClass)) {
                res.add(Float.parseFloat(data));
              } else if (Double.class.isAssignableFrom(aClass)) {
                res.add(Double.parseDouble(data));
              } else if (BigInteger.class.isAssignableFrom(aClass)) {
                res.add(new BigInteger(data));
              } else if (BigDecimal.class.isAssignableFrom(aClass)) {
                res.add(new BigDecimal(data));
              } else if (Time.class.isAssignableFrom(aClass)) {
                res.add(Time.valueOf(data));
              } else if (Date.class.isAssignableFrom(aClass)) {
                if (data.matches("\\d+")) {
                  res.add(Date.valueOf(data + "-01-01"));
                } else {
                  res.add(Date.valueOf(data));
                }
              } else if (Timestamp.class.isAssignableFrom(aClass)) {
                res.add(Timestamp.valueOf(data));
              } else {
                throw new IllegalArgumentException(
                    "Unsupported cell type: " + aClass.getName());
              }
            }
            return res;
          }
        };
      }
    });
  }

  @Override
  protected RowList append(Row row) {
    throw new UnsupportedOperationException("Result is non-modifiable");
  }

  @Override
  public RowList withLabel(int i, String s) {
    throw new UnsupportedOperationException("Result is non-modifiable");
  }

  @Override
  public RowList withNullable(int i, boolean b) {
    throw new UnsupportedOperationException("Result is non-modifiable");
  }

  /**
   * Provides meta-information about our data. Is used to determine type of the object to to convert
   * to from a string in protobuf at {@link #getRows()}.
   *
   * Is a reimplementation of {@code //third_party/golang/vitess/py/vtdb/field_types.py}. Java
   * classes mapping uses <a href=https://dev.mysql.com/doc/connector-j/en/connector-j-reference-type-conversions.html>
   * MySQL documentation</a> for reference.
   */
  @Override
  public List<Class<?>> getColumnClasses() {
    return Lists
        .transform(queryResult.getFieldsList(), new Function<QueryService.Field, Class<?>>() {
          @Nullable
          @Override
          public Class<?> apply(QueryService.Field field) {
            switch (field.getType()) {
              case DECIMAL:
                return BigDecimal.class;
              case TINY:
              case SHORT:
                return Integer.class;
              case LONG:
                return Long.class;
              case FLOAT:
                return Float.class;
              case DOUBLE:
                return Double.class;
              case NULL:
                return Object.class;
              case TIMESTAMP:
                return Timestamp.class;
              case LONGLONG:
                return BigInteger.class;
              case INT24:
                return Integer.class;
              case DATE:
                return Date.class;
              case TIME:
                return Time.class;
              case DATETIME:
                return Timestamp.class;
              case YEAR:
                return Date.class;
              case NEWDATE:
                return Timestamp.class;
              case VARCHAR:
                return String.class;
              case BIT:
                return String.class;
              case NEWDECIMAL:
                return BigDecimal.class;
              // case ENUM: not supported
              // case SET: not supported
              case TINY_BLOB:
              case MEDIUM_BLOB:
              case LONG_BLOB:
              case BLOB:
                return byte[].class;
              case VAR_STRING:
              case STRING:
                return String.class;
              default:
                throw new IllegalArgumentException(
                    "Unsupported field type: " + field.getType());
            }
          }
        });
  }

  /**
   * Provides meta-information about our data. Is not directly used for data extraction.
   */
  @Override
  public Map<String, Integer> getColumnLabels() {
    Map<String, Integer> columnLabels = Maps.newHashMap();
    int column = 1;
    for (QueryService.Field field : queryResult.getFieldsList()) {
      columnLabels.put(field.getName(), column);
      column++;
    }
    return Collections.unmodifiableMap(columnLabels);
  }

  /**
   * Provides meta-information about our data. Is not directly used for data extraction.
   */
  @Override
  public Map<Integer, Boolean> getColumnNullables() {
    Map<Integer, Boolean> columnLabels = Maps.newHashMap();
    int column = 1;
    for (QueryService.Field ignored : queryResult.getFieldsList()) {
      // Queryservice does not support nullable columns
      columnLabels.put(column, null);
      column++;
    }
    return Collections.unmodifiableMap(columnLabels);
  }

  /**
   * Factory for {@link AcolyteRowList}.
   *
   * Factory is required as {@link com.github.youtube.vitess.jdbc.vtocc.QueryService.QueryResult}
   * can not be injected.
   */
  public static class Factory {

    public RowList create(QueryResult queryResult) {
      return new AcolyteRowList(queryResult);
    }
  }
}
