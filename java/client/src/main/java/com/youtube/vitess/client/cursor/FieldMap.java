package com.youtube.vitess.client.cursor;

import com.google.common.collect.ImmutableMap;

import com.youtube.vitess.proto.Query.Field;

import java.util.List;
import java.util.Map;

/**
 * A wrapper for {@code List<Field>} that also facilitates lookup by field name.
 *
 * <p>The field name maps to an index, rather than a Field, because that same
 * index is also used to find the value in a separate list.
 */
public class FieldMap {
  private List<Field> fields;
  private Map<String, Integer> indexMap;

  public FieldMap(List<Field> fields) {
    ImmutableMap.Builder<String, Integer> builder = new ImmutableMap.Builder<>();
    for (int i = 0; i < fields.size(); i++) {
      builder.put(fields.get(i).getName(), i);
    }
    indexMap = builder.build();
    this.fields = fields;
  }

  public List<Field> getList() {
    return fields;
  }

  public Map<String, Integer> getIndexMap() {
    return indexMap;
  }

  public Field get(int i) {
    return fields.get(i);
  }

  public Integer getIndex(String fieldName) {
    return indexMap.get(fieldName);
  }
}
