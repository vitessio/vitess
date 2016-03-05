package com.youtube.vitess.client.cursor;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import com.youtube.vitess.proto.Query.Field;

import java.util.List;
import java.util.Map;

import javax.annotation.Nullable;

/**
 * A wrapper for {@code List<Field>} that also facilitates lookup by field name.
 *
 * <p>The field name maps to an index, rather than a Field, because that same
 * index is also used to find the value in a separate list.
 */
public class FieldMap {
  private final List<Field> fields;
  private final Map<String, Integer> indexMap;

  public FieldMap(Iterable<Field> fields) {
    this.fields = ImmutableList.copyOf(checkNotNull(fields));

    ImmutableMap.Builder<String, Integer> builder = new ImmutableMap.Builder<>();
    int i = 0;
    for (Field field : this.fields) {
      builder.put(field.getName(), i++);
    }
    indexMap = builder.build();
  }

  public List<Field> getList() {
    return fields;
  }

  public Field get(int fieldIndex) {
    checkArgument(fieldIndex >= 0);
    return fields.get(fieldIndex);
  }

  @Nullable
  public Integer getIndex(String fieldName) {
    return indexMap.get(fieldName);
  }
}
