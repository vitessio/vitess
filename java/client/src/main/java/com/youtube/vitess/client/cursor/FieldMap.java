package com.youtube.vitess.client.cursor;

import com.youtube.vitess.proto.Query.Field;
import org.apache.commons.collections.map.CaseInsensitiveMap;

import java.util.List;
import java.util.Map;

/**
 * A wrapper for {@code List<Field>} that also facilitates lookup by field name.
 *
 * <p>The field name maps to an index, rather than a Field, because that same
 * index is also used to find the value in a separate list.
 */
public class FieldMap {
  private final List<Field> fields;
  private final Map<String, Integer> indexMap;

  public FieldMap(List<Field> fields) {
    indexMap = new CaseInsensitiveMap();
    for (int i = 0; i < fields.size(); i++) {
      indexMap.put(fields.get(i).getName(), i);
    }
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
