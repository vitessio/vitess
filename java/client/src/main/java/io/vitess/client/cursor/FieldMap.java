/*
 * Copyright 2017 Google Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.vitess.client.cursor;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.commons.collections4.map.CaseInsensitiveMap;

import io.vitess.proto.Query.Field;

/**
 * A wrapper for {@code List<Field>} that also facilitates lookup by field name.
 *
 * <p>Lookups by field name are case-insensitive, as in {@link java.sql.ResultSet}.
 * If multiple fields have the same name, the earliest one will be returned.
 *
 * <p>The field name maps to an index, rather than a Field, because that same
 * index is also used to find the value in a separate list.
 */
public class FieldMap {
  private final List<Field> fields;
  private final Map<String, Integer> labelMap;
  private final Map<String, Integer> nameMap;
  private final Map<String, Integer> fullNameMap;

  public FieldMap(Iterable<Field> fields) {
    this.fields = ImmutableList.copyOf(checkNotNull(fields));

    labelMap = new CaseInsensitiveMap<String, Integer>();
    nameMap = new CaseInsensitiveMap<String, Integer>();
    fullNameMap = new CaseInsensitiveMap<String, Integer>();
    // columnIndex is 1-based.
    int columnIndex = 1;
    for (Field field : this.fields) {
      // If multiple columns have the same name,
      // prefer the earlier one as JDBC ResultSet does.
      String columnLabel = field.getName();
      if (!labelMap.containsKey(columnLabel)) {
        labelMap.put(columnLabel, columnIndex);
      }
      String origName = field.getOrgName();
      if (origName != null && !"".equals(origName) && !nameMap.containsKey(origName)) {
        nameMap.put(origName, columnIndex);
      }
      String tableName = field.getTable();
      if (tableName != null && !"".equals(tableName)) {
        StringBuilder fullNameBuf = new StringBuilder();
        fullNameBuf.append(tableName);
        fullNameBuf.append('.');
        fullNameBuf.append(field.getName());
        String fullName = fullNameBuf.toString();
        if (!fullNameMap.containsKey(fullName)) {
          fullNameMap.put(fullName, columnIndex);
        }
      }
      ++columnIndex;
    }
  }

  public List<Field> getList() {
    return fields;
  }

  /**
   * Returns the {@link Field} for a 1-based column index.
   *
   * @param columnIndex 1-based column number (0 is invalid)
   */
  public Field get(int columnIndex) {
    // columnIndex is 1-based.
    checkArgument(columnIndex >= 1, "columnIndex out of range: %s", columnIndex);
    return fields.get(columnIndex - 1);
  }

  /**
   * Returns the 1-based index for a column label.
   *
   * <p>If multiple columns have the same label,
   * the earlier one is returned.
   *
   * @param columnLabel case-insensitive column label
   */
  @Nullable
  public Integer getIndex(String columnLabel) {
    Integer index = labelMap.get(columnLabel);
    if (index == null) {
      index = nameMap.get(columnLabel);
    }
    if (index == null) {
      index = fullNameMap.get(columnLabel);
    }
    return index;
  }
}
