package io.vitess.client.cursor;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.collect.ImmutableList;
import io.vitess.proto.Query.Field;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.commons.collections4.map.CaseInsensitiveMap;

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
  private final Map<String, Integer> indexMap;

  public FieldMap(Iterable<Field> fields) {
    this.fields = ImmutableList.copyOf(checkNotNull(fields));

    indexMap = new CaseInsensitiveMap<String, Integer>();
    // columnIndex is 1-based.
    int columnIndex = 1;
    for (Field field : this.fields) {
      String columnLabel = field.getName();
      // If multiple columns have the same name,
      // prefer the earlier one as JDBC ResultSet does.
      if (!indexMap.containsKey(columnLabel)) {
        indexMap.put(columnLabel, columnIndex);
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
    return indexMap.get(columnLabel);
  }
}
