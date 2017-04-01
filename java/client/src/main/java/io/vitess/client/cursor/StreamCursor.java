package io.vitess.client.cursor;

import io.vitess.client.StreamIterator;
import io.vitess.proto.Query;
import io.vitess.proto.Query.Field;
import io.vitess.proto.Query.QueryResult;
import java.sql.SQLDataException;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Iterator;
import java.util.List;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * A {@link Cursor} that serves records from the sequence of {@link QueryResult} objects
 * represented by a {@link StreamIterator}.
 */
@NotThreadSafe
public class StreamCursor extends Cursor {
  private StreamIterator<QueryResult> streamIterator;
  private Iterator<Query.Row> rowIterator;

  private List<Field> fields;

  public StreamCursor(StreamIterator<QueryResult> streamIterator) {
    this.streamIterator = streamIterator;
  }

  @Override
  public long getRowsAffected() throws SQLException {
    throw new SQLFeatureNotSupportedException("getRowsAffected() is not supported on StreamCursor");
  }

  @Override
  public long getInsertId() throws SQLException {
    throw new SQLFeatureNotSupportedException("getInsertId() is not supported on StreamCursor");
  }

  @Override
  public List<Field> getFields() throws SQLException {
    if (streamIterator == null) {
      throw new SQLDataException("getFields() called on closed Cursor");
    }

    if (fields == null) {
      // The first QueryResult should have the fields.
      if (!nextQueryResult()) {
        throw new SQLDataException("stream ended before fields were received");
      }
    }

    return fields;
  }

  @Override
  public void close() throws Exception {
    streamIterator.close();
    streamIterator = null;
  }

  @Override
  public Row next() throws SQLException {
    if (streamIterator == null) {
      throw new SQLDataException("next() called on closed Cursor");
    }

    // Get the next Row from the current QueryResult.
    if (rowIterator != null && rowIterator.hasNext()) {
      return new Row(getFieldMap(), rowIterator.next());
    }

    // Get the next QueryResult. Loop in case we get a QueryResult with no Rows (e.g. only Fields).
    while (nextQueryResult()) {
      // Get the first Row from the new QueryResult.
      if (rowIterator.hasNext()) {
        return new Row(getFieldMap(), rowIterator.next());
      }
    }

    // No more Rows and no more QueryResults.
    return null;
  }

  /**
   * Fetches the next {@link QueryResult} from the stream.
   *
   * <p>Whereas the public {@link #next()} method advances the {@link Cursor} state to the next
   * {@link Row}, this method advances the internal state to the next {@link QueryResult}, which
   * contains a batch of rows. Specifically, we get the next {@link QueryResult} from
   * {@link #streamIterator}, and then set {@link #rowIterator} accordingly.
   *
   * <p>If {@link #fields} is null, we assume the next {@link QueryResult} must contain the fields,
   * and set {@link #fields} from it.
   *
   * @return false if there are no more results in the stream.
   */
  private boolean nextQueryResult() throws SQLException {
    if (streamIterator.hasNext()) {
      QueryResult queryResult = streamIterator.next();
      if (fields == null) {
        // The first QueryResult should have the fields.
        fields = queryResult.getFieldsList();
      }
      rowIterator = queryResult.getRowsList().iterator();
      return true;
    } else {
      rowIterator = null;
      return false;
    }
  }
}
