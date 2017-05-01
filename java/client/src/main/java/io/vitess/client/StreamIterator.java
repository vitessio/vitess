package io.vitess.client;

import java.sql.SQLException;
import java.util.NoSuchElementException;

/**
 * An {@link java.util.Iterator Iterator}-like interface for accessing the results of a
 * Vitess streaming call.
 *
 * <p>It is similar to {@link java.util.Iterator}, but the hasNext() method is
 * understood to block until either a result is ready, an error occurs,
 * or there are no more results. Also, unlike Iterator, these methods
 * can throw SQLException.
 *
 * <p>The {@link #close()} method should be called when done to free up threads that may be blocking
 * on the streaming connection.
 *
 * @param <E> the type of result returned by the iterator,
 *     e.g. {@link io.vitess.proto.Query.QueryResult QueryResult}
 */
public interface StreamIterator<E> extends AutoCloseable {
  /**
   * hasNext returns true if next() would return a value.
   *
   * <p>If no value is available, hasNext() will block until either:
   * <ul>
   *   <li>A value becomes available (returns true),
   *   <li>The stream completes successfully (returns false),
   *   <li>An error occurs (throws exception).
   * </ul>
   */
  boolean hasNext() throws SQLException;

  /**
   * next returns the next value if one is available.
   *
   * <p>If no value is available, next() will block until either:
   * <ul>
   *   <li>A value becomes available (returns the value),
   *   <li>The stream completes successfully (throws NoSuchElementException),
   *   <li>An error occurs (throws other exception).
   * </ul>
   */
  E next() throws NoSuchElementException, SQLException;
}
