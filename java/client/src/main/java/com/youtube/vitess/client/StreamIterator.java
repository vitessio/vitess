package com.youtube.vitess.client;

import java.util.NoSuchElementException;

/**
 * StreamIterator is used to access the results of a streaming call.
 *
 * It is similar to java.util.Iterator, but the hasNext() method is
 * understood to block until either a result is ready, an error occurs,
 * or there are no more results. Also, unlike Iterator, these methods
 * can throw VitessException or VitessRpcException.
 */
public interface StreamIterator<E> {
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
  boolean hasNext() throws VitessException, VitessRpcException;

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
  E next() throws NoSuchElementException, VitessException, VitessRpcException;
}
