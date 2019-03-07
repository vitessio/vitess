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

package io.vitess.client;

import com.google.common.util.concurrent.ForwardingListenableFuture.SimpleForwardingListenableFuture;
import com.google.common.util.concurrent.ListenableFuture;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.sql.SQLException;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * A ListenableFuture with an optional getter method that throws checked SQLException.
 *
 * <p>
 * When used as a {@link ListenableFuture}, the {@link SQLException} thrown by Vitess will be
 * wrapped in {@link ExecutionException}. You can retrieve it by calling {@link
 * ExecutionException#getCause()}.
 *
 * <p>
 * For users who want to get results synchronously, we provide {@link #checkedGet()} as a
 * convenience method. Unlike {@link #get()}, it throws only {@code SQLException}, so e.g. {@code
 * vtgateConn.execute(...).checkedGet()} behaves the same as our old synchronous API.
 *
 * <p>
 * The additional methods are similar to the {@code CheckedFuture} interface (marked as beta), but
 * this class does not declare that it implements {@code CheckedFuture} because that interface is
 * not recommended for new projects. See the <a href= "https://google.github.io/guava/releases/19.0/api/docs/com/google/common/util/concurrent/CheckedFuture.html">
 * CheckedFuture docs</a> for more information.
 */
public class SQLFuture<V> extends SimpleForwardingListenableFuture<V> {

  /**
   * Creates a SQLFuture that wraps the given ListenableFuture.
   */
  public SQLFuture(ListenableFuture<V> delegate) {
    super(delegate);
  }

  /**
   * Returns the result while ensuring the appropriate SQLException is thrown for Vitess errors.
   *
   * <p>
   * This can be used to effectively turn the Vitess client into a synchronous API. For example:
   * {@code Cursor cursor = vtgateConn.execute(...).checkedGet();}
   */
  public V checkedGet() throws SQLException {
    try {
      return get();
    } catch (InterruptedException exc) {
      Thread.currentThread().interrupt();
      throw mapException(exc);
    } catch (CancellationException | ExecutionException exc) {
      throw mapException(exc);
    }
  }

  /**
   * Returns the result while ensuring the appropriate SQLException is thrown for Vitess errors.
   *
   * <p>
   * This can be used to effectively turn the Vitess client into a synchronous API. For example:
   * {@code Cursor cursor = vtgateConn.execute(...).checkedGet();}
   */
  public V checkedGet(long timeout, TimeUnit unit) throws TimeoutException, SQLException {
    try {
      return get(timeout, unit);
    } catch (InterruptedException exc) {
      Thread.currentThread().interrupt();
      throw mapException(exc);
    } catch (CancellationException | ExecutionException exc) {
      throw mapException(exc);
    }
  }

  /**
   * Translates from an {@link InterruptedException}, {@link CancellationException} or {@link
   * ExecutionException} thrown by {@code get} to an exception of type {@code SQLException} to be
   * thrown by {@code checkedGet}.
   *
   * <p>
   * If {@code exc} is an {@code InterruptedException}, the calling {@code checkedGet} method has
   * already restored the interrupt after catching the exception. If an implementation of {@link
   * #mapException(Exception)} wishes to swallow the interrupt, it can do so by calling {@link
   * Thread#interrupted()}.
   */
  protected SQLException mapException(Exception exc) {
    if (exc instanceof ExecutionException) {
      // To preserve both the stack trace and SQLException subclass type of the error
      // being wrapped, we use reflection to create a new instance of the particular
      // subclass of the original exception.
      Throwable cause = exc.getCause();
      if (cause instanceof SQLException) {
        SQLException se = (SQLException) cause;
        try {
          Constructor<? extends Throwable> constructor =
              cause
                  .getClass()
                  .getConstructor(String.class, String.class, int.class, Throwable.class);
          return (SQLException)
              constructor.newInstance(se.getMessage(), se.getSQLState(), se.getErrorCode(), exc);
        } catch (NoSuchMethodException
            | InstantiationException
            | IllegalAccessException
            | IllegalArgumentException
            | InvocationTargetException e1) {
          throw new RuntimeException(
              "SQLException subclass can't be instantiated: " + cause.getClass().getName(), e1);
        }
      }
    }

    return new SQLException(exc);
  }
}
