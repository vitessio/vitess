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

package io.vitess.client.grpc;

import io.grpc.stub.StreamObserver;
import io.vitess.client.StreamIterator;

import java.sql.SQLDataException;
import java.sql.SQLException;
import java.util.NoSuchElementException;

/**
 * A {@link StreamIterator} that returns results provided by a gRPC {@link StreamObserver}
 *
 * <p>This adapter allows iteration (with checked exceptions) over results obtained through the
 * gRPC {@code StreamObserver} interface.
 *
 * <p>This class is abstract because it needs to be told how to extract the result
 * (e.g. {@link io.vitess.proto.Query.QueryResult QueryResult}) from a given RPC response (e.g.
 * {@link io.vitess.proto.Vtgate.StreamExecuteResponse StreamExecuteResponse}). Callers must
 * therefore implement {@link #getResult(Object)} when instantiating this class.
 *
 * <p>The {@code StreamObserver} side will block until the result has been returned to the consumer
 * by the {@code StreamIterator} side. Therefore, the {@link #close()} method must be called when
 * done, to unblock the {@code StreamObserver} side.
 *
 * @param <V> The type of value sent through the {@link StreamObserver} interface.
 * @param <E> The type of value to return through the {@link StreamIterator} interface.
 */
abstract class GrpcStreamAdapter<V, E>
    implements StreamObserver<V>, StreamIterator<E>, AutoCloseable {

  /**
   * getResult must be implemented to tell the adapter how to convert from the StreamObserver value
   * type (V) to the StreamIterator value type (E). Before converting, getResult() should check for
   * application-level errors in the RPC response and throw the appropriate SQLException.
   *
   * @param value The RPC response object.
   * @return The result object to pass to the iterator consumer.
   * @throws SQLException For errors originating within the Vitess server.
   */
  abstract E getResult(V value) throws SQLException;

  private E nextValue;
  private Throwable error;
  private boolean completed = false;
  private boolean closed = false;

  @Override
  public void onNext(V value) {
    synchronized (this) {
      try {
        // Wait until the previous value has been consumed.
        while (nextValue != null) {
          // If there's been an error, or the iterator was closed, drain the rest of the stream
          // without blocking.
          if (closed || error != null) {
            return;
          }

          wait();
        }

        nextValue = getResult(value);
        notifyAll();
      } catch (InterruptedException exc) {
        onError(exc);
      } catch (SQLException exc) {
        onError(exc);
      }
    }
  }

  @Override
  public void onCompleted() {
    synchronized (this) {
      completed = true;
      notifyAll();
    }
  }

  @Override
  public void onError(Throwable error) {
    synchronized (this) {
      this.error = error;
      notifyAll();
    }
  }

  @Override
  public boolean hasNext() throws SQLException {
    synchronized (this) {
      try {
        // Wait for a new value to show up.
        while (nextValue == null) {
          if (completed) {
            return false;
          }
          if (error != null) {
            // We got an error from the gRPC layer.
            throw GrpcClient.convertGrpcError(error);
          }

          wait();
        }

        return true;
      } catch (InterruptedException exc) {
        onError(exc);
        throw new SQLDataException("gRPC StreamIterator interrupted while waiting for value", exc);
      }
    }
  }

  @Override
  public E next() throws NoSuchElementException, SQLException {
    synchronized (this) {
      if (hasNext()) {
        E value = nextValue;
        nextValue = null;
        notifyAll();
        return value;
      } else {
        throw new NoSuchElementException("stream completed");
      }
    }
  }

  @Override
  public void close() throws Exception {
    synchronized (this) {
      closed = true;
    }
  }
}
