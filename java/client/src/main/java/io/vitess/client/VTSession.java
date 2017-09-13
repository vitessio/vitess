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

import io.vitess.proto.Query;
import io.vitess.proto.Vtgate;

/**
 * A persistence session state for each connection.
 *
 */
public class VTSession {
    private Vtgate.Session session;

    /**
     * Create session cookie.
     *
     * @param target    In the format keyspace@shard:tabletType. Only provide the part what needs to be set.
     * @param options   Additional parameters to be passed along the query to the underlying database engine.
     */
    public VTSession(String target, Query.ExecuteOptions options) {
        this.session = Vtgate.Session.newBuilder()
                .setTargetString(null == target ? "" : target)
                .setOptions(null == options ? Query.ExecuteOptions.newBuilder().build() : options)
                .setAutocommit(true)
                .setInTransaction(false)
                .build();
    }

    /**
     * Returns the persistent session cookie.
     *
     * @return Session
     */
    public Vtgate.Session getSession() {
        return this.session;
    }

    /**
     * This method set the session cookie returned from VTGate.
     * <p>
     * <p>This method is not synchronized as the callee function is synchronized.</p>
     *
     * @param session Updated globalSession to be set.
     */
    public void setSession(Vtgate.Session session) {
        this.session = session;
    }

    /**
     * Returns the current state of commit mode.
     *
     * @return autocommit state
     */
    public boolean isAutoCommit() {
        return this.session.getAutocommit();
    }

    /**
     * Set the auto commit state.
     *
     * @param autoCommit true or false
     */
    public void setAutoCommit(boolean autoCommit) {
        this.session = this.session.toBuilder().setAutocommit(autoCommit).build();
    }

    /**
     * Returns whether session is maintaining any transaction or not.
     *
     * @return true or false based on if session cookie is maintaining any transaction.
     */
    public boolean isInTransaction() {
        return this.session.getShardSessionsCount() > 0;
    }

}
