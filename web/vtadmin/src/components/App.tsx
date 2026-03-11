/**
 * Copyright 2021 The Vitess Authors.
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
import * as React from 'react';
import { BrowserRouter as Router, Navigate, Route, Routes } from 'react-router-dom';

import style from './App.module.scss';
import { Tablets } from './routes/Tablets';
import { Settings } from './routes/Settings';
import { NavRail } from './NavRail';
import { Error404 } from './routes/Error404';
import { Clusters } from './routes/Clusters';
import { Gates } from './routes/Gates';
import { Keyspaces } from './routes/Keyspaces';
import { Schemas } from './routes/Schemas';
import { Schema } from './routes/schema/Schema';
import { Stream } from './routes/stream/Stream';
import { Workflows } from './routes/Workflows';
import { Workflow } from './routes/workflow/Workflow';
import { VTExplain } from './routes/VTExplain';
import { VExplain } from './routes/VExplain';
import { Keyspace } from './routes/keyspace/Keyspace';
import { Tablet } from './routes/tablet/Tablet';
import { Backups } from './routes/Backups';
import { Shard } from './routes/shard/Shard';
import { Vtctlds } from './routes/Vtctlds';
import { SnackbarContainer } from './Snackbar';
import { isReadOnlyMode } from '../util/env';
import { CreateKeyspace } from './routes/createKeyspace/CreateKeyspace';
import { Topology } from './routes/topology/Topology';
import { ClusterTopology } from './routes/topology/ClusterTopology';
import { CreateMoveTables } from './routes/createWorkflow/CreateMoveTables';
import { Transactions } from './routes/Transactions';
import { Transaction } from './routes/transaction/Transaction';
import { CreateReshard } from './routes/createWorkflow/CreateReshard';
import { CreateMaterialize } from './routes/createWorkflow/CreateMaterialize';
import { TopologyTree } from './routes/topologyTree/TopologyTree';
import { SchemaMigrations } from './routes/SchemaMigrations';
import { CreateSchemaMigration } from './routes/createSchemaMigration/CreateSchemaMigration';

export const App = () => {
    return (
        <Router>
            <div className={style.container}>
                <div className={style.navContainer}>
                    <NavRail />
                </div>
                <SnackbarContainer />
                <div className={style.mainContainer}>
                    <Routes>
                        <Route path="/backups" element={<Backups />} />

                        <Route path="/clusters" element={<Clusters />} />

                        <Route path="/gates" element={<Gates />} />

                        <Route path="/keyspaces" element={<Keyspaces />} />

                        {!isReadOnlyMode() && <Route path="/keyspaces/create" element={<CreateKeyspace />} />}

                        <Route path="/keyspace/:clusterID/:keyspace/shard/:shard/*" element={<Shard />} />

                        <Route path="/keyspace/:clusterID/:name/*" element={<Keyspace />} />

                        <Route path="/schemas" element={<Schemas />} />

                        <Route path="/schema/:clusterID/:keyspace/:table" element={<Schema />} />

                        <Route path="/tablets" element={<Tablets />} />

                        <Route path="/tablet/:clusterID/:alias/*" element={<Tablet />} />

                        <Route path="/vtctlds" element={<Vtctlds />} />

                        <Route path="/vtexplain" element={<VTExplain />} />

                        <Route path="/vexplain" element={<VExplain />} />

                        <Route path="/workflows" element={<Workflows />} />

                        {!isReadOnlyMode() && (
                            <Route path="/workflows/movetables/create" element={<CreateMoveTables />} />
                        )}

                        {!isReadOnlyMode() && <Route path="/workflows/reshard/create" element={<CreateReshard />} />}

                        {!isReadOnlyMode() && (
                            <Route path="/workflows/materialize/create" element={<CreateMaterialize />} />
                        )}

                        <Route
                            path="/workflow/:clusterID/:keyspace/:workflowName/stream/:tabletCell/:tabletUID/:streamID/*"
                            element={<Stream />}
                        />

                        <Route path="/workflow/:clusterID/:keyspace/:name/*" element={<Workflow />} />

                        <Route path="/migrations" element={<SchemaMigrations />} />

                        {!isReadOnlyMode() && <Route path="/migrations/create" element={<CreateSchemaMigration />} />}

                        <Route path="/transactions" element={<Transactions />} />

                        <Route path="/transaction/:clusterID/:dtid" element={<Transaction />} />

                        <Route path="/topology/:clusterID" element={<ClusterTopology />} />

                        <Route path="/topologytree/:clusterID" element={<TopologyTree />} />

                        <Route path="/topology" element={<Topology />} />

                        <Route path="/settings" element={<Settings />} />

                        <Route path="/" element={<Navigate to="/schemas" replace />} />

                        <Route path="*" element={<Error404 />} />
                    </Routes>
                </div>
            </div>
        </Router>
    );
};
