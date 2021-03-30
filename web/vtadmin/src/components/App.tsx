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
import { BrowserRouter as Router, Redirect, Route, Switch } from 'react-router-dom';

import style from './App.module.scss';
import { Tablets } from './routes/Tablets';
import { Debug } from './routes/Debug';
import { NavRail } from './NavRail';
import { Error404 } from './routes/Error404';
import { Clusters } from './routes/Clusters';
import { Gates } from './routes/Gates';
import { Keyspaces } from './routes/Keyspaces';
import { Schemas } from './routes/Schemas';
import { Schema } from './routes/Schema';
import { Workflows } from './routes/Workflows';

export const App = () => {
    return (
        <Router>
            <div className={style.container}>
                <div className={style.navContainer}>
                    <NavRail />
                </div>

                <div className={style.mainContainer}>
                    <Switch>
                        <Route path="/clusters">
                            <Clusters />
                        </Route>

                        <Route path="/gates">
                            <Gates />
                        </Route>

                        <Route path="/keyspaces">
                            <Keyspaces />
                        </Route>

                        <Route path="/schemas">
                            <Schemas />
                        </Route>

                        <Route path="/schema/:clusterID/:keyspace/:table">
                            <Schema />
                        </Route>

                        <Route path="/tablets">
                            <Tablets />
                        </Route>

                        <Route path="/workflows">
                            <Workflows />
                        </Route>

                        <Route path="/debug">
                            <Debug />
                        </Route>

                        <Redirect exact from="/" to="/tablets" />

                        <Route>
                            <Error404 />
                        </Route>
                    </Switch>
                </div>
            </div>
        </Router>
    );
};
