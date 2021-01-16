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
import { BrowserRouter as Router, Link, Redirect, Route, Switch } from 'react-router-dom';

import style from './App.module.scss';
import logo from '../img/vitess-icon-color.svg';
import { Tablets } from './routes/Tablets';
import { Debug } from './routes/Debug';

export const App = () => {
    return (
        <Router>
            <div className={style.container}>
                <Link to="/">
                    <img className={style.logo} src={logo} alt="logo" height={40} />
                </Link>

                <Switch>
                    <Route path="/tablets">
                        <Tablets />
                    </Route>

                    <Route path="/debug">
                        <Debug />
                    </Route>

                    <Redirect exact from="/" to="/tablets" />
                </Switch>
            </div>
        </Router>
    );
};
