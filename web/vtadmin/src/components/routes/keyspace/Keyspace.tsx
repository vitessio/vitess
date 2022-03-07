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
import { Switch, useLocation, useParams, useRouteMatch } from 'react-router';
import { Link, Redirect, Route } from 'react-router-dom';

import { useKeyspaces } from '../../../hooks/api';
import { useDocumentTitle } from '../../../hooks/useDocumentTitle';
import { NavCrumbs } from '../../layout/NavCrumbs';
import { WorkspaceHeader } from '../../layout/WorkspaceHeader';
import { WorkspaceTitle } from '../../layout/WorkspaceTitle';
import style from './Keyspace.module.scss';
import { KeyspaceShards } from './KeyspaceShards';

interface RouteParams {
    clusterID: string;
    name: string;
}

export const Keyspace = () => {
    const { clusterID, name } = useParams<RouteParams>();
    const { path } = useRouteMatch();
    const { search } = useLocation();

    useDocumentTitle(`${name} (${clusterID})`);

    // TODO(doeg): add a vtadmin-api endpoint to fetch a single keyspace
    // See https://github.com/vitessio/vitess/projects/12#card-59980087
    const { data: keyspaces = [], ...kq } = useKeyspaces();
    const keyspace = keyspaces.find((k) => k.cluster?.id === clusterID && k.keyspace?.name === name);

    if (kq.error) {
        return (
            <div className={style.placeholder}>
                <span className={style.errorEmoji}>😰</span>
                <h1>An error occurred</h1>
                <code>{(kq.error as any).response?.error?.message || kq.error?.message}</code>
                <p>
                    <Link to="/keyspaces">← All keyspaces</Link>
                </p>
            </div>
        );
    }

    if (!kq.isLoading && !keyspace) {
        return (
            <div className={style.placeholder}>
                <span className={style.errorEmoji}>😖</span>
                <h1>Keyspace not found</h1>
                <p>
                    <Link to="/keyspaces">← All keyspaces</Link>
                </p>
            </div>
        );
    }

    return (
        <div>
            <WorkspaceHeader>
                <NavCrumbs>
                    <Link to="/keyspaces">Keyspaces</Link>
                </NavCrumbs>

                <WorkspaceTitle className="font-family-monospace">{name}</WorkspaceTitle>

                <div className={style.headingMeta}>
                    <span>
                        Cluster: <code>{clusterID}</code>
                    </span>
                </div>
            </WorkspaceHeader>

            {/* TODO skeleton placeholder */}
            {!!kq.isLoading && <div className={style.placeholder}>Loading</div>}

            <Switch>
                <Route path={`${path}/shards`}>
                    <KeyspaceShards keyspace={keyspace} />
                </Route>

                <Redirect exact from={path} to={{ pathname: `${path}/shards`, search }} />
            </Switch>
        </div>
    );
};
