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

import { Redirect, Route, Switch, useParams } from 'react-router';
import { Link, useRouteMatch } from 'react-router-dom';

import style from './Shard.module.scss';
import { NavCrumbs } from '../../layout/NavCrumbs';
import { WorkspaceHeader } from '../../layout/WorkspaceHeader';
import { WorkspaceTitle } from '../../layout/WorkspaceTitle';
import { ContentContainer } from '../../layout/ContentContainer';
import { Tab } from '../../tabs/Tab';
import { TabContainer } from '../../tabs/TabContainer';
import { Code } from '../../Code';
import { useDocumentTitle } from '../../../hooks/useDocumentTitle';
import { KeyspaceLink } from '../../links/KeyspaceLink';
import { useKeyspace } from '../../../hooks/api';
import { ShardTablets } from './ShardTablets';
import Advanced from './Advanced';

interface RouteParams {
    clusterID: string;
    keyspace: string;
    shard: string;
}

export const Shard = () => {
    const params = useParams<RouteParams>();
    const { path, url } = useRouteMatch();

    const shardName = `${params.keyspace}/${params.shard}`;

    useDocumentTitle(`${shardName} (${params.clusterID})`);

    const { data: keyspace, ...kq } = useKeyspace({ clusterID: params.clusterID, name: params.keyspace });

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

    let shard = null;
    if (keyspace?.shards && params.shard in keyspace.shards) {
        shard = keyspace.shards[params.shard];
    }

    if (!kq.isLoading && !shard) {
        return (
            <div className={style.placeholder}>
                <span className={style.errorEmoji}>😖</span>
                <h1>Shard not found</h1>
                <p>
                    <KeyspaceLink clusterID={params.clusterID} name={params.keyspace}>
                        ← All shards in {params.keyspace}
                    </KeyspaceLink>
                </p>
            </div>
        );
    }

    return (
        <div>
            <WorkspaceHeader>
                <NavCrumbs>
                    <Link to="/keyspaces">Keyspaces</Link>
                    <KeyspaceLink clusterID={params.clusterID} name={params.keyspace}>
                        {params.keyspace}
                    </KeyspaceLink>
                </NavCrumbs>

                <WorkspaceTitle className="font-mono">{shardName}</WorkspaceTitle>

                <div className={style.headingMeta}>
                    <span>
                        Cluster: <code>{params.clusterID}</code>
                    </span>
                </div>
            </WorkspaceHeader>

            <ContentContainer>
                <TabContainer>
                    <Tab text="Tablets" to={`${url}/tablets`} />
                    <Tab text="JSON" to={`${url}/json`} />
                    <Tab text="Advanced" to={`${url}/advanced`} />
                </TabContainer>

                <Switch>
                    <Route path={`${path}/tablets`}>
                        <ShardTablets {...params} />
                    </Route>

                    <Route path={`${path}/json`}>{shard && <Code code={JSON.stringify(shard, null, 2)} />}</Route>
                    <Route path={`${path}/advanced`}>
                        <Advanced />
                    </Route>
                    <Redirect from={path} to={`${path}/tablets`} />
                </Switch>
            </ContentContainer>

            {/* TODO skeleton placeholder */}
            {!!kq.isLoading && <div className={style.placeholder}>Loading</div>}
        </div>
    );
};
