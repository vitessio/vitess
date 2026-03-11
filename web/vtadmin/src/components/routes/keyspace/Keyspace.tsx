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
import { Link, Navigate, Route, Routes, useLocation, useParams } from 'react-router-dom';

import { useKeyspace } from '../../../hooks/api';
import { useDocumentTitle } from '../../../hooks/useDocumentTitle';
import { isReadOnlyMode } from '../../../util/env';
import { ContentContainer } from '../../layout/ContentContainer';
import { NavCrumbs } from '../../layout/NavCrumbs';
import { WorkspaceHeader } from '../../layout/WorkspaceHeader';
import { WorkspaceTitle } from '../../layout/WorkspaceTitle';
import { QueryLoadingPlaceholder } from '../../placeholders/QueryLoadingPlaceholder';
import { ReadOnlyGate } from '../../ReadOnlyGate';
import { Tab } from '../../tabs/Tab';
import { TabContainer } from '../../tabs/TabContainer';
import { Advanced } from './Advanced';
import style from './Keyspace.module.scss';
import { KeyspaceShards } from './KeyspaceShards';
import { KeyspaceVSchema } from './KeyspaceVSchema';
import JSONViewTree from '../../jsonViewTree/JSONViewTree';
import { Code } from '../../Code';

interface RouteParams {
    clusterID: string;
    name: string;
}

export const Keyspace = () => {
    const { clusterID, name } = useParams<RouteParams>();
    const { search } = useLocation();

    useDocumentTitle(`${name} (${clusterID})`);

    const kq = useKeyspace({ clusterID, name });
    const { data: keyspace } = kq;

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

                <WorkspaceTitle className="font-mono">{name}</WorkspaceTitle>

                <div className={style.headingMeta}>
                    <span>
                        Cluster: <code>{clusterID}</code>
                    </span>
                </div>
            </WorkspaceHeader>

            <ContentContainer>
                <TabContainer>
                    <Tab text="Shards" to="shards" />
                    <Tab text="VSchema" to="vschema" />
                    <Tab text="JSON" to="json" />
                    <Tab text="JSON Tree" to="json_tree" />

                    <ReadOnlyGate>
                        <Tab text="Advanced" to="advanced" />
                    </ReadOnlyGate>
                </TabContainer>

                <Routes>
                    <Route path="shards" element={<KeyspaceShards keyspace={keyspace} />} />

                    <Route path="vschema" element={<KeyspaceVSchema clusterID={clusterID} name={name} />} />

                    <Route
                        path="json"
                        element={
                            <>
                                <QueryLoadingPlaceholder query={kq} />
                                <Code code={JSON.stringify(keyspace, null, 2)} />
                            </>
                        }
                    />

                    <Route
                        path="json_tree"
                        element={
                            <>
                                <QueryLoadingPlaceholder query={kq} />
                                <JSONViewTree data={keyspace} />
                            </>
                        }
                    />

                    {!isReadOnlyMode() && (
                        <Route path="advanced" element={<Advanced clusterID={clusterID} name={name} />} />
                    )}

                    <Route index element={<Navigate to={{ pathname: 'shards', search }} replace />} />
                </Routes>
            </ContentContainer>
        </div>
    );
};
