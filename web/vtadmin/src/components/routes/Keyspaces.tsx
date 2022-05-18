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
import { orderBy } from 'lodash-es';
import * as React from 'react';

import { useKeyspaces } from '../../hooks/api';
import { useDocumentTitle } from '../../hooks/useDocumentTitle';
import { useSyncedURLParam } from '../../hooks/useSyncedURLParam';
import { DataCell } from '../dataTable/DataCell';
import { DataTable } from '../dataTable/DataTable';
import { Pip } from '../pips/Pip';
import { filterNouns } from '../../util/filterNouns';
import { getShardsByState } from '../../util/keyspaces';
import { ContentContainer } from '../layout/ContentContainer';
import { WorkspaceHeader } from '../layout/WorkspaceHeader';
import { WorkspaceTitle } from '../layout/WorkspaceTitle';
import { DataFilter } from '../dataTable/DataFilter';
import { KeyspaceLink } from '../links/KeyspaceLink';
import KeyspaceActions from './keyspaces/KeyspaceActions';
import { ReadOnlyGate } from '../ReadOnlyGate';
import { isReadOnlyMode } from '../../util/env';
import { Link } from 'react-router-dom';
import { QueryLoadingPlaceholder } from '../placeholders/QueryLoadingPlaceholder';

export const Keyspaces = () => {
    useDocumentTitle('Keyspaces');
    const { value: filter, updateValue: updateFilter } = useSyncedURLParam('filter');

    const keyspacesQuery = useKeyspaces();

    const ksRows = React.useMemo(() => {
        const mapped = (keyspacesQuery.data || []).map((k) => {
            const shardsByState = getShardsByState(k);

            return {
                clusterID: k.cluster?.id,
                cluster: k.cluster?.name,
                name: k.keyspace?.name,
                servingShards: shardsByState.serving.length,
                nonservingShards: shardsByState.nonserving.length,
            };
        });
        const filtered = filterNouns(filter, mapped);
        return orderBy(filtered, ['cluster', 'name']);
    }, [keyspacesQuery.data, filter]);

    const renderRows = (rows: typeof ksRows) =>
        rows.map((row, idx) => (
            <tr key={idx}>
                <DataCell>
                    <KeyspaceLink clusterID={row.clusterID} name={row.name}>
                        <div className="font-bold">{row.name}</div>
                        <div className="text-sm text-secondary">{row.cluster}</div>
                    </KeyspaceLink>
                </DataCell>
                <DataCell>
                    {!!row.servingShards && (
                        <div>
                            <Pip state="success" /> {row.servingShards} {row.servingShards === 1 ? 'shard' : 'shards'}
                        </div>
                    )}
                    {!!row.nonservingShards && (
                        <div className="font-bold">
                            <Pip state="danger" /> {row.nonservingShards}{' '}
                            {row.nonservingShards === 1 ? 'shard' : 'shards'} not serving
                        </div>
                    )}
                </DataCell>
                <ReadOnlyGate>
                    <DataCell>
                        <KeyspaceActions keyspace={row.name as string} clusterID={row.clusterID as string} />
                    </DataCell>
                </ReadOnlyGate>
            </tr>
        ));

    return (
        <div>
            <WorkspaceHeader>
                <div className="flex items-top justify-between max-w-screen-md">
                    <WorkspaceTitle>Keyspaces</WorkspaceTitle>
                    <ReadOnlyGate>
                        <div>
                            <Link className="btn btn-secondary btn-md" to="/keyspaces/create">
                                Create a Keyspace
                            </Link>
                        </div>
                    </ReadOnlyGate>
                </div>
            </WorkspaceHeader>
            <ContentContainer>
                <DataFilter
                    autoFocus
                    onChange={(e) => updateFilter(e.target.value)}
                    onClear={() => updateFilter('')}
                    placeholder="Filter keyspaces"
                    value={filter || ''}
                />
                <div className="max-w-screen-md">
                    <DataTable
                        columns={isReadOnlyMode() ? ['Keyspace', 'Shards'] : ['Keyspace', 'Shards', 'Actions']}
                        data={ksRows}
                        renderRows={renderRows}
                    />
                    <QueryLoadingPlaceholder query={keyspacesQuery} />
                </div>
            </ContentContainer>
        </div>
    );
};
