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

import { useKeyspaces, useTablets } from '../../hooks/api';
import { vtadmin as pb } from '../../proto/vtadmin';
import { orderBy } from 'lodash-es';
import { useDocumentTitle } from '../../hooks/useDocumentTitle';
import { DataTable } from '../dataTable/DataTable';
import { filterNouns } from '../../util/filterNouns';
import { DataCell } from '../dataTable/DataCell';
import { TabletServingPip } from '../pips/TabletServingPip';
import { useSyncedURLParam } from '../../hooks/useSyncedURLParam';
import { formatAlias, formatDisplayType, formatState, formatType } from '../../util/tablets';
import { ShardServingPip } from '../pips/ShardServingPip';
import { ContentContainer } from '../layout/ContentContainer';
import { WorkspaceHeader } from '../layout/WorkspaceHeader';
import { WorkspaceTitle } from '../layout/WorkspaceTitle';
import { DataFilter } from '../dataTable/DataFilter';
import { KeyspaceLink } from '../links/KeyspaceLink';
import { TabletLink } from '../links/TabletLink';
import { ExternalTabletLink } from '../links/ExternalTabletLink';

export const Tablets = () => {
    useDocumentTitle('Tablets');

    const { value: filter, updateValue: updateFilter } = useSyncedURLParam('filter');
    const { data = [] } = useTablets();
    const { data: keyspaces = [], ...ksQuery } = useKeyspaces();

    const filteredData = React.useMemo(() => {
        return formatRows(data, keyspaces, filter);
    }, [data, filter, keyspaces]);

    const renderRows = React.useCallback(
        (rows: typeof filteredData) => {
            return rows.map((t, tdx) => (
                <tr key={tdx}>
                    <DataCell>
                        <KeyspaceLink clusterID={t._raw.cluster?.id} name={t.keyspace}>
                            <div>{t.keyspace}</div>
                            <div className="font-size-small text-color-secondary">{t.cluster}</div>
                        </KeyspaceLink>
                    </DataCell>
                    <DataCell>
                        <KeyspaceLink
                            className="white-space-nowrap"
                            clusterID={t._raw.cluster?.id}
                            name={t.keyspace}
                            shard={t.shard}
                        >
                            <ShardServingPip isLoading={ksQuery.isLoading} isServing={t.isShardServing} /> {t.shard}
                            {ksQuery.isSuccess && (
                                <div className="font-size-small text-color-secondary white-space-nowrap">
                                    {!t.isShardServing && 'NOT SERVING'}
                                </div>
                            )}
                        </KeyspaceLink>
                    </DataCell>
                    <DataCell>
                        <TabletLink alias={t.alias} className="font-weight-bold" clusterID={t._raw.cluster?.id}>
                            {t.alias}
                        </TabletLink>
                    </DataCell>
                    <DataCell className="white-space-nowrap">{t.type}</DataCell>

                    <DataCell>
                        <TabletServingPip state={t._raw.state} /> {t.state}
                    </DataCell>

                    <DataCell>
                        <ExternalTabletLink fqdn={`//${t._raw.FQDN}`}>{t.hostname}</ExternalTabletLink>
                    </DataCell>
                </tr>
            ));
        },
        [ksQuery.isLoading, ksQuery.isSuccess]
    );

    return (
        <div>
            <WorkspaceHeader>
                <WorkspaceTitle>Tablets</WorkspaceTitle>
            </WorkspaceHeader>
            <ContentContainer>
                <DataFilter
                    autoFocus
                    onChange={(e) => updateFilter(e.target.value)}
                    onClear={() => updateFilter('')}
                    placeholder="Filter tablets"
                    value={filter || ''}
                />
                <DataTable
                    columns={['Keyspace', 'Shard', 'Alias', 'Type', 'Tablet State', 'Hostname']}
                    data={filteredData}
                    renderRows={renderRows}
                />
            </ContentContainer>
        </div>
    );
};

export const formatRows = (
    tablets: pb.Tablet[] | null | undefined,
    keyspaces: pb.Keyspace[] | null | undefined,
    filter: string | null | undefined
) => {
    if (!tablets) return [];

    // Properties prefixed with "_" are hidden and included for filtering only.
    // They also won't work as keys in key:value searches, e.g., you cannot
    // search for `_keyspaceShard:customers/20-40`, by design, mostly because it's
    // unexpected and a little weird to key on properties that you can't see.
    const mapped = tablets.map((t) => {
        const keyspace = (keyspaces || []).find(
            (k) => k.cluster?.id === t.cluster?.id && k.keyspace?.name === t.tablet?.keyspace
        );

        const shardName = t.tablet?.shard;
        const shard = shardName ? keyspace?.shards[shardName] : null;

        return {
            alias: formatAlias(t.tablet?.alias),
            cluster: t.cluster?.name,
            hostname: t.tablet?.hostname,
            isShardServing: shard?.shard?.is_master_serving,
            keyspace: t.tablet?.keyspace,
            shard: shardName,
            state: formatState(t),
            type: formatDisplayType(t),
            _raw: t,
            _keyspaceShard: `${t.tablet?.keyspace}/${t.tablet?.shard}`,
            // Include the unformatted type so (string) filtering by "master" works
            // even if "primary" is what we display, and what we use for key:value searches.
            _rawType: formatType(t),
            // Always sort primary tablets first, then sort alphabetically by type, etc.
            _typeSortOrder: formatDisplayType(t) === 'PRIMARY' ? 1 : 2,
        };
    });
    const filtered = filterNouns(filter, mapped);
    return orderBy(filtered, ['cluster', 'keyspace', 'shard', '_typeSortOrder', 'type', 'alias']);
};
