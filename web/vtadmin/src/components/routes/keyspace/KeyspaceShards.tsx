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
import React from 'react';
import { groupBy, orderBy, uniq } from 'lodash';

import { topodata, vtadmin as pb } from '../../../proto/vtadmin.d';
import { useTablets } from '../../../hooks/api';
import { formatAlias, formatDisplayType, formatState } from '../../../util/tablets';
import { DataTable } from '../../dataTable/DataTable';
import { DataCell } from '../../dataTable/DataCell';
import { ShardServingPip } from '../../pips/ShardServingPip';
import { TabletServingPip } from '../../pips/TabletServingPip';
import { DataFilter } from '../../dataTable/DataFilter';
import { useSyncedURLParam } from '../../../hooks/useSyncedURLParam';
import { filterNouns } from '../../../util/filterNouns';
import { ContentContainer } from '../../layout/ContentContainer';
import { TabletLink } from '../../links/TabletLink';
interface Props {
    keyspace: pb.Keyspace | null | undefined;
}

type ShardState = 'SERVING' | 'NOT_SERVING';

const TABLE_COLUMNS = ['Shard', 'Alias', 'Tablet Type', 'Tablet State', 'Hostname'];
const PAGE_SIZE = 16;

export const KeyspaceShards = ({ keyspace }: Props) => {
    const { data: tablets = [], ...tq } = useTablets();
    const { value: filter, updateValue: updateFilter } = useSyncedURLParam('shardFilter');

    const data = React.useMemo(() => {
        if (!keyspace || tq.isLoading) {
            return [];
        }

        return formatRows(keyspace, tablets, filter);
    }, [filter, keyspace, tablets, tq.isLoading]);

    const renderRows = React.useCallback(
        (rows: typeof data) => {
            return rows.reduce((acc, row) => {
                if (!row.tablets.length) {
                    acc.push(
                        <tr key={row.shard}>
                            <DataCell rowSpan={1}>
                                <ShardServingPip isServing={row.isShardServing} /> {row.shard}
                                {row.shardState === 'NOT_SERVING' && (
                                    <div className="font-size-small text-color-secondary white-space-nowrap">
                                        {row.shardState}
                                    </div>
                                )}
                            </DataCell>
                            <DataCell colSpan={TABLE_COLUMNS.length - 1}>
                                <div className="font-size-small text-color-secondary">
                                    No tablets in {keyspace?.keyspace?.name}/{row.shard}{' '}
                                    {!!filter && 'matching filters'}
                                </div>
                            </DataCell>
                        </tr>
                    );
                }

                row.tablets.forEach((tablet, tdx) => {
                    const rowKey = `${row.shard}-${tablet.alias}`;
                    acc.push(
                        <tr key={rowKey}>
                            {tdx === 0 && (
                                <DataCell rowSpan={row.tablets.length}>
                                    <ShardServingPip isServing={row.isShardServing} /> {row.shard}
                                    {row.shardState === 'NOT_SERVING' && (
                                        <div className="font-size-small text-color-secondary white-space-nowrap">
                                            {row.shardState}
                                        </div>
                                    )}
                                </DataCell>
                            )}
                            <DataCell>
                                <TabletLink alias={tablet.alias} clusterID={keyspace?.cluster?.id}>
                                    {tablet.alias}
                                </TabletLink>
                            </DataCell>
                            <DataCell>{tablet.tabletType}</DataCell>
                            <DataCell>
                                <TabletServingPip state={tablet._tabletStateEnum} /> {tablet.tabletState}
                            </DataCell>
                            <DataCell>{tablet.hostname}</DataCell>
                        </tr>
                    );
                });

                return acc;
            }, [] as JSX.Element[]);
        },
        [filter, keyspace?.cluster?.id, keyspace?.keyspace?.name]
    );

    if (!keyspace) {
        return null;
    }

    return (
        <ContentContainer>
            <DataFilter
                autoFocus
                onChange={(e) => updateFilter(e.target.value)}
                onClear={() => updateFilter('')}
                placeholder="Filter shards and tablets"
                value={filter || ''}
            />

            <DataTable columns={TABLE_COLUMNS} data={data} pageSize={PAGE_SIZE} renderRows={renderRows} />
        </ContentContainer>
    );
};

interface Row {
    isShardServing: boolean;
    shard: string | null | undefined;
    shardState: ShardState;
    tablets: {
        alias: string | null;
        hostname: string | null | undefined;
        shard: string | null | undefined;
        tabletState: string | pb.Tablet.ServingState.UNKNOWN;
        tabletType: string | topodata.TabletType.UNKNOWN | null | undefined;
        _tabletStateEnum: pb.Tablet.ServingState;
    }[];
}

// Filtering data is complicated by how we group rows by their shard,
// since we want the filtering to apply to both shard-level properties,
// ("shard:-80") as well as nested tablet-level properties ("hostname:some-tablet-123").
//
// This gets surprisingly complex: what do you do when fuzzy filtering,
// and you match a shard, and also _some_ tablets in the shard -- do you show all
// of the tablets (since the shard matches), or (more likely) just the filtered ones?
//
// Instead of doing a complicated-and-hacky solution, this approach is a
// fairly straightforward, fairly hacky, and definitely non-generalized approach
// that still feels intuitive to use:
//
//  1. Filtering all the shards in the keyspace by filter string,
//     which includes all of the tablets in that shards.
//
//  2. Filtering all the tablets in the keyspace (across all shards,
//      not just the filtered shards) by filter string.
//
//  3. Taking the intersection of these two sets of shards/tablets.
//
// Investigating a more robust + general filtering implementation
// that supports complex use cases is ticketed here:
// https://github.com/vitessio/vitess/projects/12#card-60968004)
export const formatRows = (
    keyspace: pb.Keyspace | null | undefined,
    tablets: pb.Tablet[],
    filter: string | null | undefined
): Row[] => {
    if (!keyspace) {
        return [];
    }

    // Reduce the set of tablets to just the ones for this keyspace,
    // and map them to simplified objects to allow key/value filtering
    // with `filterNouns`
    const tabletsForKeyspace = orderBy(
        tablets
            .filter((t) => t.cluster?.id === keyspace.cluster?.id && t.tablet?.keyspace === keyspace.keyspace?.name)
            .map((t) => ({
                alias: formatAlias(t.tablet?.alias),
                hostname: t.tablet?.hostname,
                shard: t.tablet?.shard,
                tabletState: formatState(t),
                tabletType: formatDisplayType(t),
                _tabletStateEnum: t.state,
            })),
        ['tabletType', 'alias']
    );

    // Compare tablets against the filter string
    const filteredTablets = filterNouns(filter, tabletsForKeyspace);
    const filteredTabletsByShard = groupBy(filteredTablets, 'shard');

    const shardsForKeyspace = Object.values(keyspace.shards || {}).map((shard) => {
        const isShardServing = !!shard.shard?.is_master_serving;
        return {
            isShardServing,
            shard: shard.name,
            shardState: (isShardServing ? 'SERVING' : 'NOT_SERVING') as ShardState,
        };
    });
    const shardsForKeyspaceByShard = groupBy(shardsForKeyspace, 'shard');

    const filteredShards = filterNouns(filter, shardsForKeyspace);
    const filteredShardsByShard = groupBy(filteredShards, 'shard');

    // Take the intersection of all shardNames across...
    const allFilteredShards = uniq([
        //  1. Shards with tablets matching the filter criteria
        ...Object.keys(filteredShardsByShard),
        //  2. Shards that themselves match the filter criteria
        ...Object.keys(filteredTabletsByShard),
    ]);

    // "Hydrate" the filtered shard names into rows
    const rows = allFilteredShards.reduce((acc, shardName) => {
        const shard = shardsForKeyspaceByShard[shardName][0];
        if (!shard) {
            return acc;
        }

        acc.push({
            ...shard,
            // Make sure we only take the filtered tablets
            tablets: filteredTabletsByShard[shardName] || [],
        });

        return acc;
    }, [] as Row[]);

    return orderBy(rows, 'shard');
};
