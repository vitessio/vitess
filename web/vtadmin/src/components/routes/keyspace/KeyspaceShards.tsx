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
import React, { useMemo } from 'react';
import { isEmpty, orderBy } from 'lodash';

import style from './KeyspaceShards.module.scss';
import { topodata, vtadmin as pb } from '../../../proto/vtadmin';
import { useTablets } from '../../../hooks/api';
import { formatAlias, formatType } from '../../../util/tablets';
import { DataTable } from '../../dataTable/DataTable';
import { DataCell } from '../../dataTable/DataCell';
import { ShardServingPip } from '../../pips/ShardServingPip';
import { TabletServingPip } from '../../pips/TabletServingPip';
import { DataFilter } from '../../dataTable/DataFilter';
import { useSyncedURLParam } from '../../../hooks/useSyncedURLParam';
import { filterNouns } from '../../../util/filterNouns';
import { TabletLink } from '../../links/TabletLink';
import { ShardLink } from '../../links/ShardLink';
import { getShardSortRange } from '../../../util/keyspaces';
import { Pip } from '../../pips/Pip';
import { Tooltip } from '../../tooltip/Tooltip';
import { QueryLoadingPlaceholder } from '../../placeholders/QueryLoadingPlaceholder';

interface Props {
    keyspace: pb.Keyspace | null | undefined;
}

const TABLE_COLUMNS = ['Shard', 'Primary Serving?', 'Tablets', 'Primary Tablet'];

export const KeyspaceShards = ({ keyspace }: Props) => {
    const tq = useTablets();
    const { data: tablets = [] } = tq;

    const { value: filter, updateValue: updateFilter } = useSyncedURLParam('shardFilter');

    const data = useMemo(() => {
        if (!keyspace || tq.isLoading) {
            return [];
        }

        const keyspaceTablets = tablets.filter(
            (t) => t.cluster?.id === keyspace.cluster?.id && t.tablet?.keyspace === keyspace.keyspace?.name
        );

        const mapped = Object.values(keyspace?.shards).map((shard) => {
            const sortRange = getShardSortRange(shard.name || '');

            const shardTablets = keyspaceTablets.filter((t) => t.tablet?.shard === shard.name);

            const primaryTablet = shardTablets.find((t) => t.tablet?.type === topodata.TabletType.PRIMARY);

            return {
                keyspace: shard.keyspace,
                isPrimaryServing: shard.shard?.is_primary_serving,
                name: shard.name,
                primaryAlias: formatAlias(primaryTablet?.tablet?.alias),
                primaryHostname: primaryTablet?.tablet?.hostname,
                // "_" prefix excludes the property name from k/v filtering
                _primaryTablet: primaryTablet,
                _sortStart: sortRange.start,
                _sortEnd: sortRange.end,
                _tabletsByType: countTablets(shardTablets),
            };
        });

        const filtered = filterNouns(filter, mapped);

        return orderBy(filtered, ['_sortStart', '_sortEnd']);
    }, [filter, keyspace, tablets, tq.isLoading]);

    const renderRows = React.useCallback(
        (rows: typeof data) => {
            return rows.map((row) => {
                return (
                    <tr key={row.name}>
                        <DataCell>
                            <ShardLink
                                className="font-bold"
                                clusterID={keyspace?.cluster?.id}
                                keyspace={keyspace?.keyspace?.name}
                                shard={row.name}
                            >
                                {row.keyspace}/{row.name}
                            </ShardLink>
                        </DataCell>
                        <DataCell>
                            <ShardServingPip isServing={row.isPrimaryServing} />{' '}
                            {row.isPrimaryServing ? 'SERVING' : 'NOT SERVING'}
                        </DataCell>
                        <DataCell>
                            {!isEmpty(row._tabletsByType) ? (
                                <div className={style.counts}>
                                    {Object.keys(row._tabletsByType)
                                        .sort()
                                        .map((tabletType) => {
                                            const tt = row._tabletsByType[tabletType];
                                            const allSuccess = tt.serving === tt.total;
                                            const tooltip = allSuccess
                                                ? `${tt.serving}/${tt.total} ${tabletType} serving`
                                                : `${tt.total - tt.serving}/${tt.total} ${tabletType} not serving`;

                                            return (
                                                <span key={tabletType}>
                                                    <Tooltip text={tooltip}>
                                                        <span>
                                                            <Pip state={allSuccess ? 'success' : 'danger'} />
                                                        </span>
                                                    </Tooltip>{' '}
                                                    {tt.total} {tabletType}
                                                </span>
                                            );
                                        })}
                                </div>
                            ) : (
                                <span className="text-secondary">No tablets</span>
                            )}
                        </DataCell>
                        <DataCell>
                            {row._primaryTablet ? (
                                <div>
                                    <TabletLink alias={row.primaryAlias} clusterID={keyspace?.cluster?.id}>
                                        <TabletServingPip state={row._primaryTablet.state} /> {row.primaryAlias}
                                    </TabletLink>
                                    <div className="text-sm text-secondary">{row.primaryHostname}</div>
                                </div>
                            ) : (
                                <span className="text-secondary">No primary tablet</span>
                            )}
                        </DataCell>
                    </tr>
                );
            });
        },
        [keyspace?.cluster?.id, keyspace?.keyspace?.name]
    );

    if (!keyspace) {
        return null;
    }

    return (
        <div className={style.container}>
            <QueryLoadingPlaceholder query={tq} />
            <DataFilter
                autoFocus
                onChange={(e) => updateFilter(e.target.value)}
                onClear={() => updateFilter('')}
                placeholder="Filter shards"
                value={filter || ''}
            />

            <DataTable columns={TABLE_COLUMNS} data={data} renderRows={renderRows} />
        </div>
    );
};

interface TabletCounts {
    // tabletType is the stringified/display version of the
    // topodata.TabletType enum.
    [tabletType: string]: {
        // The number of serving tablets for this type.
        serving: number;
        // The total number of tablets for this type.
        total: number;
    };
}

const countTablets = (tablets: pb.Tablet[]): TabletCounts => {
    return tablets.reduce((acc, t) => {
        // If t.tablet.type is truly an undefined/null/otherwise invalid
        // value (i.e,. not in the proto, which should in theory never happen),
        // then call that "UNDEFINED" so as not to co-opt the existing "UNKNOWN"
        // tablet state.
        const ft = formatType(t) || 'UNDEFINED';
        if (!(ft in acc)) acc[ft] = { serving: 0, total: 0 };

        acc[ft].total++;

        if (t.state === pb.Tablet.ServingState.SERVING) {
            acc[ft].serving++;
        }

        return acc;
    }, {} as TabletCounts);
};
