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
import { groupBy, isEmpty, orderBy } from 'lodash';

import style from './KeyspaceShards.module.scss';
import { topodata, vtadmin as pb } from '../../../proto/vtadmin';
import { useTablets } from '../../../hooks/api';
import { formatAlias, TABLET_TYPES } from '../../../util/tablets';
import { DataTable } from '../../dataTable/DataTable';
import { DataCell } from '../../dataTable/DataCell';
import { ShardServingPip } from '../../pips/ShardServingPip';
import { TabletServingPip } from '../../pips/TabletServingPip';
import { DataFilter } from '../../dataTable/DataFilter';
import { useSyncedURLParam } from '../../../hooks/useSyncedURLParam';
import { filterNouns } from '../../../util/filterNouns';
import { TabletLink } from '../../links/TabletLink';
import { ShardLink } from '../../links/ShardLink';
interface Props {
    keyspace: pb.Keyspace | null | undefined;
}

const TABLE_COLUMNS = ['Shard', 'Primary Serving?', 'Tablets', 'Primary Tablet'];

export const KeyspaceShards = ({ keyspace }: Props) => {
    const { data: tablets = [], ...tq } = useTablets();
    const { value: filter, updateValue: updateFilter } = useSyncedURLParam('shardFilter');

    const data = useMemo(() => {
        if (!keyspace || tq.isLoading) {
            return [];
        }

        const shards = Object.values(keyspace?.shards);

        const keyspaceTablets = tablets.filter(
            (t) => t.cluster?.id === keyspace.cluster?.id && t.tablet?.keyspace === keyspace.keyspace?.name
        );

        const mapped = shards.map((shard) => {
            const shardTablets = keyspaceTablets.filter((t) => t.tablet?.shard === shard.name);
            const tabletsByType = groupBy(shardTablets, (t) => t.tablet?.type);

            return {
                keyspace: shard.keyspace,
                isPrimaryServing: shard.shard?.is_primary_serving,
                name: shard.name,
                tabletsByType,
            };
        });

        const filtered = filterNouns(filter, mapped);

        // TODO use numeric ordering, not alphabetical ordering
        return orderBy(filtered, ['name']);
    }, [filter, keyspace, tablets, tq.isLoading]);

    const renderRows = React.useCallback(
        (rows: typeof data) => {
            return rows.map((row) => {
                // A shard can only ever have one primary tablet, hence we can take the "first"
                const primaryTablet = (row.tabletsByType[topodata.TabletType.PRIMARY] || [])[0];

                return (
                    <tr>
                        <DataCell>
                            <ShardLink
                                clusterID={keyspace?.cluster?.id}
                                keyspace={keyspace?.keyspace?.name}
                                shard={row.name}
                            >
                                <ShardServingPip isServing={row.isPrimaryServing} /> {row.keyspace}/{row.name}
                            </ShardLink>
                        </DataCell>
                        <DataCell>
                            <ShardServingPip isServing={row.isPrimaryServing} />{' '}
                            {row.isPrimaryServing ? 'SERVING' : 'NOT SERVING'}
                        </DataCell>
                        <DataCell>
                            {!isEmpty(row.tabletsByType) ? (
                                <div className={style.counts}>
                                    {Object.keys(row.tabletsByType)
                                        .sort()
                                        .map((tabletType) => (
                                            <span>
                                                {row.tabletsByType[tabletType].length} {TABLET_TYPES[tabletType]}
                                            </span>
                                        ))}
                                </div>
                            ) : (
                                <span className="text-color-secondary">No tablets</span>
                            )}
                        </DataCell>
                        <DataCell>
                            {primaryTablet ? (
                                <TabletLink
                                    alias={formatAlias(primaryTablet.tablet?.alias)}
                                    clusterID={keyspace?.cluster?.id}
                                >
                                    <TabletServingPip state={primaryTablet.state} /> {primaryTablet.tablet?.hostname}
                                </TabletLink>
                            ) : (
                                <span className="text-color-secondary">No primary tablet</span>
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
            <DataFilter
                autoFocus
                onChange={(e) => updateFilter(e.target.value)}
                onClear={() => updateFilter('')}
                placeholder="Filter shards and tablets"
                value={filter || ''}
            />

            <DataTable columns={TABLE_COLUMNS} data={data} renderRows={renderRows} />
        </div>
    );
};
