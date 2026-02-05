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

import { orderBy } from 'lodash';
import { useMemo } from 'react';

import style from './ShardTablets.module.scss';
import { useTablets } from '../../../hooks/api';
import { formatAlias, formatDisplayType, formatState } from '../../../util/tablets';
import { DataCell } from '../../dataTable/DataCell';
import { DataTable } from '../../dataTable/DataTable';
import { ExternalTabletLink } from '../../links/ExternalTabletLink';
import { TabletLink } from '../../links/TabletLink';
import { TabletServingPip } from '../../pips/TabletServingPip';

interface Props {
    clusterID: string;
    keyspace: string;
    shard: string;
}

const COLUMNS = ['Alias', 'Type', 'Tablet State', 'Hostname'];

export const ShardTablets: React.FunctionComponent<Props> = (props) => {
    // TODO(doeg): add more vtadmin-api params to query tablets for only this shard.
    // In practice, this isn't _too_ bad since this query will almost always be cached
    // from the /api/tablets sidebar query.
    const { data: allTablets = [], ...tq } = useTablets();

    const tablets = useMemo(() => {
        const rows = allTablets
            .filter(
                (t) =>
                    t.cluster?.id === props.clusterID &&
                    t.tablet?.keyspace === props.keyspace &&
                    t.tablet?.shard === props.shard
            )
            .map((t) => ({
                alias: formatAlias(t.tablet?.alias),
                clusterID: t.cluster?.id,
                fqdn: t.FQDN,
                hostname: t.tablet?.hostname,
                keyspace: t.tablet?.keyspace,
                state: formatState(t),
                tabletType: formatDisplayType(t),
                // underscore prefix excludes the following properties from filtering
                _state: t.state,
                _typeSortOrder: formatDisplayType(t) === 'PRIMARY' ? 1 : 2,
            }));
        return orderBy(rows, ['_typeSortOrder', 'tabletType', 'state', 'alias']);
    }, [allTablets, props.clusterID, props.keyspace, props.shard]);

    if (!tq.isLoading && !tablets.length) {
        return (
            <div className={style.placeholder}>
                <div className={style.emoji}>🏜</div>
                <div>
                    No tablets in{' '}
                    <span className="font-mono">
                        {props.keyspace}/{props.shard}
                    </span>
                    .
                </div>
            </div>
        );
    }

    const renderRows = (rows: typeof tablets) => {
        return rows.map((t) => {
            return (
                <tr key={t.alias}>
                    <DataCell>
                        <TabletLink alias={t.alias} clusterID={t.clusterID}>
                            {t.alias}
                        </TabletLink>
                    </DataCell>

                    <DataCell>{t.tabletType}</DataCell>

                    <DataCell>
                        <TabletServingPip state={t._state} /> {t.state}
                    </DataCell>

                    <DataCell>
                        <ExternalTabletLink fqdn={`${t.fqdn}`}>{t.hostname}</ExternalTabletLink>
                    </DataCell>
                </tr>
            );
        });
    };

    return <DataTable columns={COLUMNS} data={tablets} renderRows={renderRows} />;
};
