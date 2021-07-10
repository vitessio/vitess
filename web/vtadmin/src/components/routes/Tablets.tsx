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

import { useTablets } from '../../hooks/api';
import { vtadmin as pb, topodata } from '../../proto/vtadmin';
import { invertBy, orderBy } from 'lodash-es';
import { useDocumentTitle } from '../../hooks/useDocumentTitle';
import { DataTable } from '../dataTable/DataTable';
import { TextInput } from '../TextInput';
import { Icons } from '../Icon';
import { filterNouns } from '../../util/filterNouns';
import style from './Tablets.module.scss';
import { Button } from '../Button';

export const Tablets = () => {
    useDocumentTitle('Tablets');

    const [filter, setFilter] = React.useState<string>('');
    const { data = [] } = useTablets();

    const filteredData = React.useMemo(() => {
        return formatRows(data, filter);
    }, [data, filter]);

    const renderRows = React.useCallback((rows: typeof filteredData) => {
        return rows.map((t, tdx) => (
            <tr key={tdx}>
                <td>{t.cluster}</td>
                <td>{t.keyspace}</td>
                <td>{t.shard}</td>
                <td>{t.type}</td>
                <td>{t.state}</td>
                <td>{t.alias}</td>
                <td>{t.hostname}</td>
            </tr>
        ));
    }, []);

    return (
        <div className="max-width-content">
            <h1>Tablets</h1>
            <div className={style.controls}>
                <TextInput
                    autoFocus
                    iconLeft={Icons.search}
                    onChange={(e) => setFilter(e.target.value)}
                    placeholder="Filter tablets"
                    value={filter}
                />
                <Button disabled={!filter} onClick={() => setFilter('')} secondary>
                    Clear filters
                </Button>
            </div>
            <DataTable
                columns={['Cluster', 'Keyspace', 'Shard', 'Type', 'State', 'Alias', 'Hostname']}
                data={filteredData}
                renderRows={renderRows}
            />
        </div>
    );
};

const SERVING_STATES = Object.keys(pb.Tablet.ServingState);

// TABLET_TYPES maps numeric tablet types back to human readable strings.
// Note that topodata.TabletType allows duplicate values: specifically,
// both RDONLY (new name) and BATCH (old name) share the same numeric value.
// So, we make the assumption that if there are duplicate keys, we will
// always take the first value.
const TABLET_TYPES = Object.entries(invertBy(topodata.TabletType)).reduce((acc, [k, vs]) => {
    acc[k] = vs[0];
    return acc;
}, {} as { [k: string]: string });

const formatAlias = (t: pb.Tablet) =>
    t.tablet?.alias?.cell && t.tablet?.alias?.uid && `${t.tablet.alias.cell}-${t.tablet.alias.uid}`;

const formatType = (t: pb.Tablet) => {
    return t.tablet?.type && TABLET_TYPES[t.tablet?.type];
};

const formatDisplayType = (t: pb.Tablet) => {
    const tt = formatType(t);
    return tt === 'MASTER' ? 'PRIMARY' : tt;
};

const formatState = (t: pb.Tablet) => t.state && SERVING_STATES[t.state];

export const formatRows = (tablets: pb.Tablet[] | null, filter: string) => {
    if (!tablets) return [];

    // Properties prefixed with "_" are hidden and included for filtering only.
    // They also won't work as keys in key:value searches, e.g., you cannot
    // search for `_keyspaceShard:customers/20-40`, by design, mostly because it's
    // unexpected and a little weird to key on properties that you can't see.
    const mapped = tablets.map((t) => ({
        cluster: t.cluster?.name,
        keyspace: t.tablet?.keyspace,
        shard: t.tablet?.shard,
        alias: formatAlias(t),
        hostname: t.tablet?.hostname,
        type: formatDisplayType(t),
        state: formatState(t),
        _keyspaceShard: `${t.tablet?.keyspace}/${t.tablet?.shard}`,
        // Include the unformatted type so (string) filtering by "master" works
        // even if "primary" is what we display, and what we use for key:value searches.
        _rawType: formatType(t),
        // Always sort primary tablets first, then sort alphabetically by type, etc.
        _typeSortOrder: formatDisplayType(t) === 'PRIMARY' ? 1 : 2,
    }));
    const filtered = filterNouns(filter, mapped);
    return orderBy(filtered, ['cluster', 'keyspace', 'shard', '_typeSortOrder', 'type', 'alias']);
};
