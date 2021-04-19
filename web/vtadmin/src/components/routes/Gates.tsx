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

import { useGates } from '../../hooks/api';
import { useDocumentTitle } from '../../hooks/useDocumentTitle';
import { useSyncedURLParam } from '../../hooks/useSyncedURLParam';
import { filterNouns } from '../../util/filterNouns';
import { Button } from '../Button';
import { DataCell } from '../dataTable/DataCell';
import { DataTable } from '../dataTable/DataTable';
import { Icons } from '../Icon';
import { TextInput } from '../TextInput';
import style from './Gates.module.scss';

export const Gates = () => {
    useDocumentTitle('Gates');

    const { data } = useGates();
    const { value: filter, updateValue: updateFilter } = useSyncedURLParam('filter');

    const rows = React.useMemo(() => {
        const mapped = (data || []).map((g) => ({
            cell: g.cell,
            cluster: g.cluster?.name,
            hostname: g.hostname,
            keyspaces: g.keyspaces,
            pool: g.pool,
        }));
        const filtered = filterNouns(filter, mapped);
        return orderBy(filtered, ['cluster', 'pool', 'hostname', 'cell']);
    }, [data, filter]);

    const renderRows = (gates: typeof rows) =>
        gates.map((gate, idx) => (
            <tr key={idx}>
                <DataCell className="white-space-nowrap">
                    <div>{gate.pool}</div>
                    <div className="font-size-small text-color-secondary">{gate.cluster}</div>
                </DataCell>
                <DataCell className="white-space-nowrap">{gate.hostname}</DataCell>
                <DataCell className="white-space-nowrap">{gate.cell}</DataCell>
                <DataCell>{(gate.keyspaces || []).join(', ')}</DataCell>
            </tr>
        ));

    return (
        <div className="max-width-content">
            <h1>Gates</h1>
            <div className={style.controls}>
                <TextInput
                    autoFocus
                    iconLeft={Icons.search}
                    onChange={(e) => updateFilter(e.target.value)}
                    placeholder="Filter gates"
                    value={filter || ''}
                />
                <Button disabled={!filter} onClick={() => updateFilter('')} secondary>
                    Clear filters
                </Button>
            </div>
            <DataTable columns={['Pool', 'Hostname', 'Cell', 'Keyspaces']} data={rows} renderRows={renderRows} />
        </div>
    );
};
