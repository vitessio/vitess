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
import { Link } from 'react-router-dom';

import { useTableDefinitions } from '../../hooks/api';
import { useDocumentTitle } from '../../hooks/useDocumentTitle';
import { filterNouns } from '../../util/filterNouns';
import { Button } from '../Button';
import { DataTable } from '../dataTable/DataTable';
import { Icons } from '../Icon';
import { TextInput } from '../TextInput';
import style from './Schemas.module.scss';

export const Schemas = () => {
    useDocumentTitle('Schemas');

    const { data = [] } = useTableDefinitions();
    const [filter, setFilter] = React.useState<string>('');

    const filteredData = React.useMemo(() => {
        const mapped = data.map((d) => ({
            cluster: d.cluster?.name,
            clusterID: d.cluster?.id,
            keyspace: d.keyspace,
            table: d.tableDefinition?.name,
            _raw: d,
        }));

        const filtered = filterNouns(filter, mapped);
        return orderBy(filtered, ['cluster', 'keyspace', 'table']);
    }, [data, filter]);

    const renderRows = (rows: typeof filteredData) =>
        rows.map((row, idx) => {
            const href =
                row.clusterID && row.keyspace && row.table
                    ? `/schema/${row.clusterID}/${row.keyspace}/${row.table}`
                    : null;
            return (
                <tr key={idx}>
                    <td>{row.cluster}</td>
                    <td>{row.keyspace}</td>
                    <td>{href ? <Link to={href}>{row.table}</Link> : row.table}</td>
                </tr>
            );
        });

    return (
        <div className="max-width-content">
            <h1>Schemas</h1>
            <div className={style.controls}>
                <TextInput
                    autoFocus
                    iconLeft={Icons.search}
                    onChange={(e) => setFilter(e.target.value)}
                    placeholder="Filter schemas"
                    value={filter}
                />
                <Button disabled={!filter} onClick={() => setFilter('')} secondary>
                    Clear filters
                </Button>
            </div>

            <DataTable columns={['Cluster', 'Keyspace', 'Table']} data={filteredData} renderRows={renderRows} />
        </div>
    );
};
