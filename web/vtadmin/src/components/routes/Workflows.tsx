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

import { useWorkflows } from '../../hooks/api';
import { useDocumentTitle } from '../../hooks/useDocumentTitle';
import { DataCell } from '../dataTable/DataCell';
import { DataTable } from '../dataTable/DataTable';

export const Workflows = () => {
    useDocumentTitle('Workflows');
    const { data } = useWorkflows();

    const sortedData = React.useMemo(
        () => orderBy(data, ['workflow.name', 'cluster.name', 'workflow.source.keyspace', 'workflow.target.keyspace']),
        [data]
    );

    const renderRows = (rows: typeof sortedData) =>
        rows.map(({ cluster, keyspace, workflow }, idx) => {
            const href =
                cluster?.id && keyspace && workflow?.name
                    ? `/workflow/${cluster.id}/${keyspace}/${workflow.name}`
                    : null;

            return (
                <tr key={idx}>
                    <DataCell>
                        <div className="font-weight-bold">
                            {href ? <Link to={href}>{workflow?.name}</Link> : workflow?.name}
                        </div>
                        <div className="font-size-small text-color-secondary">{cluster?.name}</div>
                    </DataCell>
                    <DataCell>
                        {workflow?.source?.keyspace || <span className="text-color-secondary">n/a</span>}
                    </DataCell>
                    <DataCell>
                        {workflow?.target?.keyspace || <span className="text-color-secondary">n/a</span>}
                    </DataCell>
                </tr>
            );
        });

    return (
        <div className="max-width-content">
            <h1>Workflows</h1>
            <DataTable columns={['Workflow', 'Source', 'Target']} data={sortedData} renderRows={renderRows} />
        </div>
    );
};
