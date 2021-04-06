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
import { vtadmin as pb } from '../../proto/vtadmin';
import { DataTable } from '../dataTable/DataTable';

export const Gates = () => {
    useDocumentTitle('Gates');
    const { data } = useGates();

    const rows = React.useMemo(() => {
        return orderBy(data, ['cluster.name', 'hostname']);
    }, [data]);

    const renderRows = (gates: pb.VTGate[]) =>
        gates.map((gate, idx) => (
            <tr key={idx}>
                <td>{gate.cluster?.name}</td>
                <td>{gate.hostname}</td>
            </tr>
        ));

    return (
        <div className="max-width-content">
            <h1>Gates</h1>
            <DataTable columns={['Cluster', 'Hostname']} data={rows} renderRows={renderRows} />
        </div>
    );
};
