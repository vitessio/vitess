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
import { DataCell } from '../dataTable/DataCell';
import { DataFilter } from '../dataTable/DataFilter';
import { DataTable } from '../dataTable/DataTable';
import { ContentContainer } from '../layout/ContentContainer';
import { WorkspaceHeader } from '../layout/WorkspaceHeader';
import { WorkspaceTitle } from '../layout/WorkspaceTitle';
import { QueryLoadingPlaceholder } from '../placeholders/QueryLoadingPlaceholder';

export const Gates = () => {
    useDocumentTitle('Gates');

    const gatesQuery = useGates();
    const { value: filter, updateValue: updateFilter } = useSyncedURLParam('filter');

    const rows = React.useMemo(() => {
        const mapped = (gatesQuery.data || []).map((g) => ({
            cell: g.cell,
            cluster: g.cluster?.name,
            hostname: g.hostname,
            keyspaces: g.keyspaces,
            pool: g.pool,
            fqdn: g.FQDN,
        }));
        const filtered = filterNouns(filter, mapped);
        return orderBy(filtered, ['cluster', 'pool', 'hostname', 'cell']);
    }, [gatesQuery.data, filter]);

    const renderRows = (gates: typeof rows) =>
        gates.map((gate, idx) => (
            <tr key={idx}>
                <DataCell className="whitespace-nowrap">
                    <div>{gate.pool}</div>
                    <div className="text-sm text-secondary">{gate.cluster}</div>
                </DataCell>
                <DataCell className="whitespace-nowrap">
                    {gate.fqdn ? (
                        <div className="font-bold">
                            <a href={`//${gate.fqdn}`} rel="noopener noreferrer" target="_blank">
                                {gate.hostname}
                            </a>
                        </div>
                    ) : (
                        gate.hostname
                    )}
                </DataCell>
                <DataCell className="whitespace-nowrap">{gate.cell}</DataCell>
                <DataCell>{(gate.keyspaces || []).join(', ')}</DataCell>
            </tr>
        ));

    return (
        <div>
            <WorkspaceHeader>
                <WorkspaceTitle>Gates</WorkspaceTitle>
            </WorkspaceHeader>
            <ContentContainer>
                <DataFilter
                    autoFocus
                    onChange={(e) => updateFilter(e.target.value)}
                    onClear={() => updateFilter('')}
                    placeholder="Filter gates"
                    value={filter || ''}
                />
                <DataTable columns={['Pool', 'Hostname', 'Cell', 'Keyspaces']} data={rows} renderRows={renderRows} />
                <QueryLoadingPlaceholder query={gatesQuery} />
            </ContentContainer>
        </div>
    );
};
