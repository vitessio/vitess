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
import { useVtctlds } from '../../hooks/api';
import { useSyncedURLParam } from '../../hooks/useSyncedURLParam';
import { filterNouns } from '../../util/filterNouns';
import { DataCell } from '../dataTable/DataCell';
import { DataFilter } from '../dataTable/DataFilter';
import { DataTable } from '../dataTable/DataTable';
import { ContentContainer } from '../layout/ContentContainer';
import { WorkspaceHeader } from '../layout/WorkspaceHeader';
import { WorkspaceTitle } from '../layout/WorkspaceTitle';
import { QueryLoadingPlaceholder } from '../placeholders/QueryLoadingPlaceholder';

export const Vtctlds = () => {
    const vtctldsQuery = useVtctlds();
    const { data: vtctlds = [] } = vtctldsQuery;

    const { value: filter, updateValue: updateFilter } = useSyncedURLParam('filter');

    const data = useMemo(() => {
        const mapped = vtctlds.map((v) => ({
            cluster: v.cluster?.name,
            clusterID: v.cluster?.id,
            hostname: v.hostname,
            fqdn: v.FQDN,
        }));

        const filtered = filterNouns(filter, mapped);

        return orderBy(filtered, ['cluster', 'hostname']);
    }, [filter, vtctlds]);

    const renderRows = (rows: typeof data) => {
        return rows.map((row) => {
            return (
                <tr key={row.hostname}>
                    <DataCell>
                        <div className="font-bold">
                            {row.fqdn ? (
                                <a href={`//${row.fqdn}`} rel="noopener noreferrer" target="_blank">
                                    {row.hostname}
                                </a>
                            ) : (
                                row.hostname
                            )}
                        </div>
                    </DataCell>
                    <DataCell>
                        {row.cluster}
                        <div className="text-sm text-secondary">{row.clusterID}</div>
                    </DataCell>
                </tr>
            );
        });
    };

    return (
        <div>
            <WorkspaceHeader>
                <WorkspaceTitle>vtctlds</WorkspaceTitle>
            </WorkspaceHeader>

            <ContentContainer>
                <DataFilter
                    autoFocus
                    onChange={(e) => updateFilter(e.target.value)}
                    onClear={() => updateFilter('')}
                    placeholder="Filter vtctlds"
                    value={filter || ''}
                />
                <DataTable columns={['Hostname', 'Cluster']} data={data} renderRows={renderRows} />
                <QueryLoadingPlaceholder query={vtctldsQuery} />
            </ContentContainer>
        </div>
    );
};
