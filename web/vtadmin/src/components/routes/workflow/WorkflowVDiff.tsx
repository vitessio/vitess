/**
 * Copyright 2024 The Vitess Authors.
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

import React, { useMemo, useState } from 'react';

import { useCreateVDiff, useShowVDiff } from '../../../hooks/api';
import { DataTable } from '../../dataTable/DataTable';
import { DataCell } from '../../dataTable/DataCell';
import Dialog from '../../dialog/Dialog';
import { Icon, Icons } from '../../Icon';

interface Props {
    clusterID: string;
    keyspace: string;
    name: string;
}

const COLUMNS = ['Shard', 'State', 'Has Mismatch', 'Rows Compared', 'Started At', 'Completed At', 'Progress'];

export const WorkflowVDiff = ({ clusterID, keyspace, name }: Props) => {
    const [dialogOpen, setDialogOpen] = useState(false);

    // Status for the last VDiff
    const { data: lastVDiffStatus, ...showVDiffQuery } = useShowVDiff({
        clusterID,
        request: {
            target_keyspace: keyspace,
            workflow: name,
            arg: 'last',
        },
    });

    const createVDiffMutation = useCreateVDiff(
        {
            clusterID: clusterID,
            request: {
                target_keyspace: keyspace,
                workflow: name,
            },
        },
        {
            onSuccess: () => {
                showVDiffQuery.refetch();
            },
        }
    );

    const handleCreateVDiff = () => {
        createVDiffMutation.mutate();
    };

    let hasMutationRun = !!createVDiffMutation.data || !!createVDiffMutation.error;

    const closeDialog = () => {
        setDialogOpen(false);
        setTimeout(createVDiffMutation.reset, 500);
    };

    const shardReports = useMemo(() => {
        if (!lastVDiffStatus) {
            return [];
        }
        return Object.keys(lastVDiffStatus.shard_report).map((shard) => {
            return {
                shard,
                ...lastVDiffStatus.shard_report[shard],
            };
        });
    }, [lastVDiffStatus]);

    const isStatusEmpty =
        !lastVDiffStatus ||
        (Object.keys(lastVDiffStatus.shard_report).length > 0 &&
            !lastVDiffStatus.shard_report[Object.keys(lastVDiffStatus.shard_report)[0]].state);

    const renderRows = (rows: typeof shardReports) => {
        return rows.map((row) => {
            let hasMismatch = 'False';
            if (row.has_mismatch) {
                hasMismatch = 'True';
            }
            const progress =
                row.progress && row.progress.eta ? `ETA: ${row.progress.eta} (${row.progress.percentage}%)` : '-';
            return (
                <tr key={row.shard}>
                    <DataCell>{row.shard}</DataCell>
                    <DataCell>{row.state ? row.state.toUpperCase() : '-'}</DataCell>
                    <DataCell>{hasMismatch}</DataCell>
                    <DataCell>{row.rows_compared ? `${row.rows_compared}` : '-'}</DataCell>
                    <DataCell>{row.started_at ? row.started_at : '-'}</DataCell>
                    <DataCell>{row.completed_at ? row.completed_at : '-'}</DataCell>
                    <DataCell>{progress}</DataCell>
                </tr>
            );
        });
    };

    return (
        <div className="mt-8 mb-16">
            <div className="w-full flex flex-row flex-wrap justify-between items-center">
                <h3>Last VDiff Status</h3>
                <div>
                    <button onClick={() => setDialogOpen(true)} className="btn btn-secondary btn-md">
                        Create a New VDiff
                    </button>
                </div>
            </div>
            {!isStatusEmpty ? (
                <DataTable
                    columns={COLUMNS}
                    data={shardReports}
                    renderRows={renderRows}
                    pageSize={10}
                    title="Status By Shard"
                />
            ) : (
                <div className="mt-4">No VDiff status to show.</div>
            )}
            <Dialog
                isOpen={dialogOpen}
                confirmText={hasMutationRun ? 'Close' : 'Create'}
                cancelText="Cancel"
                onConfirm={hasMutationRun ? closeDialog : handleCreateVDiff}
                loadingText={'Creating'}
                loading={createVDiffMutation.isLoading}
                onCancel={closeDialog}
                onClose={closeDialog}
                hideCancel={hasMutationRun}
                title={hasMutationRun ? undefined : 'Create VDiff'}
                description={hasMutationRun ? undefined : `Create a New VDiff for ${name}.`}
            >
                <div className="w-full">
                    {createVDiffMutation.data && !createVDiffMutation.error && (
                        <div className="w-full flex flex-col justify-center items-center">
                            <span className="flex h-12 w-12 relative items-center justify-center">
                                <Icon className="fill-current text-green-500" icon={Icons.checkSuccess} />
                            </span>
                            <div className="text-lg mt-3 font-bold text-center">Created VDiff</div>
                            Successfully created VDiff: {createVDiffMutation.data.UUID}
                        </div>
                    )}
                    {createVDiffMutation.error && (
                        <div className="w-full flex flex-col justify-center items-center">
                            <span className="flex h-12 w-12 relative items-center justify-center">
                                <Icon className="fill-current text-red-500" icon={Icons.alertFail} />
                            </span>
                            <div className="text-lg mt-3 font-bold text-center">Error creating VDiff</div>
                            {createVDiffMutation.error.message}
                        </div>
                    )}
                </div>
            </Dialog>
        </div>
    );
};
