/**
 * Copyright 2022 The Vitess Authors.
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

import React from 'react';
import { UseMutationResult } from 'react-query';
import { useHistory } from 'react-router-dom';
import { DeleteTabletParams } from '../../../api/http';
import {
    useDeleteTablet,
    useRefreshTabletReplicationSource,
    useSetReadOnly,
    useSetReadWrite,
    useStartReplication,
    useStopReplication,
} from '../../../hooks/api';
import { topodata, vtadmin } from '../../../proto/vtadmin';
import { isPrimary } from '../../../util/tablets';
import ActionPanel from '../../ActionPanel';
import { success, warn } from '../../Snackbar';

interface AdvancedProps {
    alias: string;
    clusterID: string;
    tablet: vtadmin.Tablet | undefined;
}

const Advanced: React.FC<AdvancedProps> = ({ alias, clusterID, tablet }) => {
    const history = useHistory();
    const primary = isPrimary(tablet);

    const deleteParams: DeleteTabletParams = { alias, clusterID };
    if (tablet?.tablet?.type === topodata.TabletType.PRIMARY) {
        deleteParams.allowPrimary = true;
    }

    const deleteTabletMutation = useDeleteTablet(deleteParams, {
        onSuccess: () => {
            success(
                `Initiated deletion for tablet ${alias}. It may take some time for the tablet to disappear from the topology.`
            );
            history.push('/tablets');
        },
        onError: (error) => warn(`There was an error deleting tablet: ${error}`),
    });

    const refreshTabletReplicationSourceMutation = useRefreshTabletReplicationSource(
        { alias, clusterID },
        {
            onSuccess: (result) => {
                success(
                    `Successfully refreshed tablet replication source for tablet ${alias} to replicate from primary ${result.primary}`,
                    { autoClose: 7000 }
                );
            },
            onError: (error) => warn(`There was an error refreshing tablet replication source: ${error}`),
        }
    );

    const setReadOnlyMutation = useSetReadOnly(
        { alias, clusterID },
        {
            onSuccess: () => {
                success(`Successfully set tablet ${alias} to read-only`);
            },
            onError: (error) => warn(`There was an error setting tablet ${alias} to read-only: ${error}`),
        }
    );

    const setReadWriteMutation = useSetReadWrite(
        { alias, clusterID },
        {
            onSuccess: () => {
                success(`Successfully set tablet ${alias} to read-write`);
            },
            onError: (error) => warn(`There was an error setting tablet ${alias} to read-write: ${error}`),
        }
    );

    const startReplicationMutation = useStartReplication(
        { alias, clusterID },
        {
            onSuccess: () => {
                success(`Successfully started replication on tablet ${alias}.`, { autoClose: 7000 });
            },
            onError: (error) => warn(`There was an error starting replication on tablet: ${error}`),
        }
    );

    const stopReplicationMutation = useStopReplication(
        { alias, clusterID },
        {
            onSuccess: () => success(`Successfully stopped replication on tablet ${alias}.`, { autoClose: 7000 }),
            onError: (error) => warn(`There was an error stopping replication on tablet: ${error}`),
        }
    );

    return (
        <div className="pt-4">
            <div className="my-8">
                <h3 className="mb-4">Replication</h3>
                <div>
                    <ActionPanel
                        confirmationValue=""
                        description={
                            <>
                                This will run the underlying database command to start replication on tablet{' '}
                                <span className="font-bold">{alias}</span>. For example, in mysql 8, this will be{' '}
                                <span className="font-mono text-sm p-1 bg-gray-100">start replication</span>.
                            </>
                        }
                        disabled={primary}
                        documentationLink="https://vitess.io/docs/reference/programs/vtctl/tablets/#startreplication"
                        loadedText="Start Replication"
                        loadingText="Starting Replication..."
                        mutation={startReplicationMutation as UseMutationResult}
                        title="Start Replication"
                        warnings={[primary && 'Command StartReplication cannot be run on the primary tablet.']}
                    />

                    <ActionPanel
                        confirmationValue=""
                        description={
                            <>
                                This will run the underlying database command to stop replication on tablet{' '}
                                <span className="font-bold">{alias}</span>. For example, in mysql 8, this will be{' '}
                                <span className="font-mono text-sm p-1 bg-gray-100">stop replication</span>.
                            </>
                        }
                        disabled={primary}
                        documentationLink="https://vitess.io/docs/reference/programs/vtctl/tablets/#stopreplication"
                        loadedText="Stop Replication"
                        loadingText="Stopping Replication..."
                        mutation={stopReplicationMutation as UseMutationResult}
                        title="Stop Replication"
                        warnings={[primary && 'Command StopReplication cannot be run on the primary tablet.']}
                    />
                </div>
            </div>

            <div className="my-8">
                <h3 className="mb-4">Reparent</h3>
                <div>
                    <ActionPanel
                        confirmationValue=""
                        description={
                            <>
                                Refresh replication source for tablet <span className="font-bold">{alias}</span> to the
                                current shard primary tablet. This only works if the current replication position
                                matches the last known reparent action.
                            </>
                        }
                        disabled={primary}
                        documentationLink="https://vitess.io/docs/reference/programs/vtctl/tablets/#reparenttablet"
                        loadedText="Refresh"
                        loadingText="Refreshing..."
                        mutation={refreshTabletReplicationSourceMutation as UseMutationResult}
                        title="Refresh Replication Source"
                        warnings={[primary && 'Command ReparentTablet cannot be run on the primary tablet.']}
                    />
                </div>
            </div>
            <div className="my-8">
                <h3 className="mb-4">Danger</h3>
                <div>
                    {primary && (
                        <>
                            <ActionPanel
                                confirmationValue={alias}
                                danger
                                description={
                                    <>
                                        Set tablet <span className="font-bold">{alias}</span> to read-only.
                                    </>
                                }
                                documentationLink="https://vitess.io/docs/reference/programs/vtctl/tablets/#setreadonly"
                                loadingText="Setting..."
                                loadedText="Set to read-only"
                                mutation={setReadOnlyMutation as UseMutationResult}
                                title="Set Read-Only"
                                warnings={[
                                    `This will disable writing on the primary tablet ${alias}. Use with caution.`,
                                ]}
                            />

                            <ActionPanel
                                confirmationValue={alias}
                                danger
                                description={
                                    <>
                                        Set tablet <span className="font-bold">{alias}</span> to read-write.
                                    </>
                                }
                                documentationLink="https://vitess.io/docs/reference/programs/vtctl/tablets/#setreadwrite"
                                mutation={setReadWriteMutation as UseMutationResult}
                                loadingText="Setting..."
                                loadedText="Set to read-write"
                                title="Set Read-Write"
                                warnings={[
                                    `This will re-enable writing on the primary tablet ${alias}. Use with caution.`,
                                ]}
                            />
                        </>
                    )}
                    <ActionPanel
                        confirmationValue={alias}
                        danger
                        description={
                            <>
                                Delete tablet <span className="font-bold">{alias}</span>. Doing so will remove it from
                                the topology, but vttablet and MySQL won't be touched.
                            </>
                        }
                        documentationLink="https://vitess.io/docs/reference/programs/vtctl/tablets/#deletetablet"
                        loadingText="Deleting..."
                        loadedText="Delete"
                        mutation={deleteTabletMutation as UseMutationResult}
                        title="Delete Tablet"
                        warnings={[
                            primary && (
                                <>
                                    Tablet {alias} is the primary tablet. Flag{' '}
                                    <span className="font-mono bg-red-100 p-1 text-sm">-allow_primary=true</span> will
                                    be applied in order to delete the primary tablet.
                                </>
                            ),
                        ]}
                    />
                </div>
            </div>
        </div>
    );
};

export default Advanced;
