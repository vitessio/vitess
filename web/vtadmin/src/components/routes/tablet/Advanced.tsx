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
    useReparentTablet,
    useSetReadOnly,
    useSetReadWrite,
    useStartReplication,
    useStopReplication,
} from '../../../hooks/api';
import { topodata, vtadmin } from '../../../proto/vtadmin';
import { isPrimary } from '../../../util/tablets';
import DangerAction from '../../DangerAction';
import { Icon, Icons } from '../../Icon';
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

    const reparentTabletMutation = useReparentTablet(
        { alias, clusterID },
        {
            onSuccess: (result) => {
                success(`Successfully reparented tablet ${alias} under primary ${result.primary}`, { autoClose: 7000 });
            },
            onError: (error) => warn(`There was an error reparenting tablet: ${error}`),
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
                <div className="w-full border rounded-lg border-gray-400">
                    <div className="p-8 border-b border-gray-400">
                        <div className="flex justify-between items-center">
                            <p className="text-base font-bold m-0 text-gray-900">Start Replication</p>
                            <a
                                href="https://vitess.io/docs/reference/programs/vtctl/tablets/#startreplication"
                                target="_blank"
                                rel="noreferrer"
                                className="text-gray-900 ml-1"
                            >
                                <span className="text-sm font-semibold text-gray-900">Documentation</span>
                                <Icon
                                    icon={Icons.open}
                                    className="h-6 w-6 ml-1 inline-block text-gray-900 fill-current"
                                />
                            </a>
                        </div>
                        <p className="text-base m-0">
                            This will run the underlying database command to start replication on tablet{' '}
                            <span className="font-bold">{alias}</span>. For example, in mysql 8, this will be{' '}
                            <span className="font-mono text-sm p-1 bg-gray-100">start replication</span>.
                        </p>
                        {primary && (
                            <p className="text-danger">
                                <Icon icon={Icons.alertFail} className="fill-current text-danger inline mr-2" />
                                Command StartTablet cannot be run on the primary tablet.
                            </p>
                        )}
                        <button
                            onClick={() => startReplicationMutation.mutate()}
                            className="btn btn-secondary mt-4"
                            disabled={primary || startReplicationMutation.isLoading}
                        >
                            Start replication
                        </button>
                    </div>
                    <div className="p-8">
                        <div className="flex justify-between items-center">
                            <p className="text-base font-bold m-0 text-gray-900">Stop Replication</p>
                            <a
                                href="https://vitess.io/docs/reference/programs/vtctl/tablets/#stopreplication"
                                target="_blank"
                                rel="noreferrer"
                                className="text-gray-900"
                            >
                                <span className="text-sm font-semibold text-gray-900">Documentation</span>{' '}
                                <Icon
                                    icon={Icons.open}
                                    className="ml-1 inline-block h-6 w-6 text-gray-900 fill-current"
                                />
                            </a>
                        </div>
                        <p className="text-base m-0">
                            This will run the underlying database command to stop replication on tablet{' '}
                            <span className="font-bold">{alias}</span>. For example, in mysql 8, this will be{' '}
                            <span className="font-mono text-sm p-1 bg-gray-100">stop replication</span>.
                        </p>
                        {primary && (
                            <p className="text-danger">
                                <Icon icon={Icons.alertFail} className="fill-current text-danger inline mr-2" />
                                Command StopTablet cannot be run on the primary tablet.
                            </p>
                        )}
                        <button
                            onClick={() => stopReplicationMutation.mutate()}
                            className="btn btn-secondary mt-4"
                            disabled={primary || stopReplicationMutation.isLoading}
                        >
                            Stop replication
                        </button>
                    </div>
                </div>
            </div>
            <div className="my-8">
                <h3 className="mb-4">Reparent</h3>
                <div className="w-full border rounded-lg border-gray-400">
                    <div className="p-8 border-b border-gray-400">
                        <div className="flex justify-between items-center">
                            <p className="text-base font-bold m-0 text-gray-900">Reparent Tablet</p>
                            <a
                                href="https://vitess.io/docs/reference/programs/vtctl/tablets/#reparenttablet"
                                target="_blank"
                                rel="noreferrer"
                                className="text-gray-900 ml-1"
                            >
                                <span className="text-sm font-semibold text-gray-900">Documentation</span>
                                <Icon
                                    icon={Icons.open}
                                    className="ml-1 h-6 w-6 inline-block text-gray-900 fill-current"
                                />
                            </a>
                        </div>
                        <p className="text-base m-0">
                            Reconnect replication for tablet <span className="font-bold">{alias}</span> to the current
                            primary tablet. This only works if the current replication position matches the last known
                            reparent action.
                        </p>
                        {primary && (
                            <p className="text-danger">
                                <Icon icon={Icons.alertFail} className="fill-current text-danger inline mr-2" />
                                Command ReparentTablet cannot be run on the primary tablet.
                            </p>
                        )}
                        <button
                            className="btn btn-secondary mt-4"
                            disabled={primary || reparentTabletMutation.isLoading}
                            onClick={() => reparentTabletMutation.mutate()}
                        >
                            Reparent tablet
                        </button>
                    </div>
                </div>
            </div>
            <div className="my-8">
                <h3 className="mb-4">Danger</h3>
                <div>
                    {primary && (
                        <>
                            <DangerAction
                                confirmationValue={alias}
                                title="Set Read-Only"
                                documentationLink="https://vitess.io/docs/reference/programs/vtctl/tablets/#setreadonly"
                                warnings={[
                                    `This will disable writing on the primary tablet ${alias}. Use with caution.`,
                                ]}
                                description={
                                    <>
                                        Set tablet <span className="font-bold">{alias}</span> to read-only.
                                    </>
                                }
                                mutation={setReadOnlyMutation as UseMutationResult}
                                loadingText="Setting..."
                                loadedText="Set to read-only"
                            />

                            <DangerAction
                                confirmationValue={alias}
                                title="Set Read-Write"
                                documentationLink="https://vitess.io/docs/reference/programs/vtctl/tablets/#setreadwrite"
                                warnings={[
                                    `This will re-enable writing on the primary tablet ${alias}. Use with caution.`,
                                ]}
                                description={
                                    <>
                                        Set tablet <span className="font-bold">{alias}</span> to read-write.
                                    </>
                                }
                                mutation={setReadWriteMutation as UseMutationResult}
                                loadingText="Setting..."
                                loadedText="Set to read-write"
                            />
                        </>
                    )}
                    <DangerAction
                        confirmationValue={alias}
                        title="Delete Tablet"
                        documentationLink="https://vitess.io/docs/reference/programs/vtctl/tablets/#deletetablet"
                        warnings={[
                            primary && (
                                <>
                                    Tablet {alias} is the primary tablet. Flag{' '}
                                    <span className="font-mono bg-red-100 p-1 text-sm">-allow_primary=true</span> will
                                    be applied in order to delete the primary tablet.
                                </>
                            ),
                        ]}
                        description={
                            <>
                                Delete tablet <span className="font-bold">{alias}</span>. Doing so will remove it from
                                the topology, but vttablet and MySQL won't be touched.
                            </>
                        }
                        mutation={deleteTabletMutation as UseMutationResult}
                        loadingText="Deleting..."
                        loadedText="Delete"
                    />
                </div>
            </div>
        </div>
    );
};

export default Advanced;
