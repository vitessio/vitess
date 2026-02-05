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

import { useState } from 'react';
import { UseMutationResult } from 'react-query';

import {
    useCreateShard,
    useKeyspace,
    useRebuildKeyspaceGraph,
    useReloadSchema,
    useRemoveKeyspaceCell,
} from '../../../hooks/api';
import ActionPanel from '../../ActionPanel';
import { Label } from '../../inputs/Label';
import { QueryLoadingPlaceholder } from '../../placeholders/QueryLoadingPlaceholder';
import { success, warn } from '../../Snackbar';
import { TextInput } from '../../TextInput';
import Toggle from '../../toggle/Toggle';

interface Props {
    clusterID: string;
    name: string;
}

export const Advanced: React.FC<Props> = ({ clusterID, name }) => {
    const kq = useKeyspace({ clusterID, name });
    const { data: keyspace } = kq;

    // RebuildKeyspaceGraph params
    const [cells, setCells] = useState('');
    const [allowPartial, setAllowPartial] = useState(false);

    // RemoveKeyspaceCell params
    const [force, setForce] = useState(false);
    const [recursive, setRecursive] = useState(false);
    const [removeKeyspaceCellCell, setRemoveKeyspaceCellCell] = useState('');

    // CreateShard params
    const [includeParent, setIncludeParent] = useState(false);
    const [forceCreateShard, setForceCreateShard] = useState(false);
    const [shardName, setShardName] = useState('');

    const reloadSchemaMutation = useReloadSchema(
        {
            clusterIDs: [clusterID],
            keyspaces: [name],
        },
        {
            onError: (error) => warn(`There was an error reloading the schemas in the ${name} keyspace: ${error}`),
            onSuccess: () => {
                success(`Successfully reloaded schemas in the ${name} keyspace.`, { autoClose: 1600 });
            },
        }
    );

    const rebuildKeyspaceGraphMutation = useRebuildKeyspaceGraph(
        {
            keyspace: name,
            clusterID,
            allowPartial,
            cells,
        },
        {
            onError: (error) =>
                warn(`There was an error rebuilding the keyspace graph in the ${name} keyspace: ${error}`),
            onSuccess: () => {
                success(`Successfully rebuilt the keyspace graph in the ${name} keyspace.`, { autoClose: 1600 });
            },
        }
    );

    const removeKeyspaceCellMutation = useRemoveKeyspaceCell(
        {
            keyspace: name,
            clusterID,
            force,
            recursive,
            cell: removeKeyspaceCellCell,
        },
        {
            onError: (error) =>
                warn(`There was an error removing cell ${removeKeyspaceCellCell} from keyspace ${name}: ${error}`),
            onSuccess: () => {
                success(`Successfully removed cell ${removeKeyspaceCellCell} from the ${name} keyspace.`, {
                    autoClose: 1600,
                });
            },
        }
    );

    const createShardMutation = useCreateShard(
        {
            keyspace: name,
            clusterID,
            force,
            include_parent: includeParent,
            shard_name: shardName,
        },
        {
            onError: (error) => warn(`There was an error creating shard ${shardName} in keyspace ${name}: ${error}`),
            onSuccess: () => {
                success(`Successfully created shard ${shardName} in the ${name} keyspace.`, {
                    autoClose: 1600,
                });
            },
        }
    );

    return (
        <div className="pt-4">
            <div className="my-8">
                <h3 className="mb-4">Rebuild and Reload</h3>
                <QueryLoadingPlaceholder query={kq} />
                {keyspace && (
                    <div>
                        <ActionPanel
                            description={
                                <>
                                    Reloads the schema on all the tablets, except the primary tablet, in the{' '}
                                    <span className="font-bold">{name}</span> keyspace.
                                </>
                            }
                            documentationLink="https://vitess.io/docs/13.0/reference/programs/vtctl/schema-version-permissions/#reloadschemakeyspace"
                            loadedText="Reload Schema"
                            loadingText="Reloading Schema..."
                            mutation={reloadSchemaMutation as UseMutationResult}
                            title="Reload Schema"
                        />
                        <ActionPanel
                            description={
                                <>
                                    Rebuilds the serving data for the <span className="font-bold">{name}</span>{' '}
                                    keyspace. This command may trigger an update to all connected clients.
                                </>
                            }
                            documentationLink="https://vitess.io/docs/14.0/reference/programs/vtctl/keyspaces/#rebuildkeyspacegraph"
                            loadedText="Rebuild Keyspace Graph"
                            loadingText="Rebuilding keyspace graph..."
                            mutation={rebuildKeyspaceGraphMutation as UseMutationResult}
                            title="Rebuild Keyspace Graph"
                            body={
                                <>
                                    <p className="text-base">
                                        <strong>Cells</strong> <br />
                                        Specify a comma-separated list of cells to update:
                                    </p>
                                    <div className="w-1/3">
                                        <TextInput value={cells} onChange={(e) => setCells(e.target.value)} />
                                    </div>
                                    <div className="mt-2">
                                        <div className="flex items-center">
                                            <Toggle
                                                className="mr-2"
                                                enabled={allowPartial}
                                                onChange={() => setAllowPartial(!allowPartial)}
                                            />
                                            <Label label="Allow Partial" />
                                        </div>
                                        When set, allows a SNAPSHOT keyspace to serve with an incomplete set of shards.
                                        It is ignored for all other keyspace types.
                                    </div>
                                </>
                            }
                        />
                    </div>
                )}
            </div>
            <div className="my-8">
                <h3 className="mb-4">Change</h3>
                {keyspace && (
                    <div>
                        <ActionPanel
                            description={
                                <>
                                    Remove a cell from the Cells list for all shards in the{' '}
                                    <span className="font-bold">{name}</span> keyspace, and the SrvKeyspace for the
                                    keyspace in that cell.
                                </>
                            }
                            documentationLink="https://vitess.io/docs/14.0/reference/programs/vtctl/keyspaces/#removekeyspacecell"
                            loadedText="Remove Keyspace Cell"
                            loadingText="Removing keyspace cell..."
                            mutation={removeKeyspaceCellMutation as UseMutationResult}
                            title="Remove Keyspace Cell"
                            disabled={removeKeyspaceCellCell === ''}
                            body={
                                <>
                                    <Label label="Cell" aria-required required />
                                    <div className="w-1/3">
                                        <TextInput
                                            required={true}
                                            value={removeKeyspaceCellCell}
                                            onChange={(e) => setRemoveKeyspaceCellCell(e.target.value)}
                                        />
                                    </div>
                                    <div className="mt-2">
                                        <div className="flex items-center">
                                            <Toggle
                                                className="mr-2"
                                                enabled={force}
                                                onChange={() => setForce(!force)}
                                            />
                                            <Label label="Force" />
                                        </div>
                                        When set, proceeds even if the cell's topology service cannot be reached. The
                                        assumption is that you turned down the entire cell, and just need to update the
                                        global topo data.
                                    </div>
                                    <div className="mt-2">
                                        <div className="flex items-center">
                                            <Toggle
                                                className="mr-2"
                                                enabled={recursive}
                                                onChange={() => setRecursive(!recursive)}
                                            />
                                            <Label label="Recursive" />
                                        </div>
                                        When set, also deletes all tablets in that cell belonging to the specified
                                        keyspace.
                                    </div>
                                </>
                            }
                        />
                        <ActionPanel
                            description={
                                <>
                                    Creates a new shard in the <span className="font-bold">{name}</span> keyspace.
                                </>
                            }
                            documentationLink="https://vitess.io/docs/14.0/reference/programs/vtctl/shards/#createshard"
                            loadedText="Create Shard"
                            loadingText="Creating shard..."
                            mutation={createShardMutation as UseMutationResult}
                            title="Create Shard"
                            disabled={shardName === ''}
                            body={
                                <>
                                    <Label label="Shard Name" aria-required required />
                                    <br />
                                    <div className="mb-2">The name of the shard to create. E.g. "-" or "-80".</div>
                                    <div className="w-1/3">
                                        <TextInput
                                            required={true}
                                            value={shardName}
                                            onChange={(e) => setShardName(e.target.value)}
                                        />
                                    </div>
                                    <div className="mt-2">
                                        <div className="flex items-center">
                                            <Toggle
                                                className="mr-2"
                                                enabled={forceCreateShard}
                                                onChange={() => setForceCreateShard(!forceCreateShard)}
                                            />
                                            <Label label="Force" />
                                        </div>
                                        When set, proceeds with the command even if the keyspace already exists.
                                    </div>
                                    <div className="mt-2">
                                        <div className="flex items-center">
                                            <Toggle
                                                className="mr-2"
                                                enabled={includeParent}
                                                onChange={() => setIncludeParent(!recursive)}
                                            />
                                            <Label label="Include Parent" />
                                        </div>
                                        When set, creates the parent keyspace if it doesn't already exist.
                                    </div>
                                </>
                            }
                        />
                    </div>
                )}
            </div>
        </div>
    );
};
