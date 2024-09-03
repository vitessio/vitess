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
import { useEffect, useState } from 'react';
import { useQueryClient } from 'react-query';
import { Link, useHistory } from 'react-router-dom';

import { useClusters, useCreateMoveTables, useKeyspaces } from '../../../hooks/api';
import { useDocumentTitle } from '../../../hooks/useDocumentTitle';
import { Label } from '../../inputs/Label';
import { Select } from '../../inputs/Select';
import { ContentContainer } from '../../layout/ContentContainer';
import { NavCrumbs } from '../../layout/NavCrumbs';
import { WorkspaceHeader } from '../../layout/WorkspaceHeader';
import { WorkspaceTitle } from '../../layout/WorkspaceTitle';
import { TextInput } from '../../TextInput';
import { success } from '../../Snackbar';
import { FormError } from '../../forms/FormError';
import Toggle from '../../toggle/Toggle';
import { vtadmin } from '../../../proto/vtadmin';
import { MultiSelect } from '../../multiSelect/MultiSelect';

interface FormData {
    clusterID: string;
    workflow: string;
    targetKeyspace: string;
    sourceKeyspace: string;
    tables: string;
    cells: string;
    tabletTypes?: number[];
    externalCluster: string;
    onDDL: string;
    sourceTimeZone: string;
    autoStart: boolean;
    allTables: boolean;
}

const DEFAULT_FORM_DATA: FormData = {
    clusterID: '',
    workflow: '',
    targetKeyspace: '',
    sourceKeyspace: '',
    tables: '',
    cells: '',
    externalCluster: '',
    onDDL: '',
    sourceTimeZone: '',
    autoStart: true,
    allTables: false,
};

export const CreateMoveTables = () => {
    useDocumentTitle('Create a Move Tables Workflow');

    const queryClient = useQueryClient();
    const history = useHistory();

    const [formData, setFormData] = useState<FormData>(DEFAULT_FORM_DATA);

    const [clusterKeyspaces, setClusterKeyspaces] = useState<vtadmin.Keyspace[]>([]);

    const { data: clusters = [], ...clustersQuery } = useClusters();

    const { data: keyspaces = [], ...keyspacesQuery } = useKeyspaces();

    const mutation = useCreateMoveTables(
        {
            clusterID: formData.clusterID,
            request: {
                workflow: formData.workflow,
                source_keyspace: formData.sourceKeyspace,
                target_keyspace: formData.targetKeyspace,
                include_tables: formData.tables.split(',').map((table) => table.trim()),
                cells: formData.cells.split(',').map((cell) => cell.trim()),
                tablet_types: formData.tabletTypes,
                all_tables: formData.allTables,
                on_ddl: formData.onDDL,
                external_cluster_name: formData.externalCluster,
                source_time_zone: formData.sourceTimeZone,
                auto_start: formData.autoStart,
            },
        },
        {
            onSuccess: () => {
                success(`Created workflow ${formData.workflow}`, { autoClose: 1600 });
                history.push(`/workflows`);
            },
        }
    );

    let selectedCluster = null;
    if (!!formData.clusterID) {
        selectedCluster = clusters.find((c) => c.id === formData.clusterID);
    }

    let selectedSourceKeyspace = null;
    if (!!formData.sourceKeyspace) {
        selectedSourceKeyspace = keyspaces.find((ks) => ks.keyspace?.name === formData.sourceKeyspace);
    }

    let selectedTargetKeyspace = null;
    if (!!formData.targetKeyspace) {
        selectedTargetKeyspace = keyspaces.find((ks) => ks.keyspace?.name === formData.targetKeyspace);
    }

    const isValid = !!selectedCluster && !!formData.targetKeyspace && !!formData.targetKeyspace && !!formData.workflow;
    const isDisabled = !isValid || mutation.isLoading;

    const onSubmit: React.FormEventHandler<HTMLFormElement> = (e) => {
        e.preventDefault();
        mutation.mutate();
    };

    useEffect(() => {
        setClusterKeyspaces(keyspaces.filter((ks) => ks.cluster?.id === formData.clusterID));
    }, [formData.clusterID, keyspaces]);

    // TODO: Refac this
    const tabletTypes = ['PRIMARY, RDONLY, BACKUP'];
    const [selectedTabletTypes, setSelecltedTabletTypes] = useState<string[]>([]);

    return (
        <div>
            <WorkspaceHeader>
                <NavCrumbs>
                    <Link to="/workflows">Workflows</Link>
                </NavCrumbs>

                <WorkspaceTitle>Create a Move Tables Workflow</WorkspaceTitle>
            </WorkspaceHeader>

            <ContentContainer className="max-w-screen-sm">
                <form onSubmit={onSubmit}>
                    <Select
                        className="block w-full"
                        disabled={clustersQuery.isLoading}
                        inputClassName="block w-full"
                        itemToString={(cluster) => cluster?.name || ''}
                        items={clusters}
                        label="Cluster"
                        onChange={(c) => setFormData({ ...formData, clusterID: c?.id || '' })}
                        placeholder={clustersQuery.isLoading ? 'Loading clusters...' : 'Select a cluster'}
                        renderItem={(c) => `${c?.name} (${c?.id})`}
                        selectedItem={selectedCluster}
                    />

                    {clustersQuery.isError && (
                        <FormError
                            error={clustersQuery.error}
                            title="Couldn't load clusters. Please reload the page to try again."
                        />
                    )}

                    <Select
                        className="block w-full"
                        disabled={keyspacesQuery.isLoading || !selectedCluster}
                        inputClassName="block w-full"
                        itemToString={(ks) => ks?.keyspace?.name || ''}
                        items={clusterKeyspaces}
                        label="Source Keyspace"
                        onChange={(ks) => setFormData({ ...formData, sourceKeyspace: ks?.keyspace?.name || '' })}
                        placeholder={keyspacesQuery.isLoading ? 'Loading keyspaces...' : 'Select a keyspace'}
                        renderItem={(ks) => `${ks?.keyspace?.name}`}
                        selectedItem={selectedSourceKeyspace}
                    />

                    <Select
                        className="block w-full"
                        disabled={keyspacesQuery.isLoading || !selectedCluster}
                        inputClassName="block w-full"
                        itemToString={(ks) => ks?.keyspace?.name || ''}
                        items={clusterKeyspaces}
                        label="Target Keyspace"
                        onChange={(ks) => setFormData({ ...formData, targetKeyspace: ks?.keyspace?.name || '' })}
                        placeholder={keyspacesQuery.isLoading ? 'Loading keyspaces...' : 'Select a keyspace'}
                        renderItem={(ks) => `${ks?.keyspace?.name}`}
                        selectedItem={selectedTargetKeyspace}
                    />

                    <MultiSelect
                        items={tabletTypes}
                        selectedItems={selectedTabletTypes}
                        setSelectedItems={setSelecltedTabletTypes}
                        itemToString={(item) => item}
                        renderDisplayText={(items) => items.join(', ')}
                    />

                    <Label className="block my-8" label="Workflow Name">
                        <TextInput
                            onChange={(e) => setFormData({ ...formData, workflow: e.target.value })}
                            value={formData.workflow || ''}
                            required
                        />
                    </Label>

                    <Label className="block my-8" label="Tables">
                        <TextInput
                            onChange={(e) => setFormData({ ...formData, tables: e.target.value })}
                            value={formData.tables || ''}
                        />
                    </Label>

                    <Label className="block my-8" label="Cells">
                        <TextInput
                            onChange={(e) => setFormData({ ...formData, cells: e.target.value })}
                            value={formData.cells || ''}
                        />
                    </Label>

                    <Label className="block my-8" label="External Cluster">
                        <TextInput
                            onChange={(e) => setFormData({ ...formData, externalCluster: e.target.value })}
                            value={formData.externalCluster || ''}
                        />
                    </Label>

                    <div className="mt-2">
                        <div className="flex items-center">
                            <Toggle
                                className="mr-2"
                                enabled={formData.autoStart}
                                onChange={() => setFormData({ ...formData, autoStart: !formData.autoStart })}
                            />
                            <Label label="Auto Start" />
                        </div>
                        If enabled, the move will be started automatically.
                    </div>

                    <div className="mt-2">
                        <div className="flex items-center">
                            <Toggle
                                className="mr-2"
                                enabled={formData.allTables}
                                onChange={() => setFormData({ ...formData, allTables: !formData.allTables })}
                            />
                            <Label label="All Tables" />
                        </div>
                        If enabled, the move will copy all the tables from source keyspace.
                    </div>

                    {mutation.isError && !mutation.isLoading && (
                        <FormError error={mutation.error} title="Couldn't create workflow. Please try again." />
                    )}

                    <div className="my-12">
                        <button className="btn" disabled={isDisabled} type="submit">
                            {mutation.isLoading ? 'Creating Workflow...' : 'Create Workflow'}
                        </button>
                    </div>
                </form>
            </ContentContainer>
        </div>
    );
};
