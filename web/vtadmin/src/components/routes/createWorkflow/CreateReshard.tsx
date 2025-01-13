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
import { useEffect, useState } from 'react';
import { Link, useHistory } from 'react-router-dom';

import { useClusters, useCreateReshard, useKeyspaces } from '../../../hooks/api';
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
import { tabletmanagerdata, vtadmin } from '../../../proto/vtadmin';
import { MultiSelect } from '../../inputs/MultiSelect';
import { TABLET_TYPES } from '../../../util/tablets';
import ErrorDialog from '../../dialog/ErrorDialog';

interface FormData {
    clusterID: string;
    workflow: string;
    keyspace: string;
    sourceShards: string;
    targetShards: string;
    cells: string;
    tabletTypes: number[];
    onDDL: string;
    autoStart: boolean;
    tabletSelectionPreference: boolean;
    skipSchemaCopy: boolean;
    stopAfterCopy: boolean;
    deferSecondaryKeys: boolean;
}

const DEFAULT_FORM_DATA: FormData = {
    clusterID: '',
    workflow: '',
    keyspace: '',
    sourceShards: '',
    targetShards: '',
    cells: '',
    tabletTypes: [1, 2],
    onDDL: 'IGNORE',
    autoStart: true,
    tabletSelectionPreference: true,
    skipSchemaCopy: false,
    stopAfterCopy: false,
    deferSecondaryKeys: false,
};

const TABLET_OPTIONS = [1, 2, 3];

const onDDLOptions = ['IGNORE', 'STOP', 'EXEC', 'EXEC_IGNORE'];

export const CreateReshard = () => {
    useDocumentTitle('Create a Reshard Workflow');

    const history = useHistory();

    const [formData, setFormData] = useState<FormData>(DEFAULT_FORM_DATA);

    const [clusterKeyspaces, setClusterKeyspaces] = useState<vtadmin.Keyspace[]>([]);

    const [errorDialogOpen, setErrorDialogOpen] = useState<boolean>(false);

    const { data: clusters = [], ...clustersQuery } = useClusters();

    const { data: keyspaces = [], ...keyspacesQuery } = useKeyspaces();

    const mutation = useCreateReshard(
        {
            clusterID: formData.clusterID,
            request: {
                workflow: formData.workflow,
                keyspace: formData.keyspace,
                source_shards: formData.sourceShards.split(',').map((shard) => shard.trim()),
                target_shards: formData.targetShards.split(',').map((shard) => shard.trim()),
                cells: formData.cells.split(',').map((cell) => cell.trim()),
                tablet_types: formData.tabletTypes,
                tablet_selection_preference: formData.tabletSelectionPreference
                    ? tabletmanagerdata.TabletSelectionPreference.INORDER
                    : tabletmanagerdata.TabletSelectionPreference.ANY,
                skip_schema_copy: formData.skipSchemaCopy,
                on_ddl: formData.onDDL,
                auto_start: formData.autoStart,
                stop_after_copy: formData.stopAfterCopy,
                defer_secondary_keys: formData.deferSecondaryKeys,
            },
        },
        {
            onSuccess: () => {
                success(`Created workflow ${formData.workflow}`, { autoClose: 1600 });
                history.push(`/workflows`);
            },
            onError: () => {
                setErrorDialogOpen(true);
            },
        }
    );

    let selectedCluster = null;
    if (!!formData.clusterID) {
        selectedCluster = clusters.find((c) => c.id === formData.clusterID);
    }

    let selectedKeyspace = null;
    if (!!formData.keyspace) {
        selectedKeyspace = keyspaces.find((ks) => ks.keyspace?.name === formData.keyspace);
    }

    const isValid =
        !!selectedCluster &&
        !!formData.keyspace &&
        !!formData.workflow &&
        !!formData.targetShards &&
        !!formData.sourceShards &&
        !!formData.tabletTypes.length &&
        !!formData.onDDL;

    const isDisabled = !isValid || mutation.isLoading;

    const onSubmit: React.FormEventHandler<HTMLFormElement> = (e) => {
        e.preventDefault();
        mutation.mutate();
    };

    useEffect(() => {
        // Clear out the selected keyspaces if selected cluster is changed.
        setFormData((prevFormData) => ({ ...prevFormData, keyspace: '' }));
        setClusterKeyspaces(keyspaces.filter((ks) => ks.cluster?.id === formData.clusterID));
    }, [formData.clusterID, keyspaces]);

    useEffect(() => {
        if (clusters.length === 1) {
            setFormData((prevFormData) => ({ ...prevFormData, clusterID: clusters[0].id }));
        }
    }, [clusters]);
    return (
        <div>
            <WorkspaceHeader>
                <NavCrumbs>
                    <Link to="/workflows">Workflows</Link>
                </NavCrumbs>

                <WorkspaceTitle>Create New Reshard Workflow</WorkspaceTitle>
            </WorkspaceHeader>

            <ContentContainer className="max-w-screen-sm">
                <form onSubmit={onSubmit}>
                    <div className="flex flex-row gap-4 flex-wrap">
                        <Label className="block grow min-w-[300px]" label="Workflow Name">
                            <TextInput
                                onChange={(e) => setFormData({ ...formData, workflow: e.target.value })}
                                value={formData.workflow || ''}
                                required
                            />
                        </Label>
                        <Select
                            className="block grow min-w-[300px]"
                            disabled={keyspacesQuery.isLoading || !selectedCluster}
                            inputClassName="block w-full"
                            itemToString={(ks) => ks?.keyspace?.name || ''}
                            items={clusterKeyspaces}
                            label="Keyspace"
                            onChange={(ks) => setFormData({ ...formData, keyspace: ks?.keyspace?.name || '' })}
                            placeholder={keyspacesQuery.isLoading ? 'Loading keyspaces...' : 'Select a keyspace'}
                            renderItem={(ks) => `${ks?.keyspace?.name}`}
                            selectedItem={selectedKeyspace}
                        />
                        <div className="flex grow flex-row gap-4 flex-wrap">
                            <Label
                                className="block grow min-w-[300px]"
                                label="Source Shards"
                                helpText={'Comma separated source shards'}
                            >
                                <TextInput
                                    onChange={(e) => setFormData({ ...formData, sourceShards: e.target.value })}
                                    value={formData.sourceShards || ''}
                                />
                            </Label>
                            <Label
                                className="block grow min-w-[300px]"
                                label="Target Shards"
                                helpText={'Comma separated target shards'}
                            >
                                <TextInput
                                    onChange={(e) => setFormData({ ...formData, targetShards: e.target.value })}
                                    value={formData.targetShards || ''}
                                />
                            </Label>
                        </div>
                    </div>

                    <div className="my-2 mt-4">
                        <div className="flex items-center">
                            <Toggle
                                className="mr-2"
                                enabled={formData.autoStart}
                                onChange={() => setFormData({ ...formData, autoStart: !formData.autoStart })}
                            />
                            <Label
                                label="Auto Start"
                                helpText={'If enabled, the workflow will be started automatically.'}
                            />
                        </div>
                    </div>

                    <h3 className="mt-8 mb-2">Advanced</h3>

                    <div className="flex flex-row gap-4 flex-wrap">
                        <Label
                            className="block grow min-w-[300px]"
                            label="Cells"
                            helpText={'Cells and/or CellAliases to copy table data from'}
                        >
                            <TextInput
                                onChange={(e) => setFormData({ ...formData, cells: e.target.value })}
                                value={formData.cells || ''}
                            />
                        </Label>
                        <Select
                            className="block grow min-w-[300px]"
                            inputClassName="block w-full"
                            items={onDDLOptions}
                            label="OnDDL Strategy"
                            helpText={'What to do when DDL is encountered in the VReplication stream'}
                            onChange={(option) => setFormData({ ...formData, onDDL: option || '' })}
                            placeholder={'Select the OnDDL strategy'}
                            selectedItem={formData.onDDL}
                        />
                        <Select
                            className="block grow min-w-[300px]"
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
                        <MultiSelect
                            className="block grow min-w-[300px]"
                            inputClassName="block w-full"
                            items={TABLET_OPTIONS}
                            itemToString={(tt) => TABLET_TYPES[tt]}
                            selectedItems={formData.tabletTypes}
                            label="Tablet Types"
                            helpText={'Source tablet types to replicate table data from'}
                            onChange={(types) => setFormData({ ...formData, tabletTypes: types })}
                            placeholder="Select tablet types"
                        />
                    </div>

                    <div className="flex flex-row gap-4 flex-wrap">
                        <div className="my-2 mt-4">
                            <div className="flex items-center">
                                <Toggle
                                    className="mr-2"
                                    enabled={formData.tabletSelectionPreference}
                                    onChange={() =>
                                        setFormData({
                                            ...formData,
                                            tabletSelectionPreference: !formData.tabletSelectionPreference,
                                        })
                                    }
                                />
                                <Label
                                    label="Tablet Selection Preference"
                                    helpText={
                                        'When performing source tablet selection, look for candidates in the type order as they are listed in the Tablet Types.'
                                    }
                                />
                            </div>
                        </div>

                        <div className="my-2 mt-4">
                            <div className="flex items-center">
                                <Toggle
                                    className="mr-2"
                                    enabled={formData.skipSchemaCopy}
                                    onChange={() =>
                                        setFormData({ ...formData, skipSchemaCopy: !formData.skipSchemaCopy })
                                    }
                                />
                                <Label
                                    label="Skip Schema Copy"
                                    helpText={'Skip copying the schema from the source shards to the target shards.'}
                                />
                            </div>
                        </div>

                        <div className="my-2 mt-4">
                            <div className="flex items-center">
                                <Toggle
                                    className="mr-2"
                                    enabled={formData.stopAfterCopy}
                                    onChange={() =>
                                        setFormData({ ...formData, stopAfterCopy: !formData.stopAfterCopy })
                                    }
                                />
                                <Label
                                    label="Stop After Copy"
                                    helpText={
                                        "Stop the workflow after it's finished copying the existing rows and before it starts replicating changes."
                                    }
                                />
                            </div>
                        </div>

                        <div className="my-2 mt-4">
                            <div className="flex items-center">
                                <Toggle
                                    className="mr-2"
                                    enabled={formData.deferSecondaryKeys}
                                    onChange={() =>
                                        setFormData({ ...formData, deferSecondaryKeys: !formData.deferSecondaryKeys })
                                    }
                                />
                                <Label
                                    label="Defer Secondary Keys"
                                    helpText={
                                        'Defer secondary index creation for a table until after it has been copied.'
                                    }
                                />
                            </div>
                        </div>
                    </div>

                    {clustersQuery.isError && (
                        <FormError
                            error={clustersQuery.error}
                            title="Couldn't load clusters. Please reload the page to try again."
                        />
                    )}

                    <div className="my-8">
                        <button className="btn" disabled={isDisabled} type="submit">
                            {mutation.isLoading ? 'Creating Workflow...' : 'Create Workflow'}
                        </button>
                    </div>
                </form>

                {mutation.isError && !mutation.isLoading && (
                    <ErrorDialog
                        errorDescription={mutation.error.message}
                        errorTitle="Error Creating Workflow"
                        isOpen={errorDialogOpen}
                        onClose={() => {
                            setErrorDialogOpen(false);
                        }}
                    />
                )}
            </ContentContainer>
        </div>
    );
};
