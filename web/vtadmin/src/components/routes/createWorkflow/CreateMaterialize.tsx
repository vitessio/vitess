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

import { useClusters, useCreateMaterialize, useKeyspaces, useSchemas } from '../../../hooks/api';
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
import { tabletmanagerdata, vtadmin, vtctldata } from '../../../proto/vtadmin';
import { MultiSelect } from '../../inputs/MultiSelect';
import { TABLET_TYPES } from '../../../util/tablets';
import ErrorDialog from '../../dialog/ErrorDialog';

interface FormData {
    clusterID: string;
    workflow: string;
    targetKeyspace: string;
    sourceKeyspace: string;
    tableSettings: string;
    cells: string;
    referenceTables: string[];
    tabletTypes: number[];
    stopAfterCopy: boolean;
    tabletSelectionPreference: boolean;
}

const DEFAULT_FORM_DATA: FormData = {
    clusterID: '',
    workflow: '',
    targetKeyspace: '',
    sourceKeyspace: '',
    tableSettings: '',
    cells: '',
    referenceTables: [],
    tabletTypes: [1, 2],
    stopAfterCopy: false,
    tabletSelectionPreference: true,
};

const TABLET_OPTIONS = [1, 2, 3];

export const CreateMaterialize = () => {
    useDocumentTitle('Create a Materialize Workflow');

    const history = useHistory();

    const [formData, setFormData] = useState<FormData>(DEFAULT_FORM_DATA);

    const [clusterKeyspaces, setClusterKeyspaces] = useState<vtadmin.Keyspace[]>([]);

    const [targetTables, setTargetTables] = useState<string[]>([]);

    const [errorDialogOpen, setErrorDialogOpen] = useState<boolean>(false);

    const { data: schemas = [] } = useSchemas();

    const { data: clusters = [], ...clustersQuery } = useClusters();

    const { data: keyspaces = [], ...keyspacesQuery } = useKeyspaces();

    const mutation = useCreateMaterialize(
        {
            clusterID: formData.clusterID,
            tableSettings: formData.tableSettings,
            request: {
                settings: {
                    workflow: formData.workflow,
                    source_keyspace: formData.sourceKeyspace,
                    target_keyspace: formData.targetKeyspace,
                    reference_tables: formData.referenceTables,
                    cell: formData.cells
                        .split(',')
                        .map((cell) => cell.trim())
                        .join(','),
                    tablet_types: formData.tabletTypes.map((tt) => TABLET_TYPES[tt]).join(','),
                    stop_after_copy: formData.stopAfterCopy,
                    tablet_selection_preference: formData.tabletSelectionPreference
                        ? tabletmanagerdata.TabletSelectionPreference.INORDER
                        : tabletmanagerdata.TabletSelectionPreference.ANY,
                    // Default Value
                    materialization_intent: vtctldata.MaterializationIntent.CUSTOM,
                },
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

    let selectedSourceKeyspace = null;
    if (!!formData.sourceKeyspace) {
        selectedSourceKeyspace = keyspaces.find((ks) => ks.keyspace?.name === formData.sourceKeyspace);
    }

    let selectedTargetKeyspace = null;
    if (!!formData.targetKeyspace) {
        selectedTargetKeyspace = keyspaces.find((ks) => ks.keyspace?.name === formData.targetKeyspace);
    }

    const isValid =
        !!selectedCluster &&
        !!formData.sourceKeyspace &&
        !!formData.targetKeyspace &&
        !!formData.workflow &&
        !!formData.tableSettings;

    const isDisabled = !isValid || mutation.isLoading;

    const onSubmit: React.FormEventHandler<HTMLFormElement> = (e) => {
        e.preventDefault();
        mutation.mutate();
    };

    useEffect(() => {
        // Clear out the selected keyspaces if selected cluster is changed.
        setFormData((prevFormData) => ({ ...prevFormData, sourceKeyspace: '', targetKeyspace: '' }));
        setClusterKeyspaces(keyspaces.filter((ks) => ks.cluster?.id === formData.clusterID));
    }, [formData.clusterID, keyspaces]);

    useEffect(() => {
        if (clusters.length === 1) {
            setFormData((prevFormData) => ({ ...prevFormData, clusterID: clusters[0].id }));
        }
    }, [clusters]);

    useEffect(() => {
        // Clear out the selected tables if the source keypsace is changed.
        setFormData((prevFormData) => ({ ...prevFormData, tables: [] }));
        setTargetTables([]);
        if (schemas) {
            const schemaData = schemas.find(
                (s) => s.keyspace === formData.targetKeyspace && s.cluster?.id === formData.clusterID
            );
            if (schemaData) {
                setTargetTables(schemaData?.table_definitions.map((def) => def.name || ''));
            }
        }
    }, [formData.targetKeyspace, formData.clusterID, schemas]);

    return (
        <div>
            <WorkspaceHeader>
                <NavCrumbs>
                    <Link to="/workflows">Workflows</Link>
                </NavCrumbs>

                <WorkspaceTitle>Create New Materialize Workflow</WorkspaceTitle>
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
                            label="Source Keyspace"
                            helpText={
                                "Keyspace where the tables queried in the 'source_expression' values within table-settings live"
                            }
                            onChange={(ks) => setFormData({ ...formData, sourceKeyspace: ks?.keyspace?.name || '' })}
                            placeholder={keyspacesQuery.isLoading ? 'Loading keyspaces...' : 'Select a keyspace'}
                            renderItem={(ks) => `${ks?.keyspace?.name}`}
                            selectedItem={selectedSourceKeyspace}
                        />
                        <Select
                            className="block grow min-w-[300px]"
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
                        <Label
                            className="block grow min-w-[300px]"
                            label="Table Settings"
                            helpText={'A JSON array defining what tables to materialize using what select statements.'}
                        >
                            <TextInput
                                onChange={(e) => setFormData({ ...formData, tableSettings: e.target.value })}
                                value={formData.tableSettings || ''}
                                required
                            />
                        </Label>
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
                        <MultiSelect
                            className="block grow min-w-[300px] max-w-screen-md"
                            inputClassName="block w-full"
                            items={targetTables}
                            selectedItems={formData.referenceTables}
                            disabled={!formData.targetKeyspace}
                            label="Reference Tables"
                            helpText={'Reference tables to materialize on every target shard'}
                            onChange={(referenceTables) => setFormData({ ...formData, referenceTables })}
                            placeholder="Select tables"
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
