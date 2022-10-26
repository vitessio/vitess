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
import { useQueryClient } from 'react-query';
import { Link, useHistory } from 'react-router-dom';

import { useClusters, useCreateKeyspace } from '../../../hooks/api';
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

interface FormData {
    clusterID: string;
    keyspaceName: string;
}

const DEFAULT_FORM_DATA: FormData = {
    clusterID: '',
    keyspaceName: '',
};

export const CreateKeyspace = () => {
    useDocumentTitle('Create a Keyspace');

    const queryClient = useQueryClient();
    const history = useHistory();

    const [formData, setFormData] = useState<FormData>(DEFAULT_FORM_DATA);

    const { data: clusters = [], ...clustersQuery } = useClusters();

    const mutation = useCreateKeyspace(
        {
            clusterID: formData.clusterID,
            options: {
                name: formData.keyspaceName,
            },
        },
        {
            onSuccess: (res) => {
                queryClient.invalidateQueries('keyspaces');
                success(`Created keyspace ${res?.keyspace?.keyspace?.name}`, { autoClose: 1600 });
                history.push(`/keyspace/${res?.keyspace?.cluster?.id}/${res?.keyspace?.keyspace?.name}`);
            },
        }
    );

    let selectedCluster = null;
    if (!!formData.clusterID) {
        selectedCluster = clusters.find((c) => c.id === formData.clusterID);
    }

    const isValid = !!selectedCluster && !!formData.keyspaceName;
    const isDisabled = !isValid || mutation.isLoading;

    const onSubmit: React.FormEventHandler<HTMLFormElement> = (e) => {
        e.preventDefault();
        mutation.mutate();
    };

    return (
        <div>
            <WorkspaceHeader>
                <NavCrumbs>
                    <Link to="/keyspaces">Keyspaces</Link>
                </NavCrumbs>

                <WorkspaceTitle>Create a Keyspace</WorkspaceTitle>
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

                    <Label className="block my-8" label="Keyspace Name">
                        <TextInput
                            onChange={(e) => setFormData({ ...formData, keyspaceName: e.target.value })}
                            value={formData.keyspaceName || ''}
                        />
                    </Label>

                    {mutation.isError && !mutation.isLoading && (
                        <FormError error={mutation.error} title="Couldn't create keyspace. Please try again." />
                    )}

                    <div className="my-12">
                        <button className="btn" disabled={isDisabled} type="submit">
                            {mutation.isLoading ? 'Creating Keyspace...' : 'Create Keyspace'}
                        </button>
                    </div>
                </form>
            </ContentContainer>
        </div>
    );
};
