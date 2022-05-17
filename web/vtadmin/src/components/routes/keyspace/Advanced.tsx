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

import { UseMutationResult } from 'react-query';

import { useKeyspace, useReloadSchema } from '../../../hooks/api';
import ActionPanel from '../../ActionPanel';
import { QueryLoadingPlaceholder } from '../../placeholders/QueryLoadingPlaceholder';
import { success, warn } from '../../Snackbar';

interface Props {
    clusterID: string;
    name: string;
}

export const Advanced: React.FC<Props> = ({ clusterID, name }) => {
    const kq = useKeyspace({ clusterID, name });

    const { data: keyspace } = kq;

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

    return (
        <div className="pt-4">
            <div className="my-8">
                <h3 className="mb-4">Schemas</h3>

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
                    </div>
                )}
            </div>
        </div>
    );
};
