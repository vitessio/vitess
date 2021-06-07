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
import React from 'react';
import { orderBy } from 'lodash-es';

import { vtadmin as pb } from '../../proto/vtadmin';
import { useKeyspaces, useVTExplain } from '../../hooks/api';
import { Button } from '../Button';
import { Select } from '../inputs/Select';
import { ContentContainer } from '../layout/ContentContainer';
import { WorkspaceHeader } from '../layout/WorkspaceHeader';
import { WorkspaceTitle } from '../layout/WorkspaceTitle';
import style from './VTExplain.module.scss';
import { Code } from '../Code';
import { useDocumentTitle } from '../../hooks/useDocumentTitle';
import { Label } from '../inputs/Label';

// TODO(doeg): persist form data in URL, error handling, loading state, um... hm. Most things still need doing.
// This whole component is the hastiest prototype ever. :')
export const VTExplain = () => {
    useDocumentTitle('VTExplain');

    const { data: keyspaces = [] } = useKeyspaces();

    const [clusterID, updateCluster] = React.useState<string | null | undefined>(null);
    const [keyspaceName, updateKeyspace] = React.useState<string | null | undefined>(null);
    const [sql, updateSQL] = React.useState<string | null | undefined>(null);

    const selectedKeyspace =
        clusterID && keyspaceName
            ? keyspaces?.find((k) => k.cluster?.id === clusterID && k.keyspace?.name === keyspaceName)
            : null;

    const { data, error, refetch } = useVTExplain(
        { cluster: clusterID, keyspace: keyspaceName, sql },
        {
            // Never cache, never refetch.
            cacheTime: 0,
            enabled: false,
            refetchOnWindowFocus: false,
            retry: false,
        }
    );

    const onChangeKeyspace = (selectedKeyspace: pb.Keyspace | null | undefined) => {
        updateCluster(selectedKeyspace?.cluster?.id);
        updateKeyspace(selectedKeyspace?.keyspace?.name);
    };

    const onChangeSQL: React.ChangeEventHandler<HTMLTextAreaElement> = (e) => {
        updateSQL(e.target.value);
    };

    const onSubmit: React.FormEventHandler<HTMLFormElement> = (e) => {
        e.preventDefault();
        refetch();
    };

    return (
        <div>
            <WorkspaceHeader>
                <WorkspaceTitle>VTExplain</WorkspaceTitle>
            </WorkspaceHeader>
            <ContentContainer className={style.container}>
                <section className={style.panel}>
                    <form className={style.form} onSubmit={onSubmit}>
                        <div>
                            <Select
                                itemToString={(keyspace) => keyspace?.keyspace?.name || ''}
                                items={orderBy(keyspaces, ['keyspace.name', 'cluster.id'])}
                                label="Keyspace"
                                onChange={onChangeKeyspace}
                                placeholder="Choose a keyspace"
                                renderItem={(keyspace) => `${keyspace?.keyspace?.name} (${keyspace?.cluster?.id})`}
                                selectedItem={selectedKeyspace || null}
                            />
                        </div>
                        <div>
                            <Label label="SQL">
                                <textarea
                                    className={style.sqlInput}
                                    onChange={onChangeSQL}
                                    rows={10}
                                    value={sql || ''}
                                />
                            </Label>
                        </div>
                        <div className={style.buttons}>
                            <Button type="submit">Run VTExplain</Button>
                        </div>
                    </form>
                </section>

                {error && (
                    <section className={style.errorPanel}>
                        <Code code={JSON.stringify(error, null, 2)} />
                    </section>
                )}

                {data?.response && (
                    <section className={style.panel}>
                        <div className={style.codeContainer}>
                            <Code code={data?.response} />
                        </div>
                    </section>
                )}
            </ContentContainer>
        </div>
    );
};
