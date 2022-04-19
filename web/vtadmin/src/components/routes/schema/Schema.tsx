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
import * as React from 'react';
import { Link, useParams } from 'react-router-dom';

import style from './Schema.module.scss';
import { useSchema, useVSchema } from '../../../hooks/api';
import { Code } from '../../Code';
import { useDocumentTitle } from '../../../hooks/useDocumentTitle';
import { getVindexesForTable } from '../../../util/vschemas';
import { ContentContainer } from '../../layout/ContentContainer';
import { NavCrumbs } from '../../layout/NavCrumbs';
import { WorkspaceHeader } from '../../layout/WorkspaceHeader';
import { WorkspaceTitle } from '../../layout/WorkspaceTitle';
import { Tooltip } from '../../tooltip/Tooltip';
import { KeyspaceLink } from '../../links/KeyspaceLink';

interface RouteParams {
    clusterID: string;
    keyspace: string;
    table: string;
}

export const Schema = () => {
    const { clusterID, keyspace, table } = useParams<RouteParams>();
    useDocumentTitle(`${table} (${keyspace})`);

    const { data, error, isError, isLoading, isSuccess } = useSchema({ clusterID, keyspace, table });
    const vschemaQuery = useVSchema({ clusterID, keyspace });

    const tableDefinition = React.useMemo(
        () =>
            data && Array.isArray(data.table_definitions)
                ? data?.table_definitions.find((t) => t.name === table)
                : null,
        [data, table]
    );

    const tableVindexes = React.useMemo(
        () => (vschemaQuery.data ? getVindexesForTable(vschemaQuery.data, table) : []),
        [vschemaQuery.data, table]
    );

    const is404 = isSuccess && !tableDefinition;

    return (
        <div>
            {!is404 && !isError && (
                <WorkspaceHeader>
                    <NavCrumbs>
                        <Link to="/schemas">Schemas</Link>
                    </NavCrumbs>

                    <WorkspaceTitle className="font-family-monospace">{table}</WorkspaceTitle>

                    <div className={style.headingMeta}>
                        <span>
                            Cluster: <code>{clusterID}</code>
                        </span>
                        <span>
                            Keyspace:{' '}
                            <KeyspaceLink clusterID={clusterID} name={keyspace}>
                                <code>{keyspace}</code>
                            </KeyspaceLink>
                        </span>
                    </div>
                </WorkspaceHeader>
            )}

            <ContentContainer>
                {/* TODO: skeleton placeholder */}
                {isLoading && <div className={style.loadingPlaceholder}>Loading...</div>}

                {isError && (
                    <div className={style.errorPlaceholder}>
                        <span className={style.errorEmoji}>😰</span>
                        <h1>An error occurred</h1>
                        <code>{(error as any).response?.error?.message || error?.message}</code>
                        <p>
                            <Link to="/schemas">← All schemas</Link>
                        </p>
                    </div>
                )}

                {is404 && (
                    <div className={style.errorPlaceholder}>
                        <span className={style.errorEmoji}>😖</span>
                        <h1>Schema not found</h1>
                        <p>
                            No schema found with table <code>{table}</code> in keyspace <code>{keyspace}</code> (cluster{' '}
                            <code>{clusterID}</code>).
                        </p>
                        <p>
                            <Link to="/schemas">← All schemas</Link>
                        </p>
                    </div>
                )}

                {!is404 && !isError && tableDefinition && (
                    <div className={style.container}>
                        <section className={style.panel}>
                            <h3>Table Definition</h3>
                            <Code code={tableDefinition.schema} />
                        </section>

                        {!!tableVindexes.length && (
                            <section className={style.panel}>
                                <h3>Vindexes</h3>
                                <p>
                                    A Vindex provides a way to map a column value to a keyspace ID. Since each shard in
                                    Vitess covers a range of keyspace ID values, this mapping can be used to identify
                                    which shard contains a row.{' '}
                                    <a
                                        href="https://vitess.io/docs/reference/features/vindexes/"
                                        target="_blank"
                                        rel="noopen noreferrer"
                                    >
                                        Learn more about Vindexes in the Vitess documentation.
                                    </a>
                                </p>

                                <table>
                                    <thead>
                                        <tr>
                                            <th>Vindex</th>
                                            <th>Columns</th>
                                            <th>Type</th>
                                            <th>Params</th>
                                        </tr>
                                    </thead>
                                    <tbody className="font-family-monospace">
                                        {tableVindexes.map((v, vdx) => {
                                            const columns = v.column ? [v.column] : v.columns;
                                            return (
                                                <tr key={v.name}>
                                                    <td>
                                                        {v.name}

                                                        {vdx === 0 && (
                                                            <Tooltip text="A table's Primary Vindex maps a column value to a keyspace ID.">
                                                                <span className={style.skBadge}>Primary</span>
                                                            </Tooltip>
                                                        )}
                                                    </td>
                                                    <td>{(columns || []).join(', ')}</td>
                                                    <td>{v.meta?.type}</td>
                                                    <td>
                                                        {v.meta?.params ? (
                                                            Object.entries(v.meta.params).map(([k, val]) => (
                                                                <div key={k}>
                                                                    <strong>{k}: </strong> {val}
                                                                </div>
                                                            ))
                                                        ) : (
                                                            <span className="font-size-small text-color-secondary">
                                                                N/A
                                                            </span>
                                                        )}
                                                    </td>
                                                </tr>
                                            );
                                        })}
                                    </tbody>
                                </table>
                            </section>
                        )}
                    </div>
                )}
            </ContentContainer>
        </div>
    );
};
