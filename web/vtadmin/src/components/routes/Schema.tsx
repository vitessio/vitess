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
import { useSchema } from '../../hooks/api';
import { Code } from '../Code';
import { useDocumentTitle } from '../../hooks/useDocumentTitle';

interface RouteParams {
    clusterID: string;
    keyspace: string;
    table: string;
}

export const Schema = () => {
    const { clusterID, keyspace, table } = useParams<RouteParams>();
    const { data, error, isError, isLoading, isSuccess } = useSchema({ clusterID, keyspace, table });

    useDocumentTitle(`${table} (${keyspace})`);

    const tableDefinition = React.useMemo(
        () =>
            data && Array.isArray(data.table_definitions)
                ? data?.table_definitions.find((t) => t.name === table)
                : null,
        [data, table]
    );

    const is404 = isSuccess && !tableDefinition;

    return (
        <div>
            {!is404 && !isError && (
                <header className={style.header}>
                    <p>
                        <Link to="/schemas">← All schemas</Link>
                    </p>
                    <code>
                        <h1>{table}</h1>
                    </code>
                    <div className={style.headingMeta}>
                        <span>
                            Cluster: <code>{clusterID}</code>
                        </span>
                        <span>
                            Keyspace: <code>{keyspace}</code>
                        </span>
                    </div>
                </header>
            )}

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
                </div>
            )}
        </div>
    );
};
