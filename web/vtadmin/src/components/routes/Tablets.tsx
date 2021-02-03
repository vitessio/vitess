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

import { useTablets } from '../../hooks/api';
import { vtadmin as pb, topodata } from '../../proto/vtadmin';
import { orderBy } from 'lodash-es';

export const Tablets = () => {
    const { data = [] } = useTablets();

    const rows = React.useMemo(() => {
        return orderBy(data, ['cluster.name', 'tablet.keyspace', 'tablet.shard', 'tablet.type']);
    }, [data]);

    return (
        <div>
            <h1>Tablets</h1>
            <table>
                <thead>
                    <tr>
                        <th>Cluster</th>
                        <th>Keyspace</th>
                        <th>Shard</th>
                        <th>Alias</th>
                        <th>Hostname</th>

                        <th>Type</th>
                        <th>State</th>
                    </tr>
                </thead>
                <tbody>
                    {rows.map((t, tdx) => (
                        <tr key={tdx}>
                            <td>{t.cluster?.name}</td>
                            <td>{t.tablet?.keyspace}</td>
                            <td>{t.tablet?.shard}</td>
                            <td>{formatAlias(t)}</td>
                            <td>{t.tablet?.hostname}</td>
                            <td>{formatType(t)}</td>
                            <td>{formatState(t)}</td>
                        </tr>
                    ))}
                </tbody>
            </table>
        </div>
    );
};

const SERVING_STATES = Object.keys(pb.Tablet.ServingState);
const TABLET_TYPES = Object.keys(topodata.TabletType);

const formatAlias = (t: pb.Tablet) =>
    t.tablet?.alias?.cell && t.tablet?.alias?.uid && `${t.tablet.alias.cell}-${t.tablet.alias.uid}`;

const formatType = (t: pb.Tablet) => t.tablet?.type && TABLET_TYPES[t.tablet?.type];

const formatState = (t: pb.Tablet) => t.state && SERVING_STATES[t.state];
