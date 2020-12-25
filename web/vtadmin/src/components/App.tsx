/**
 * Copyright 2020 The Vitess Authors.
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

import style from './App.module.scss';
import logo from '../img/vitess-icon-color.svg';
import { TabletList } from './TabletList';
import { vtadmin as pb, topodata } from '../proto/vtadmin';

export const App = () => {
    return (
        <div className={style.container}>
            <img src={logo} alt="logo" height={40} />
            <h1>VTAdmin</h1>
            <TabletList tablets={fakeTablets} />
        </div>
    );
};

// Add some fake data solely for this super duper simple hello world example.
// When we do this for reals, we'll be fetching cluster JSON over HTTP
// from vtadmin-api and casting/validating the responses to Cluster objects.
// This requires merging https://github.com/vitessio/vitess/pull/7187 first.
//
// Long-term, we can use grpc in the browser with a library like https://github.com/grpc/grpc-web,
// which will obviate the casting/validation steps, too.
const fakeTablets = [
    {
        cluster: { id: 'prod', name: 'prod' },
        tablet: {
            hostname: 'tablet-prod-commerce-00-00-abcd',
            keyspace: 'commerce',
            shard: '-',
            type: topodata.TabletType.MASTER,
        },
        state: pb.Tablet.ServingState.SERVING,
    },
    {
        cluster: { id: 'prod', name: 'prod' },
        tablet: {
            hostname: 'tablet-prod-commerce-00-00-efgh',
            keyspace: 'commerce',
            shard: '-',
            type: topodata.TabletType.REPLICA,
        },
        state: pb.Tablet.ServingState.SERVING,
    },
    {
        cluster: { id: 'prod', name: 'prod' },
        tablet: {
            hostname: 'tablet-prod-commerce-00-00-ijkl',
            keyspace: 'commerce',
            shard: '-',
            type: topodata.TabletType.REPLICA,
        },
        state: pb.Tablet.ServingState.SERVING,
    },
    {
        cluster: { id: 'dev', name: 'dev' },
        tablet: {
            hostname: 'tablet-dev-commerce-00-00-mnop',
            keyspace: 'commerce',
            shard: '-',
            type: topodata.TabletType.MASTER,
        },
        state: pb.Tablet.ServingState.SERVING,
    },
    {
        cluster: { id: 'dev', name: 'dev' },
        tablet: {
            hostname: 'tablet-dev-commerce-00-00-qrst',
            keyspace: 'commerce',
            shard: '-',
            type: topodata.TabletType.REPLICA,
        },
        state: pb.Tablet.ServingState.SERVING,
    },
    {
        cluster: { id: 'dev', name: 'dev' },
        tablet: {
            hostname: 'tablet-dev-commerce-00-00-uvwx',
            keyspace: 'commerce',
            shard: '-',
            type: topodata.TabletType.REPLICA,
        },
        state: pb.Tablet.ServingState.SERVING,
    },
].map((opts) => pb.Tablet.create(opts));
