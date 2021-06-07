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
import { groupBy } from 'lodash-es';
import { vtadmin as pb, vtctldata } from '../proto/vtadmin';

export enum ShardState {
    serving = 'serving',
    nonserving = 'nonserving',
}

export type ShardsByState = { [k in ShardState]: vtctldata.IShard[] };

export const getShardsByState = <K extends pb.IKeyspace>(keyspace: K | null | undefined): ShardsByState => {
    const grouped = groupBy(Object.values(keyspace?.shards || {}), (s) =>
        s.shard?.is_master_serving ? ShardState.serving : ShardState.nonserving
    );

    // Add exhaustive enum keys (since groupBy only returns a dictionary), as well as define defaults
    return {
        [ShardState.serving]: grouped.serving || [],
        [ShardState.nonserving]: grouped.nonserving || [],
    };
};
