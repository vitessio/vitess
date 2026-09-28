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
        s.shard?.is_primary_serving ? ShardState.serving : ShardState.nonserving
    );

    // Add exhaustive enum keys (since groupBy only returns a dictionary), as well as define defaults
    return {
        [ShardState.serving]: grouped.serving || [],
        [ShardState.nonserving]: grouped.nonserving || [],
    };
};

export interface ShardRange {
    start: number;
    end: number;
}

/**
 * getShardSortRange returns the start and end described by the shard name,
 * in base 10. Ranges at the start/end of the shard range are parsed as
 * minimum/maximum integer values. Useful for sorting shard names numerically.
 */
export const getShardSortRange = (shardName: string): ShardRange => {
    if (shardName === '0' || shardName === '-') {
        return {
            start: Number.MIN_VALUE,
            end: Number.MAX_VALUE,
        };
    }

    const parsed = shardName.split('-');
    if (parsed.length !== 2) {
        throw Error(`could not parse sortable range from shard ${shardName}`);
    }

    // Parse the hexadecimal values into base 10 integers, and normalize the
    // start and end of the ranges.
    const start = parsed[0] === '' ? Number.MIN_VALUE : parseInt(parsed[0], 16);
    const end = parsed[1] === '' ? Number.MAX_VALUE : parseInt(parsed[1], 16);

    if (isNaN(start) || isNaN(end)) {
        throw Error(`could not parse sortable range from shard ${shardName}`);
    }

    return { start, end };
};
