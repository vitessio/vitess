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
import { vtctldata, vtadmin as pb } from '../proto/vtadmin';
import { formatAlias } from './tablets';

/**
 * getStreams returns a flat list of streams across all keyspaces/shards in the workflow.
 */
export const getStreams = <W extends pb.IWorkflow>(workflow: W | null | undefined): vtctldata.Workflow.IStream[] => {
    if (!workflow) {
        return [];
    }

    return Object.values(workflow.workflow?.shard_streams || {}).reduce((acc, shardStream) => {
        (shardStream.streams || []).forEach((stream) => {
            acc.push(stream);
        });
        return acc;
    }, [] as vtctldata.Workflow.IStream[]);
};

export const formatStreamKey = <S extends vtctldata.Workflow.IStream>(stream: S | null | undefined): string | null => {
    return stream?.tablet && stream?.id ? `${formatAlias(stream.tablet)}/${stream.id}` : null;
};

export const getStreamSource = <S extends vtctldata.Workflow.IStream>(stream: S | null | undefined): string | null => {
    return stream?.binlog_source?.keyspace && stream?.binlog_source.shard
        ? `${stream.binlog_source.keyspace}/${stream.binlog_source.shard}`
        : null;
};

export const getStreamTarget = <S extends vtctldata.Workflow.IStream>(
    stream: S | null | undefined,
    workflowKeyspace: string | null | undefined
): string | null => {
    return stream?.shard && workflowKeyspace ? `${workflowKeyspace}/${stream.shard}` : null;
};

/**
 * getTimeUpdated returns the `time_updated` timestamp of the most recently
 * updated stream in the workflow.
 */
export const getTimeUpdated = <W extends pb.IWorkflow>(workflow: W | null | undefined): number => {
    // Note: long-term it may be better to get this from the `vreplication_log` data
    // added by https://github.com/vitessio/vitess/pull/7831
    const timestamps = getStreams(workflow).map((s) => parseInt(`${s.time_updated?.seconds}`, 10));
    return Math.max(...timestamps);
};
