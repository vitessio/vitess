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
import { useQuery, useQueryClient, UseQueryOptions } from 'react-query';
import {
    fetchClusters,
    fetchExperimentalTabletDebugVars,
    fetchGates,
    fetchKeyspaces,
    fetchSchema,
    FetchSchemaParams,
    fetchSchemas,
    fetchTablet,
    fetchTablets,
    fetchVSchema,
    FetchVSchemaParams,
    fetchVTExplain,
    fetchWorkflow,
    fetchWorkflows,
} from '../api/http';
import { vtadmin as pb } from '../proto/vtadmin';
import { TabletDebugVars } from '../util/tabletDebugVars';
import { formatAlias } from '../util/tablets';

/**
 * useClusters is a query hook that fetches all clusters VTAdmin is configured to discover.
 */
export const useClusters = (options?: UseQueryOptions<pb.Cluster[], Error> | undefined) =>
    useQuery(['clusters'], fetchClusters, options);

/**
 * useGates is a query hook that fetches all VTGates across every cluster.
 */
export const useGates = (options?: UseQueryOptions<pb.VTGate[], Error> | undefined) =>
    useQuery(['gates'], fetchGates, options);

/**
 * useKeyspaces is a query hook that fetches all keyspaces across every cluster.
 */
export const useKeyspaces = (options?: UseQueryOptions<pb.Keyspace[], Error> | undefined) =>
    useQuery(['keyspaces'], fetchKeyspaces, options);

/**
 * useSchemas is a query hook that fetches all schemas across every cluster.
 */
export const useSchemas = (options?: UseQueryOptions<pb.Schema[], Error> | undefined) =>
    useQuery(['schemas'], fetchSchemas, options);

/**
 * useTablets is a query hook that fetches all tablets across every cluster.
 */
export const useTablets = (options?: UseQueryOptions<pb.Tablet[], Error> | undefined) =>
    useQuery(['tablets'], fetchTablets, options);

/**
 * useTablet is a query hook that fetches a single tablet by alias.
 */
export const useTablet = (params: Parameters<typeof fetchTablet>[0], options?: UseQueryOptions<pb.Tablet, Error>) => {
    const queryClient = useQueryClient();
    return useQuery(['tablet', params], () => fetchTablet(params), {
        initialData: () => {
            const tablets = queryClient.getQueryData<pb.Tablet[]>('tablets');
            return (tablets || []).find(
                (t) => t.cluster?.id === params.clusterID && formatAlias(t.tablet?.alias) === params.alias
            );
        },
        ...options,
    });
};

export const useExperimentalTabletDebugVars = (
    params: Parameters<typeof fetchExperimentalTabletDebugVars>[0],
    options?: UseQueryOptions<TabletDebugVars, Error>
) => {
    return useQuery(
        ['experimental/tablet/debug/vars', params],
        () => fetchExperimentalTabletDebugVars(params),
        options
    );
};

/**
 * useWorkflowsResponse is a query hook that fetches all workflows (by cluster) across every cluster.
 */
export const useWorkflowsResponse = (options?: UseQueryOptions<pb.GetWorkflowsResponse, Error> | undefined) =>
    useQuery(['workflows'], fetchWorkflows, options);

/**
 * useWorkflows is a helper hook for when a flattened list of workflows
 * (across all clusters) is required. Under the hood, this call uses the
 * useWorkflowsResponse hook and therefore uses the same query cache.
 */
export const useWorkflows = (...args: Parameters<typeof useWorkflowsResponse>) => {
    const { data, ...query } = useWorkflowsResponse(...args);

    if (!data?.workflows_by_cluster) {
        return { data: undefined, ...query };
    }

    const workflows = Object.entries(data.workflows_by_cluster).reduce(
        (acc: pb.Workflow[], [clusterID, { workflows }]) => {
            (workflows || []).forEach((w) => acc.push(pb.Workflow.create(w)));
            return acc;
        },
        []
    );

    return { data: workflows, ...query };
};

/**
 * useSchema is a query hook that fetches a single schema for the given parameters.
 */
export const useSchema = (params: FetchSchemaParams, options?: UseQueryOptions<pb.Schema, Error> | undefined) => {
    const queryClient = useQueryClient();
    return useQuery(['schema', params], () => fetchSchema(params), {
        initialData: () => {
            const schemas = queryClient.getQueryData<pb.Schema[]>('schemas');
            return (schemas || []).find(
                (s: pb.Schema) =>
                    s.cluster?.id === params.clusterID &&
                    s.keyspace === params.keyspace &&
                    s.table_definitions.find((td) => td.name === params.table)
            );
        },
        ...options,
    });
};

/**
 * useVSchema is a query hook that fetches a single vschema definition for the given parameters.
 */
export const useVSchema = (params: FetchVSchemaParams, options?: UseQueryOptions<pb.VSchema, Error> | undefined) => {
    return useQuery(['vschema', params], () => fetchVSchema(params));
};

export const useVTExplain = (
    params: Parameters<typeof fetchVTExplain>[0],
    options?: UseQueryOptions<pb.VTExplainResponse, Error> | undefined
) => {
    return useQuery(['vtexplain', params], () => fetchVTExplain(params), { ...options });
};

/**
 * useWorkflow is a query hook that fetches a single workflow for the given parameters.
 */
export const useWorkflow = (
    params: Parameters<typeof fetchWorkflow>[0],
    options?: UseQueryOptions<pb.Workflow, Error> | undefined
) => {
    const queryClient = useQueryClient();
    return useQuery(['workflow', params], () => fetchWorkflow(params), {
        // If the workflow already exists in the cache from a previous fetchWorkflows call,
        // then use that for the initial data.
        //
        // Important note: `initialData` is persisted to the query cache. If the shapes of the GetWorkflowsResponse
        // and Workflow protobuf types ever change such that Workflow is not a subset of GetWorkflowsResponse
        // (e.g., the /api/workflow/... route provides different information than the /api/workflows route)
        // then instead we will want to use `placeholderData`. (Unfortunately, the HTTP request boundary
        // is one area where we have to assume typesafety... until we can, perhaps, one day switch to using
        // gRPC on the client. Or, we could investigate code generating a TypeScript HTTP client. Possibilities!)
        //
        // See https://react-query.tanstack.com/guides/initial-query-data for more context on how initialData works.
        initialData: () => {
            const workflows = queryClient.getQueryData<pb.GetWorkflowsResponse>('workflows');
            const cw = workflows?.workflows_by_cluster[params.clusterID];
            if (!cw) return undefined;

            const workflow = (cw.workflows || []).find(
                (w) =>
                    w.cluster?.id === params.clusterID &&
                    w.keyspace === params.keyspace &&
                    w.workflow?.name === params.name
            );

            if (!workflow) return undefined;

            return pb.Workflow.create(workflow);
        },
        ...options,
    });
};
