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
import {
    useMutation,
    UseMutationOptions,
    useQueries,
    useQuery,
    useQueryClient,
    UseQueryOptions,
    UseQueryResult,
} from 'react-query';
import {
    fetchBackups,
    fetchClusters,
    fetchExperimentalTabletDebugVars,
    fetchGates,
    fetchKeyspace,
    fetchKeyspaces,
    fetchSchema,
    FetchSchemaParams,
    fetchSchemas,
    fetchTablet,
    FetchTabletParams,
    pingTablet,
    fetchTablets,
    fetchVSchema,
    FetchVSchemaParams,
    fetchVtctlds,
    fetchVTExplain,
    fetchWorkflow,
    fetchWorkflows,
    TabletDebugVarsResponse,
    refreshState,
    runHealthCheck,
    deleteTablet,
    refreshTabletReplicationSource,
    startReplication,
    stopReplication,
    setReadOnly,
    setReadWrite,
    ValidateKeyspaceParams,
    validateKeyspace,
    validateSchemaKeyspace,
    ValidateSchemaKeyspaceParams,
    ValidateVersionKeyspaceParams,
    validateVersionKeyspace,
    fetchShardReplicationPositions,
    createKeyspace,
    reloadSchema,
    deleteShard,
    reloadSchemaShard,
    tabletExternallyPromoted,
    plannedFailoverShard,
    emergencyFailoverShard,
    rebuildKeyspaceGraph,
    removeKeyspaceCell,
    createShard,
    validate,
    ValidateParams,
    validateShard,
    ValidateShardParams,
    getFullStatus,
    GetFullStatusParams,
} from '../api/http';
import { vtadmin as pb, vtctldata } from '../proto/vtadmin';
import { formatAlias } from '../util/tablets';

/**
 * useBackups is a query hook that fetches all backups across every cluster.
 */
export const useBackups = (options?: UseQueryOptions<pb.ClusterBackup[], Error> | undefined) =>
    useQuery(['backups'], fetchBackups, options);

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
 * useKeyspace is a query hook that fetches a single keyspace by name.
 */
export const useKeyspace = (
    params: Parameters<typeof fetchKeyspace>[0],
    options?: UseQueryOptions<pb.Keyspace, Error>
) => {
    const queryClient = useQueryClient();
    return useQuery(['keyspace', params], () => fetchKeyspace(params), {
        initialData: () => {
            const keyspaces = queryClient.getQueryData<pb.Keyspace[]>('keyspaces');
            return (keyspaces || []).find(
                (k) => k.cluster?.id === params.clusterID && k.keyspace?.name === params.name
            );
        },
        ...options,
    });
};

/**
 * useCreateKeyspace is a mutation query hook that creates a keyspace.
 */
export const useCreateKeyspace = (
    params: Parameters<typeof createKeyspace>[0],
    options: UseMutationOptions<Awaited<ReturnType<typeof createKeyspace>>, Error>
) => {
    return useMutation<Awaited<ReturnType<typeof createKeyspace>>, Error>(() => {
        return createKeyspace(params);
    }, options);
};

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
 * useVtctlds is a query hook that fetches all vtctlds across every cluster.
 */
export const useVtctlds = (options?: UseQueryOptions<pb.Vtctld[], Error> | undefined) =>
    useQuery(['vtctlds'], fetchVtctlds, options);

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

/**
 *
 * useDeleteTablet is a mutate hook that deletes a tablet by alias and optionally, cluster id.
 */
export const useDeleteTablet = (
    params: Parameters<typeof deleteTablet>[0],
    options: UseMutationOptions<Awaited<ReturnType<typeof deleteTablet>>, Error>
) => {
    return useMutation<Awaited<ReturnType<typeof deleteTablet>>, Error>(() => {
        return deleteTablet(params);
    }, options);
};

/**
 * useRefreshTabletReplicationSource performs a `CHANGE REPLICATION SOURCE TO`
 * on a tablet to replicate from the current primary in the shard.
 */
export const useRefreshTabletReplicationSource = (
    params: Parameters<typeof refreshTabletReplicationSource>[0],
    options: UseMutationOptions<Awaited<ReturnType<typeof refreshTabletReplicationSource>>, Error>
) => {
    return useMutation<Awaited<ReturnType<typeof refreshTabletReplicationSource>>, Error>(() => {
        return refreshTabletReplicationSource(params);
    }, options);
};

/**
 * useSetReadOnly sets the tablet to read only
 */
export const useSetReadOnly = (
    params: Parameters<typeof setReadOnly>[0],
    options: UseMutationOptions<Awaited<ReturnType<typeof setReadOnly>>, Error>
) => {
    return useMutation<Awaited<ReturnType<typeof setReadOnly>>, Error>(() => {
        return setReadOnly(params);
    }, options);
};

/**
 * useSetReadWrite sets the tablet to read only
 */
export const useSetReadWrite = (
    params: Parameters<typeof setReadWrite>[0],
    options: UseMutationOptions<Awaited<ReturnType<typeof setReadWrite>>, Error>
) => {
    return useMutation<Awaited<ReturnType<typeof setReadWrite>>, Error>(() => {
        return setReadWrite(params);
    }, options);
};

/**
 * useShardReplicationPositions is a query hook that shows the replication status
 * of each replica machine in the shard graph.
 */
export const useShardReplicationPositions = (
    params: Parameters<typeof fetchShardReplicationPositions>[0],
    options?: UseQueryOptions<pb.GetShardReplicationPositionsResponse, Error> | undefined
) => useQuery(['shard_replication_positions', params], () => fetchShardReplicationPositions(params), options);

/**
 * useStartReplication starts replication on the specified tablet.
 */
export const useStartReplication = (
    params: Parameters<typeof startReplication>[0],
    options: UseMutationOptions<Awaited<ReturnType<typeof startReplication>>, Error>
) => {
    return useMutation<Awaited<ReturnType<typeof startReplication>>, Error>(() => {
        return startReplication(params);
    }, options);
};

/**
 * useStopReplication stops replication on the specified tablet.
 */
export const useStopReplication = (
    params: Parameters<typeof stopReplication>[0],
    options: UseMutationOptions<Awaited<ReturnType<typeof stopReplication>>, Error>
) => {
    return useMutation<Awaited<ReturnType<typeof stopReplication>>, Error>(() => {
        return stopReplication(params);
    }, options);
};

/**
 * usePingTablet is a query hook that pings a single tablet by tablet alias and (optionally) cluster id.
 */
export const usePingTablet = (
    params: Parameters<typeof pingTablet>[0],
    options?: UseQueryOptions<pb.PingTabletResponse, Error>
) => {
    return useQuery(['ping-tablet', params], () => pingTablet(params), options);
};

/**
 * useRefreshState is a query hook that reloads the tablet record on the specified tablet.
 */
export const useRefreshState = (
    params: Parameters<typeof refreshState>[0],
    options?: UseQueryOptions<pb.RefreshStateResponse, Error>
) => {
    return useQuery(['refresh-state', params], () => refreshState(params), options);
};

/**
 * useRefreshState is a query hook that reloads the tablet record on the specified tablet.
 */
export const useHealthCheck = (
    params: Parameters<typeof runHealthCheck>[0],
    options?: UseQueryOptions<pb.RunHealthCheckResponse, Error>
) => {
    return useQuery(['run-health-check', params], () => runHealthCheck(params), options);
};

export const useExperimentalTabletDebugVars = (
    params: FetchTabletParams,
    options?: UseQueryOptions<TabletDebugVarsResponse, Error>
) => {
    return useQuery(
        ['experimental/tablet/debug/vars', params],
        () => fetchExperimentalTabletDebugVars(params),
        options
    );
};

// Future enhancement: add vtadmin-api endpoint to fetch /debug/vars
// for multiple tablets in a single request. https://github.com/vitessio/vitess/projects/12#card-63086674
export const useManyExperimentalTabletDebugVars = (
    params: FetchTabletParams[],
    defaultOptions: UseQueryOptions<TabletDebugVarsResponse, Error> = {}
) => {
    // Robust typing for useQueries is still in progress, so we do
    // some sneaky type-casting. See https://github.com/tannerlinsley/react-query/issues/1675
    const queries = params.map((p) => ({
        queryKey: ['experimental/tablet/debug/vars', p],
        queryFn: () => fetchExperimentalTabletDebugVars(p),
        ...(defaultOptions as any),
    }));
    return useQueries(queries) as UseQueryResult<TabletDebugVarsResponse, Error>[];
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
 * useValidateKeyspace is a query hook that validates that all nodes reachable from the specified keyspace are consistent.
 */
export const useValidateKeyspace = (
    params: ValidateKeyspaceParams,
    options?: UseMutationOptions<Awaited<ReturnType<typeof validateKeyspace>>, Error>
) => {
    return useMutation<Awaited<ReturnType<typeof validateKeyspace>>, Error>(() => {
        return validateKeyspace(params);
    }, options);
};

/**
 * useValidateKeyspace is a query hook that validates that all nodes reachable from the specified keyspace are consistent.
 */
export const useValidateSchemaKeyspace = (
    params: ValidateSchemaKeyspaceParams,
    options?: UseMutationOptions<Awaited<ReturnType<typeof validateSchemaKeyspace>>, Error>
) => {
    return useMutation<Awaited<ReturnType<typeof validateSchemaKeyspace>>, Error>(() => {
        return validateSchemaKeyspace(params);
    }, options);
};

/**
 * useValidateVersion is a query hook that validates that all nodes reachable from the specified keyspace are consistent.
 */
export const useValidateVersionKeyspace = (
    params: ValidateVersionKeyspaceParams,
    options?: UseMutationOptions<Awaited<ReturnType<typeof validateVersionKeyspace>>, Error>
) => {
    return useMutation<Awaited<ReturnType<typeof validateVersionKeyspace>>, Error>(() => {
        return validateVersionKeyspace(params);
    }, options);
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

/**
 * useReloadSchema is a mutate hook that reloads schemas in one or more
 * keyspaces, shards, or tablets in the cluster, depending on the request parameters.
 */
export const useReloadSchema = (
    params: Parameters<typeof reloadSchema>[0],
    options?: UseMutationOptions<Awaited<ReturnType<typeof reloadSchema>>, Error>
) => {
    return useMutation<Awaited<ReturnType<typeof reloadSchema>>, Error>(() => {
        return reloadSchema(params);
    }, options);
};

/**
 * useDeleteShard is a mutate hook that deletes a shard in a keyspace.
 */
export const useDeleteShard = (
    params: Parameters<typeof deleteShard>[0],
    options?: UseMutationOptions<Awaited<ReturnType<typeof deleteShard>>, Error>
) => {
    return useMutation<Awaited<ReturnType<typeof deleteShard>>, Error>(() => {
        return deleteShard(params);
    }, options);
};

/*
 * useRebuildKeyspaceGraph is a mutate hook that rebuilds keyspace graphs for one or
 * more cells in a keyspace.
 */
export const useRebuildKeyspaceGraph = (
    params: Parameters<typeof rebuildKeyspaceGraph>[0],
    options?: UseMutationOptions<Awaited<ReturnType<typeof rebuildKeyspaceGraph>>, Error>
) => {
    return useMutation<Awaited<ReturnType<typeof rebuildKeyspaceGraph>>, Error>(() => {
        return rebuildKeyspaceGraph(params);
    }, options);
};

/**
 * useReloadSchemaShard is a mutate hook that reloads the schema on all tablets in a shard. This is done on a best-effort basis.
 */
export const useReloadSchemaShard = (
    params: Parameters<typeof reloadSchemaShard>[0],
    options?: UseMutationOptions<Awaited<ReturnType<typeof reloadSchemaShard>>, Error>
) => {
    return useMutation<Awaited<ReturnType<typeof reloadSchemaShard>>, Error>(() => {
        return reloadSchemaShard(params);
    }, options);
};

/**
 * useTabletExternallyPromoted is a mutate hook that changes metadata in the topology server to
 * acknowledge a shard primary change performed by an external tool (e.g.
 * orchestrator).
 */
export const useTabletExternallyPromoted = (
    params: Parameters<typeof tabletExternallyPromoted>[0],
    options?: UseMutationOptions<Awaited<ReturnType<typeof tabletExternallyPromoted>>, Error>
) => {
    return useMutation<Awaited<ReturnType<typeof tabletExternallyPromoted>>, Error>(() => {
        return tabletExternallyPromoted(params);
    }, options);
};

/**
 * usePlannedFailoverShard reparents the shard to a new primary that can either be explicitly specified, or chosen by Vitess.
 * This calls PlannedReparentShard in vtctlservice
 * See https://vitess.io/docs/reference/programs/vtctl/shards/#plannedreparentshard
 */
export const usePlannedFailoverShard = (
    params: Parameters<typeof plannedFailoverShard>[0],
    options?: UseMutationOptions<Awaited<ReturnType<typeof plannedFailoverShard>>, Error>
) => {
    return useMutation<Awaited<ReturnType<typeof plannedFailoverShard>>, Error>(() => {
        return plannedFailoverShard(params);
    }, options);
};

/**
 * useEmergencyFailoverShard reparents the shard to the new primary. Assumes the old primary is dead and not responding.
 * This calls EmergencyReparentShard in vtctlservice
 * See https://vitess.io/docs/reference/programs/vtctl/shards/#emergencyreparentshard
 */
export const useEmergencyFailoverShard = (
    params: Parameters<typeof emergencyFailoverShard>[0],
    options?: UseMutationOptions<Awaited<ReturnType<typeof emergencyFailoverShard>>, Error>
) => {
    return useMutation<Awaited<ReturnType<typeof emergencyFailoverShard>>, Error>(() => {
        return emergencyFailoverShard(params);
    }, options);
};

/**
 * useRemoveKeyspaceCell is a mutate hook that removes a keyspace cell from the Cells list for all shards in the keyspace, and the SrvKeyspace for that keyspace in that cell.
 */
export const useRemoveKeyspaceCell = (
    params: Parameters<typeof removeKeyspaceCell>[0],
    options?: UseMutationOptions<Awaited<ReturnType<typeof removeKeyspaceCell>>, Error>
) => {
    return useMutation<Awaited<ReturnType<typeof removeKeyspaceCell>>, Error>(() => {
        return removeKeyspaceCell(params);
    }, options);
};

/**
 * useCreateShard is a mutate hook that creates a shard in a keyspace
 */
export const useCreateShard = (
    params: Parameters<typeof createShard>[0],
    options?: UseMutationOptions<Awaited<ReturnType<typeof createShard>>, Error>
) => {
    return useMutation<Awaited<ReturnType<typeof createShard>>, Error>(() => {
        return createShard(params);
    }, options);
};

/**
 * useValidate is a mutate hook that validates that all nodes reachable from the global replication graph,
 * as well as all tablets in discoverable cells, are consistent.
 */
export const useValidate = (
    params: Parameters<typeof validate>[0],
    options?: UseMutationOptions<Awaited<ReturnType<typeof validate>>, Error, ValidateParams>
) => {
    return useMutation<Awaited<ReturnType<typeof validate>>, Error, ValidateParams>(() => {
        return validate(params);
    }, options);
};

/**
 * useValidate is a mutate hook that validates that all nodes reachable from the global replication graph,
 * as well as all tablets in discoverable cells, are consistent.
 */
export const useValidateShard = (
    params: Parameters<typeof validateShard>[0],
    options?: UseMutationOptions<Awaited<ReturnType<typeof validateShard>>, Error, ValidateShardParams>
) => {
    return useMutation<Awaited<ReturnType<typeof validateShard>>, Error, ValidateShardParams>(() => {
        return validateShard(params);
    }, options);
};

/**
 * useGetFullStatus is a query hook that fetches the full status of a tablet
 */
export const useGetFullStatus = (
    params: GetFullStatusParams,
    options?: UseQueryOptions<vtctldata.GetFullStatusResponse, Error> | undefined
) => useQuery(['full-status', params], () => getFullStatus(params), options);
