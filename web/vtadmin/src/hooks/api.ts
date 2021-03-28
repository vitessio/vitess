import { useQuery, useQueryClient, UseQueryOptions } from 'react-query';
import {
    fetchClusters,
    fetchGates,
    fetchKeyspaces,
    fetchSchema,
    FetchSchemaParams,
    fetchSchemas,
    fetchTablets,
} from '../api/http';
import { vtadmin as pb } from '../proto/vtadmin';

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

export interface TableDefinition {
    cluster?: pb.Schema['cluster'];
    keyspace?: pb.Schema['keyspace'];
    // The [0] index is a typescript quirk to infer the type of
    // an entry in an array, and therefore the type of ALL entries
    // in the array (not just the first one).
    tableDefinition?: pb.Schema['table_definitions'][0];
}

/**
 * useTableDefinitions is a helper hook for when a flattened list
 * of table definitions (across all keyspaces and clusters) is required,
 * instead of the default vtadmin-api/Vitess grouping of schemas by keyspace.
 *
 * Under the hood, this calls the useSchemas hook and therefore uses
 * the same query cache.
 */
export const useTableDefinitions = (...args: Parameters<typeof useSchemas>) => {
    const { data, ...query } = useSchemas(...args);

    if (!Array.isArray(data)) {
        return { data, ...query };
    }

    const tds = data.reduce((acc: TableDefinition[], schema: pb.Schema) => {
        (schema.table_definitions || []).forEach((td) => {
            acc.push({
                cluster: schema.cluster,
                keyspace: schema.keyspace,
                tableDefinition: td,
            });
        });
        return acc;
    }, []);

    return { ...query, data: tds };
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
