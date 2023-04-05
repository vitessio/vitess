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

import { vtadmin as pb, vtadmin, vtctldata } from '../proto/vtadmin';
import * as errorHandler from '../errors/errorHandler';
import { HttpFetchError, HttpResponseNotOkError, MalformedHttpResponseError } from '../errors/errorTypes';
import { HttpOkResponse } from './responseTypes';
import { TabletDebugVars } from '../util/tabletDebugVars';
import { env, isReadOnlyMode } from '../util/env';

/**
 * vtfetch makes HTTP requests against the given vtadmin-api endpoint
 * and returns the parsed response.
 *
 * HttpResponse envelope types are not defined in vtadmin.proto (nor should they be)
 * thus we have to validate the shape of the API response with more care.
 *
 * Note that this only validates the HttpResponse envelope; it does not
 * do any type checking or validation on the result.
 */
export const vtfetch = async (endpoint: string, options: RequestInit = {}): Promise<HttpOkResponse> => {
    try {
        if (isReadOnlyMode() && options.method && options.method.toLowerCase() !== 'get') {
            // Any UI controls that ultimately trigger a write request should be hidden when in read-only mode,
            // so getting to this point (where we actually execute a write request) is an error.
            // So: we fail obnoxiously, as failing silently (e.g, logging and returning an empty "ok" response)
            // could imply to the user that a write action succeeded.
            throw new Error(`Cannot execute write request in read-only mode: ${options.method} ${endpoint}`);
        }

        const url = `${env().VITE_VTADMIN_API_ADDRESS}${endpoint}`;
        const opts = { ...vtfetchOpts(), ...options };

        let response = null;
        try {
            response = await global.fetch(url, opts);
        } catch (error) {
            // Capture fetch() promise rejections and rethrow as HttpFetchError.
            // fetch() promises will reject with a TypeError when a network error is
            // encountered or CORS is misconfigured, in which case the request never
            // makes it to the server.
            // See https://developer.mozilla.org/en-US/docs/Web/API/Fetch_API/Using_Fetch#checking_that_the_fetch_was_successful
            throw new HttpFetchError(url);
        }

        let json = null;
        try {
            json = await response.json();
        } catch (error) {
            throw new MalformedHttpResponseError((error as Error).message, endpoint, json, response);
        }

        if (!('ok' in json)) {
            throw new MalformedHttpResponseError('invalid HTTP envelope', endpoint, json, response);
        }

        if (!json.ok) {
            throw new HttpResponseNotOkError(endpoint, json, response);
        }

        return json as HttpOkResponse;
    } catch (error) {
        // Most commonly, react-query is the downstream consumer of
        // errors thrown in vtfetch. Because react-query "handles" errors
        // by propagating them to components (as it should!), any errors thrown
        // from vtfetch are _not_ automatically logged as "unhandled errors".
        // Instead, we catch errors and manually notify our error handling serivce(s),
        // and then rethrow the error for react-query to propagate the usual way.
        // See https://react-query.tanstack.com/guides/query-functions#handling-and-throwing-errors
        errorHandler.notify(error as Error);
        throw error;
    }
};

export const vtfetchOpts = (): RequestInit => {
    const credentials = env().VITE_FETCH_CREDENTIALS;
    if (credentials && credentials !== 'omit' && credentials !== 'same-origin' && credentials !== 'include') {
        throw Error(
            `Invalid fetch credentials property: ${credentials}. Must be undefined or one of omit, same-origin, include`
        );
    }

    return { credentials };
};

// vtfetchEntities is a helper function for querying vtadmin-api endpoints
// that return a list of protobuf entities.
export const vtfetchEntities = async <T>(opts: {
    endpoint: string;
    // Extract the list of entities from the response. We can't (strictly)
    // guarantee type safety for API responses, hence the `any` return type.
    extract: (res: HttpOkResponse) => any;
    // Transform an individual entity in the array to its (proto)typed form.
    // This will almost always be a `.verify` followed by a `.create`,
    // but because of how protobufjs structures its generated types,
    // writing this in a generic way is... unpleasant, and difficult to read.
    transform: (e: object) => T;
}): Promise<T[]> => {
    const res = await vtfetch(opts.endpoint);

    const entities = opts.extract(res);
    if (!Array.isArray(entities)) {
        // Since react-query is the downstream consumer of vtfetch + vtfetchEntities,
        // errors thrown in either function will be "handled" and will not automatically
        // propagate as "unhandled" errors, meaning we have to log them manually.
        const error = Error(`expected entities to be an array, got ${entities}`);
        errorHandler.notify(error);
        throw error;
    }

    return entities.map(opts.transform);
};

export const fetchBackups = async () =>
    vtfetchEntities({
        endpoint: '/api/backups',
        extract: (res) => res.result.backups,
        transform: (e) => {
            const err = pb.ClusterBackup.verify(e);
            if (err) throw Error(err);
            return pb.ClusterBackup.create(e);
        },
    });

export const fetchClusters = async () =>
    vtfetchEntities({
        endpoint: '/api/clusters',
        extract: (res) => res.result.clusters,
        transform: (e) => {
            const err = pb.Cluster.verify(e);
            if (err) throw Error(err);
            return pb.Cluster.create(e);
        },
    });

export const fetchGates = async () =>
    vtfetchEntities({
        endpoint: '/api/gates',
        extract: (res) => res.result.gates,
        transform: (e) => {
            const err = pb.VTGate.verify(e);
            if (err) throw Error(err);
            return pb.VTGate.create(e);
        },
    });

export const fetchVtctlds = async () =>
    vtfetchEntities({
        endpoint: '/api/vtctlds',
        extract: (res) => res.result.vtctlds,
        transform: (e) => {
            const err = pb.Vtctld.verify(e);
            if (err) throw Error(err);
            return pb.Vtctld.create(e);
        },
    });

export interface FetchKeyspaceParams {
    clusterID: string;
    name: string;
}

export const fetchKeyspace = async ({ clusterID, name }: FetchKeyspaceParams) => {
    const { result } = await vtfetch(`/api/keyspace/${clusterID}/${name}`);

    const err = pb.Keyspace.verify(result);
    if (err) throw Error(err);

    return pb.Keyspace.create(result);
};

export const fetchKeyspaces = async () =>
    vtfetchEntities({
        endpoint: '/api/keyspaces',
        extract: (res) => res.result.keyspaces,
        transform: (e) => {
            const err = pb.Keyspace.verify(e);
            if (err) throw Error(err);
            return pb.Keyspace.create(e);
        },
    });

export interface CreateKeyspaceParams {
    clusterID: string;
    options: vtctldata.ICreateKeyspaceRequest;
}

export const createKeyspace = async (params: CreateKeyspaceParams) => {
    const { result } = await vtfetch(`/api/keyspace/${params.clusterID}`, {
        body: JSON.stringify(params.options),
        method: 'post',
    });

    const err = pb.CreateKeyspaceResponse.verify(result);
    if (err) throw Error(err);

    return pb.CreateKeyspaceResponse.create(result);
};

export const fetchSchemas = async () =>
    vtfetchEntities({
        endpoint: '/api/schemas',
        extract: (res) => res.result.schemas,
        transform: (e) => {
            const err = pb.Schema.verify(e);
            if (err) throw Error(err);
            return pb.Schema.create(e);
        },
    });

export interface FetchSchemaParams {
    clusterID: string;
    keyspace: string;
    table: string;
}

export const fetchSchema = async ({ clusterID, keyspace, table }: FetchSchemaParams) => {
    const { result } = await vtfetch(`/api/schema/${clusterID}/${keyspace}/${table}`);

    const err = pb.Schema.verify(result);
    if (err) throw Error(err);

    return pb.Schema.create(result);
};

export interface FetchTabletParams {
    clusterID: string;
    alias: string;
}

export const fetchTablet = async ({ clusterID, alias }: FetchTabletParams) => {
    const { result } = await vtfetch(`/api/tablet/${alias}?cluster=${clusterID}`);

    const err = pb.Tablet.verify(result);
    if (err) throw Error(err);

    return pb.Tablet.create(result);
};

export interface DeleteTabletParams {
    allowPrimary?: boolean;
    clusterID: string;
    alias: string;
}

export const deleteTablet = async ({ allowPrimary, clusterID, alias }: DeleteTabletParams) => {
    const req = new URLSearchParams();
    req.append('cluster', clusterID);

    // Do not append `allow_primary` if undefined in order to fall back to server default
    if (typeof allowPrimary === 'boolean') {
        req.append('allow_primary', allowPrimary.toString());
    }

    const { result } = await vtfetch(`/api/tablet/${alias}?${req}`, { method: 'delete' });

    const err = pb.DeleteTabletResponse.verify(result);
    if (err) throw Error(err);

    return pb.DeleteTabletResponse.create(result);
};

export interface RefreshTabletReplicationSourceParams {
    clusterID: string;
    alias: string;
}

export const refreshTabletReplicationSource = async ({ clusterID, alias }: RefreshTabletReplicationSourceParams) => {
    const { result } = await vtfetch(`/api/tablet/${alias}/refresh_replication_source`, { method: 'put' });

    const err = pb.RefreshTabletReplicationSourceResponse.verify(result);
    if (err) throw Error(err);

    return pb.RefreshTabletReplicationSourceResponse.create(result);
};

export interface PingTabletParams {
    clusterID?: string;
    alias: string;
}

export const pingTablet = async ({ clusterID, alias }: PingTabletParams) => {
    const { result } = await vtfetch(`/api/tablet/${alias}/ping?cluster=${clusterID}`);
    const err = pb.PingTabletResponse.verify(result);
    if (err) throw Error(err);

    return pb.PingTabletResponse.create(result);
};

export interface RefreshStateParams {
    clusterID?: string;
    alias: string;
}

export const refreshState = async ({ clusterID, alias }: RefreshStateParams) => {
    const { result } = await vtfetch(`/api/tablet/${alias}/refresh?cluster=${clusterID}`, { method: 'put' });
    const err = pb.RefreshStateResponse.verify(result);
    if (err) throw Error(err);

    return pb.RefreshStateResponse.create(result);
};

export interface RunHealthCheckParams {
    clusterID?: string;
    alias: string;
}

export const runHealthCheck = async ({ clusterID, alias }: RunHealthCheckParams) => {
    const { result } = await vtfetch(`/api/tablet/${alias}/healthcheck?cluster=${clusterID}`);
    const err = pb.RunHealthCheckResponse.verify(result);
    if (err) throw Error(err);

    return pb.RunHealthCheckResponse.create(result);
};

export interface SetReadOnlyParams {
    clusterID?: string;
    alias: string;
}

export const setReadOnly = async ({ clusterID, alias }: SetReadOnlyParams) => {
    const { result } = await vtfetch(`/api/tablet/${alias}/set_read_only?cluster=${clusterID}`, { method: 'put' });
    const err = pb.SetReadOnlyResponse.verify(result);
    if (err) throw Error(err);

    return pb.SetReadOnlyResponse.create(result);
};

export interface SetReadWriteParams {
    clusterID?: string;
    alias: string;
}

export const setReadWrite = async ({ clusterID, alias }: SetReadWriteParams) => {
    const { result } = await vtfetch(`/api/tablet/${alias}/set_read_write?cluster=${clusterID}`, { method: 'put' });
    const err = pb.SetReadWriteResponse.verify(result);
    if (err) throw Error(err);

    return pb.SetReadWriteResponse.create(result);
};

export interface StartReplicationParams {
    clusterID?: string;
    alias: string;
}

export const startReplication = async ({ clusterID, alias }: StartReplicationParams) => {
    const { result } = await vtfetch(`/api/tablet/${alias}/start_replication?cluster=${clusterID}`, { method: 'put' });
    const err = pb.StartReplicationResponse.verify(result);
    if (err) throw Error(err);

    return pb.StartReplicationResponse.create(result);
};

export interface StopReplicationParams {
    clusterID?: string;
    alias: string;
}

export const stopReplication = async ({ clusterID, alias }: StopReplicationParams) => {
    const { result } = await vtfetch(`/api/tablet/${alias}/stop_replication?cluster=${clusterID}`, { method: 'put' });
    const err = pb.StopReplicationResponse.verify(result);
    if (err) throw Error(err);

    return pb.StopReplicationResponse.create(result);
};
export interface TabletDebugVarsResponse {
    params: FetchTabletParams;
    data?: TabletDebugVars;
}

export const fetchExperimentalTabletDebugVars = async (params: FetchTabletParams): Promise<TabletDebugVarsResponse> => {
    if (!env().VITE_ENABLE_EXPERIMENTAL_TABLET_DEBUG_VARS) {
        return Promise.resolve({ params });
    }

    const { clusterID, alias } = params;
    const { result } = await vtfetch(`/api/experimental/tablet/${alias}/debug/vars?cluster=${clusterID}`);

    // /debug/vars doesn't contain cluster/tablet information, so we
    // return that as part of the response.
    return { params, data: result };
};

export const fetchTablets = async () =>
    vtfetchEntities({
        endpoint: '/api/tablets',
        extract: (res) => res.result.tablets,
        transform: (e) => {
            const err = pb.Tablet.verify(e);
            if (err) throw Error(err);
            return pb.Tablet.create(e);
        },
    });
export interface FetchVSchemaParams {
    clusterID: string;
    keyspace: string;
}

export const fetchVSchema = async ({ clusterID, keyspace }: FetchVSchemaParams) => {
    const { result } = await vtfetch(`/api/vschema/${clusterID}/${keyspace}`);

    const err = pb.VSchema.verify(result);
    if (err) throw Error(err);

    return pb.VSchema.create(result);
};

export const fetchWorkflows = async () => {
    const { result } = await vtfetch(`/api/workflows`);

    const err = pb.GetWorkflowsResponse.verify(result);
    if (err) throw Error(err);

    return pb.GetWorkflowsResponse.create(result);
};

export const fetchWorkflow = async (params: { clusterID: string; keyspace: string; name: string }) => {
    const { result } = await vtfetch(`/api/workflow/${params.clusterID}/${params.keyspace}/${params.name}`);

    const err = pb.Workflow.verify(result);
    if (err) throw Error(err);

    return pb.Workflow.create(result);
};

export const fetchVTExplain = async <R extends pb.IVTExplainRequest>({ cluster, keyspace, sql }: R) => {
    // As an easy enhancement for later, we can also validate the request parameters on the front-end
    // instead of defaulting to '', to save a round trip.
    const req = new URLSearchParams();
    req.append('cluster', cluster || '');
    req.append('keyspace', keyspace || '');
    req.append('sql', sql || '');

    const { result } = await vtfetch(`/api/vtexplain?${req}`);

    const err = pb.VTExplainResponse.verify(result);
    if (err) throw Error(err);

    return pb.VTExplainResponse.create(result);
};

export interface ValidateKeyspaceParams {
    clusterID: string;
    keyspace: string;
    pingTablets: boolean;
}

export const validateKeyspace = async ({ clusterID, keyspace, pingTablets }: ValidateKeyspaceParams) => {
    const body = JSON.stringify({ pingTablets });

    const { result } = await vtfetch(`/api/keyspace/${clusterID}/${keyspace}/validate`, { method: 'put', body });
    const err = vtctldata.ValidateKeyspaceResponse.verify(result);
    if (err) throw Error(err);

    return vtctldata.ValidateKeyspaceResponse.create(result);
};

export interface ValidateSchemaKeyspaceParams {
    clusterID: string;
    keyspace: string;
}

export const validateSchemaKeyspace = async ({ clusterID, keyspace }: ValidateSchemaKeyspaceParams) => {
    const { result } = await vtfetch(`/api/keyspace/${clusterID}/${keyspace}/validate/schema`, { method: 'put' });
    const err = vtctldata.ValidateSchemaKeyspaceResponse.verify(result);
    if (err) throw Error(err);

    return vtctldata.ValidateSchemaKeyspaceResponse.create(result);
};

export interface ValidateVersionKeyspaceParams {
    clusterID: string;
    keyspace: string;
}

export const validateVersionKeyspace = async ({ clusterID, keyspace }: ValidateVersionKeyspaceParams) => {
    const { result } = await vtfetch(`/api/keyspace/${clusterID}/${keyspace}/validate/version`, { method: 'put' });
    const err = vtctldata.ValidateVersionKeyspaceResponse.verify(result);
    if (err) throw Error(err);

    return vtctldata.ValidateVersionKeyspaceResponse.create(result);
};

export interface FetchShardReplicationPositionsParams {
    clusterIDs?: (string | null | undefined)[];
    keyspaces?: (string | null | undefined)[];
    keyspaceShards?: (string | null | undefined)[];
}

export const fetchShardReplicationPositions = async ({
    clusterIDs = [],
    keyspaces = [],
    keyspaceShards = [],
}: FetchShardReplicationPositionsParams) => {
    const req = new URLSearchParams();
    clusterIDs.forEach((c) => c && req.append('cluster', c));
    keyspaces.forEach((k) => k && req.append('keyspace', k));
    keyspaceShards.forEach((s) => s && req.append('keyspace_shard', s));

    const { result } = await vtfetch(`/api/shard_replication_positions?${req}`);
    const err = pb.GetShardReplicationPositionsResponse.verify(result);
    if (err) throw Error(err);

    return pb.GetShardReplicationPositionsResponse.create(result);
};

export interface ReloadSchemaParams {
    clusterIDs?: (string | null | undefined)[];
    concurrency?: number;
    includePrimary?: boolean;
    keyspaces?: (string | null | undefined)[];

    // e.g., ["commerce/0"]
    keyspaceShards?: (string | null | undefined)[];

    // A list of tablet aliases; e.g., ["zone1-101", "zone1-102"]
    tablets?: (string | null | undefined)[];

    waitPosition?: string;
}

export const reloadSchema = async (params: ReloadSchemaParams) => {
    const req = new URLSearchParams();

    (params.clusterIDs || []).forEach((c) => c && req.append('cluster', c));
    (params.keyspaces || []).forEach((k) => k && req.append('keyspace', k));
    (params.keyspaceShards || []).forEach((k) => k && req.append('keyspaceShard', k));
    (params.tablets || []).forEach((t) => t && req.append('tablet', t));

    if (typeof params.concurrency === 'number') {
        req.append('concurrency', params.concurrency.toString());
    }

    if (typeof params.includePrimary === 'boolean') {
        req.append('include_primary', params.includePrimary.toString());
    }

    if (typeof params.waitPosition === 'string') {
        req.append('wait_position', params.waitPosition);
    }

    const { result } = await vtfetch(`/api/schemas/reload?${req}`, { method: 'put' });

    const err = pb.ReloadSchemasResponse.verify(result);
    if (err) throw Error(err);

    return pb.ReloadSchemasResponse.create(result);
};

export interface DeleteShardParams {
    clusterID: string;
    keyspaceShard: string;
    evenIfServing: boolean;
    recursive: boolean;
}

export const deleteShard = async (params: DeleteShardParams) => {
    const req = new URLSearchParams();
    req.append('keyspace_shard', params.keyspaceShard);
    req.append('even_if_serving', String(params.evenIfServing));
    req.append('recursive', String(params.recursive));

    const { result } = await vtfetch(`/api/shards/${params.clusterID}?${req}`, { method: 'delete' });

    const err = vtctldata.DeleteShardsResponse.verify(result);
    if (err) throw Error(err);

    return vtctldata.DeleteShardsResponse.create(result);
};

export interface ReloadSchemaShardParams {
    clusterID: string;
    keyspace: string;
    shard: string;

    waitPosition?: string;
    includePrimary: boolean;
    concurrency?: number;
}

export const reloadSchemaShard = async (params: ReloadSchemaShardParams) => {
    const body: Record<string, string | boolean | number> = {
        include_primary: params.includePrimary,
    };

    if (params.waitPosition) {
        body.wait_position = params.waitPosition;
    }

    if (params.concurrency) {
        body.concurrency = params.concurrency;
    }

    const { result } = await vtfetch(
        `/api/shard/${params.clusterID}/${params.keyspace}/${params.shard}/reload_schema_shard`,
        {
            method: 'put',
            body: JSON.stringify(body),
        }
    );

    const err = pb.ReloadSchemaShardResponse.verify(result);
    if (err) throw Error(err);

    return pb.ReloadSchemaShardResponse.create(result);
};

export interface TabletExternallyPromotedParams {
    alias?: string;
    clusterIDs: string[];
}

export const tabletExternallyPromoted = async (params: TabletExternallyPromotedParams) => {
    const req = new URLSearchParams();
    req.append('cluster', params.clusterIDs[0]);

    const { result } = await vtfetch(`/api/tablet/${params.alias}/externally_promoted?${req}`, {
        method: 'post',
    });

    const err = pb.TabletExternallyPromotedResponse.verify(result);
    if (err) throw Error(err);

    return pb.TabletExternallyPromotedResponse.create(result);
};

export interface PlannedFailoverShardParams {
    clusterID: string;
    keyspace: string;
    shard: string;
    new_primary?: vtadmin.Tablet;
}

export const plannedFailoverShard = async (params: PlannedFailoverShardParams) => {
    const body: Partial<pb.PlannedFailoverShardRequest['options']> = {};
    if (params.new_primary) body['new_primary'] = params.new_primary.tablet?.alias;

    const { result } = await vtfetch(
        `/api/shard/${params.clusterID}/${params.keyspace}/${params.shard}/planned_failover`,
        {
            method: 'post',
            body: JSON.stringify(body),
        }
    );

    const err = pb.PlannedFailoverShardResponse.verify(result);
    if (err) throw Error(err);

    return pb.PlannedFailoverShardResponse.create(result);
};

export interface EmergencyFailoverShardParams {
    clusterID: string;
    keyspace: string;
    shard: string;
    new_primary?: vtadmin.Tablet;
}

export const emergencyFailoverShard = async (params: EmergencyFailoverShardParams) => {
    const body: Partial<pb.PlannedFailoverShardRequest['options']> = {};
    if (params.new_primary && params.new_primary.tablet?.alias) body['new_primary'] = params.new_primary.tablet?.alias;

    const { result } = await vtfetch(
        `/api/shard/${params.clusterID}/${params.keyspace}/${params.shard}/emergency_failover`,
        {
            method: 'post',
            body: JSON.stringify(body),
        }
    );

    const err = pb.EmergencyFailoverShardResponse.verify(result);
    if (err) throw Error(err);

    return pb.EmergencyFailoverShardResponse.create(result);
};

export interface RebuildKeyspaceGraphParams {
    clusterID: string;
    keyspace: string;

    // A comma-separated list of cells, eg. "zone1,zone2"
    cells?: string;

    allowPartial?: boolean;
}

export const rebuildKeyspaceGraph = async (params: RebuildKeyspaceGraphParams) => {
    const { result } = await vtfetch(`/api/keyspace/${params.clusterID}/${params.keyspace}/rebuild_keyspace_graph`, {
        method: 'put',
        body: JSON.stringify({ cells: params.cells, allow_partial: params.allowPartial }),
    });
    const err = pb.RebuildKeyspaceGraphRequest.verify(result);
    if (err) throw Error(err);

    return pb.RebuildKeyspaceGraphResponse.create(result);
};

export interface RemoveKeyspaceCellParams {
    clusterID: string;
    keyspace: string;
    cell: string;
    force: boolean;
    recursive: boolean;
}

export const removeKeyspaceCell = async (params: RemoveKeyspaceCellParams) => {
    const { result } = await vtfetch(`/api/keyspace/${params.clusterID}/${params.keyspace}/remove_keyspace_cell`, {
        method: 'put',
        body: JSON.stringify({ cell: params.cell, force: params.force, recursive: params.recursive }),
    });
    const err = pb.RemoveKeyspaceCellRequest.verify(result);
    if (err) throw Error(err);

    return pb.RemoveKeyspaceCellResponse.create(result);
};

export interface CreateShardParams {
    keyspace: string;
    clusterID: string;

    // shardName is the name of the shard to create. E.g. "-" or "-80".
    shard_name: string;

    // force treats an attempt to create a shard that already exists as a
    // non-error.
    force?: boolean;

    // IncludeParent creates the parent keyspace as an empty BASE keyspace, if it
    // doesn't already exist.
    include_parent?: boolean;
}

export const createShard = async (params: CreateShardParams) => {
    const { result } = await vtfetch(`/api/shards/${params.clusterID}`, {
        method: 'post',
        body: JSON.stringify(params),
    });
    const err = vtctldata.CreateShardResponse.verify(result);
    if (err) throw Error(err);

    return vtctldata.CreateShardResponse.create(result);
};

export interface GetTopologyPathParams {
    clusterID: string;
    path: string;
}

export const getTopologyPath = async (params: GetTopologyPathParams) => {
    const req = new URLSearchParams({ path: params.path });
    const { result } = await vtfetch(`/api/cluster/${params.clusterID}/topology?${req}`);

    const err = vtctldata.GetTopologyPathResponse.verify(result);
    if (err) throw Error(err);

    return vtctldata.GetTopologyPathResponse.create(result);
};
export interface ValidateParams {
    clusterID: string;
    pingTablets: boolean;
}

export const validate = async (params: ValidateParams) => {
    const { result } = await vtfetch(`/api/cluster/${params.clusterID}/validate`, {
        method: 'put',
        body: JSON.stringify({ ping_tablets: params.pingTablets }),
    });
    const err = pb.ValidateRequest.verify(result);
    if (err) throw Error(err);

    return vtctldata.ValidateResponse.create(result);
};

export interface ValidateShardParams {
    clusterID: string;
    keyspace: string;
    shard: string;
    pingTablets: boolean;
}

export const validateShard = async (params: ValidateShardParams) => {
    const { result } = await vtfetch(`/api/shard/${params.clusterID}/${params.keyspace}/${params.shard}/validate`, {
        method: 'put',
        body: JSON.stringify({ ping_tablets: params.pingTablets }),
    });

    const err = vtctldata.ValidateShardResponse.verify(result);
    if (err) throw Error(err);

    return vtctldata.ValidateShardResponse.create(result);
};

export interface GetFullStatusParams {
    clusterID: string;
    alias: string;
}

export const getFullStatus = async (params: GetFullStatusParams) => {
    const req = new URLSearchParams();
    req.append('cluster', params.clusterID);

    const { result } = await vtfetch(`/api/tablet/${params.alias}/full_status?${req.toString()}`);

    const err = vtctldata.GetFullStatusResponse.verify(result);
    if (err) throw Error(err);

    return vtctldata.GetFullStatusResponse.create(result);
};

export interface ValidateVersionShardParams {
    clusterID: string;
    keyspace: string;
    shard: string;
}

export const validateVersionShard = async (params: ValidateVersionShardParams) => {
    const { result } = await vtfetch(
        `/api/shard/${params.clusterID}/${params.keyspace}/${params.shard}/validate_version`,
        {
            method: 'put',
        }
    );

    const err = vtctldata.ValidateVersionShardResponse.verify(result);
    if (err) throw Error(err);

    return vtctldata.ValidateVersionShardResponse.create(result);
};
