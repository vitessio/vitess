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

import { vtadmin as pb } from '../proto/vtadmin';

interface HttpOkResponse {
    ok: true;
    result: any;
}

interface HttpErrorResponse {
    ok: false;
}

export const MALFORMED_HTTP_RESPONSE_ERROR = 'MalformedHttpResponseError';

// MalformedHttpResponseError is thrown when the JSON response envelope
// is an unexpected shape.
class MalformedHttpResponseError extends Error {
    responseJson: object;

    constructor(message: string, responseJson: object) {
        super(message);
        this.name = MALFORMED_HTTP_RESPONSE_ERROR;
        this.responseJson = responseJson;
    }
}

export const HTTP_RESPONSE_NOT_OK_ERROR = 'HttpResponseNotOkError';

// HttpResponseNotOkError is throw when the `ok` is false in
// the JSON response envelope.
class HttpResponseNotOkError extends Error {
    response: HttpErrorResponse | null;

    constructor(endpoint: string, response: HttpErrorResponse) {
        super(endpoint);
        this.name = HTTP_RESPONSE_NOT_OK_ERROR;
        this.response = response;
    }
}

// vtfetch makes HTTP requests against the given vtadmin-api endpoint
// and returns the parsed response.
//
// HttpResponse envelope types are not defined in vtadmin.proto (nor should they be)
// thus we have to validate the shape of the API response with more care.
//
// Note that this only validates the HttpResponse envelope; it does not
// do any type checking or validation on the result.
export const vtfetch = async (endpoint: string): Promise<HttpOkResponse> => {
    const { REACT_APP_VTADMIN_API_ADDRESS } = process.env;

    const url = `${REACT_APP_VTADMIN_API_ADDRESS}${endpoint}`;
    const opts = vtfetchOpts();

    const response = await global.fetch(url, opts);

    const json = await response.json();
    if (!('ok' in json)) throw new MalformedHttpResponseError('invalid http envelope', json);

    // Throw "not ok" responses so that react-query correctly interprets them as errors.
    // See https://react-query.tanstack.com/guides/query-functions#handling-and-throwing-errors
    if (!json.ok) throw new HttpResponseNotOkError(endpoint, json);

    return json as HttpOkResponse;
};

export const vtfetchOpts = (): RequestInit => {
    const credentials = process.env.REACT_APP_FETCH_CREDENTIALS;
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
        throw Error(`expected entities to be an array, got ${entities}`);
    }

    return entities.map(opts.transform);
};

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

export const fetchWorkflows = async () => {
    const { result } = await vtfetch(`/api/workflows`);

    const err = pb.GetWorkflowsResponse.verify(result);
    if (err) throw Error(err);

    return pb.GetWorkflowsResponse.create(result);
};
