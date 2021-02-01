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

type HttpResponse = HttpOkResponse | HttpErrorResponse;

export const MALFORMED_HTTP_RESPONSE_ERROR = 'MalformedHttpResponseError';
class MalformedHttpResponseError extends Error {
    responseJson: object;

    constructor(message: string, responseJson: object) {
        super(message);
        this.name = MALFORMED_HTTP_RESPONSE_ERROR;
        this.responseJson = responseJson;
    }
}

export const HTTP_RESPONSE_NOT_OK_ERROR = 'HttpResponseNotOkError';
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
export const vtfetch = async (endpoint: string): Promise<HttpResponse> => {
    const { REACT_APP_VTADMIN_API_ADDRESS } = process.env;

    const url = `${REACT_APP_VTADMIN_API_ADDRESS}${endpoint}`;
    const opts = vtfetchOpts();

    const response = await global.fetch(url, opts);

    const json = await response.json();
    if (!('ok' in json)) throw new MalformedHttpResponseError('invalid http envelope', json);

    return json as HttpResponse;
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

export const fetchTablets = async () => {
    const endpoint = '/api/tablets';
    const res = await vtfetch(endpoint);

    // Throw "not ok" responses so that react-query correctly interprets them as errors.
    // See https://react-query.tanstack.com/guides/query-functions#handling-and-throwing-errors
    if (!res.ok) throw new HttpResponseNotOkError(endpoint, res);

    const tablets = res.result?.tablets;
    if (!Array.isArray(tablets)) throw Error(`expected tablets to be an array, got ${tablets}`);

    return tablets.map((t: any) => {
        const err = pb.Tablet.verify(t);
        if (err) throw Error(err);

        return pb.Tablet.create(t);
    });
};
