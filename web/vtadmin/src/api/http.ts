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

export const vtfetch = async (endpoint: string): Promise<HttpResponse> => {
    const url = `${process.env.REACT_APP_VTADMIN_API_ADDRESS}${endpoint}`;
    const response = await fetch(url);

    const json = await response.json();
    if (!('ok' in json)) throw Error('invalid http envelope');

    return json as HttpResponse;
};

export const fetchTablets = async () => {
    const res = await vtfetch('/api/tablets');
    if (!res.ok) throw Error('not ok');

    const { result } = res;
    if (!Array.isArray(result.tablets)) throw Error('no tablets');

    const tablets: pb.Tablet[] = result.tablets.map((t: any) => {
        const err = pb.Tablet.verify(t);
        if (err) throw Error(err);

        return pb.Tablet.create(t);
    });
    return tablets;
};
