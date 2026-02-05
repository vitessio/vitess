/**
 * Copyright 2024 The Vitess Authors.
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
import { invertBy } from 'lodash-es';
import { query } from '../proto/vtadmin';

/**
 * TRANSACTION_STATES maps numeric transaction state back to human readable strings.
 */
export const TRANSACTION_STATES = Object.entries(invertBy(query.TransactionState)).reduce((acc, [k, vs]) => {
    acc[k] = vs[0];
    return acc;
}, {} as { [k: string]: string });

export const formatTransactionState = (transaction: query.ITransactionMetadata) =>
    transaction.state && TRANSACTION_STATES[transaction.state];
