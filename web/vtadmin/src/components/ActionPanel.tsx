/**
 * Copyright 2022 The Vitess Authors.
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

import React, { useState } from 'react';
import { UseMutationResult } from 'react-query';

import { Icon, Icons } from './Icon';
import { TextInput } from './TextInput';

type Mutation = UseMutationResult & {
    mutate: () => void;
};

export interface ActionPanelProps {
    confirmationValue?: string;
    danger?: boolean;
    description: React.ReactNode;
    disabled?: boolean;
    documentationLink: string;
    loadingText: string;
    loadedText: string;
    mutation: UseMutationResult;
    title: string;
    warnings?: React.ReactNodeArray;
}

/**
 * ActionPanel is a panel used for initiating mutations on entity pages.
 * When rendering multiple ActionPanel components, ensure they are in
 * a surrounding <div> to ensure the first: and last: CSS selectors work.
 */
const ActionPanel: React.FC<ActionPanelProps> = ({
    confirmationValue,
    danger,
    disabled,
    title,
    description,
    documentationLink,
    mutation,
    loadingText,
    loadedText,
    warnings = [],
}) => {
    const [typedConfirmation, setTypedConfirmation] = useState('');

    const requiresConfirmation = typeof confirmationValue === 'string' && !!confirmationValue;

    const isDisabled =
        !!disabled || mutation.isLoading || (requiresConfirmation && typedConfirmation !== confirmationValue);

    return (
        <div
            className={`p-9 pb-12 last:border-b border ${
                danger ? 'border-red-400' : 'border-gray-400'
            } border-b-0 first:rounded-t-lg last:rounded-b-lg`}
            title={title}
        >
            <div className="flex justify-between items-center">
                <p className="text-base font-bold m-0 text-gray-900">{title}</p>
                <a
                    href={documentationLink}
                    target="_blank"
                    rel="noreferrer"
                    className="text-gray-900 ml-1 inline-block"
                >
                    <span className="text-sm font-semibold text-gray-900">Documentation</span>
                    <Icon icon={Icons.open} className="ml-1 h-6 w-6 text-gray-900 fill-current inline" />
                </a>
            </div>
            <p className="text-base mt-0">{description}</p>

            {warnings.map(
                (warning, i) =>
                    warning && (
                        <div className="text-danger flex items-center" key={i}>
                            <Icon icon={Icons.alertFail} className="fill-current text-danger inline mr-2" />
                            {warning}
                        </div>
                    )
            )}

            {/* Don't render the confirmation input if "disabled" prop is set */}
            {requiresConfirmation && !disabled && (
                <>
                    <p className="text-base">
                        Please type <span className="font-bold">{confirmationValue}</span> confirm.
                    </p>
                    <div className="w-1/3">
                        <TextInput value={typedConfirmation} onChange={(e) => setTypedConfirmation(e.target.value)} />
                    </div>
                </>
            )}

            <button
                className={`btn btn-secondary ${danger && 'btn-danger'} mt-4`}
                disabled={isDisabled}
                onClick={() => {
                    (mutation as Mutation).mutate();
                    setTypedConfirmation('');
                }}
            >
                {mutation.isLoading ? loadingText : loadedText}
            </button>
        </div>
    );
};

export default ActionPanel;
