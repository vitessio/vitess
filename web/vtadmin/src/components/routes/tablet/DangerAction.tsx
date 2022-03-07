import React, { useState } from 'react';
import { UseMutationResult } from 'react-query';
import { Icon, Icons } from '../../Icon';
import { TextInput } from '../../TextInput';

type Mutation = UseMutationResult & {
    mutate: () => void;
};

interface DangerActionProps {
    title: string;
    action: string;
    description: JSX.Element;
    primary: boolean;
    primaryDescription: JSX.Element;
    alias: string;
    mutation: UseMutationResult;
    loadingText: string;
    loadedText: string;
    documentationLink: string;
}

const DangerAction: React.FC<DangerActionProps> = ({
    title,
    description,
    action,
    documentationLink,
    primary,
    primaryDescription,
    alias,
    mutation,
    loadingText,
    loadedText,
}) => {
    const [typedAlias, setTypedAlias] = useState('');

    return (
        <div className="p-8">
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
            {primary && (
                <div className="text-danger flex items-center">
                    <Icon icon={Icons.alertFail} className="fill-current text-danger inline mr-2" />
                    {primaryDescription}
                </div>
            )}

            <p className="text-base">Please type the tablet's alias to {action}:</p>
            <div className="w-1/3">
                <TextInput placeholder="zone-xxx" value={typedAlias} onChange={(e) => setTypedAlias(e.target.value)} />
            </div>
            <button
                className="btn btn-secondary btn-danger mt-4"
                disabled={typedAlias !== alias || mutation.isLoading}
                onClick={() => {
                    (mutation as Mutation).mutate();
                    setTypedAlias('');
                }}
            >
                {mutation.isLoading ? loadingText : loadedText}
            </button>
        </div>
    );
};

export default DangerAction;
