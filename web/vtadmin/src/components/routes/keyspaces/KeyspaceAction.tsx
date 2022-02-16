import React from 'react';
import { Icon, Icons } from '../../Icon';
import Dialog from '../../dialog/Dialog';
import { UseMutationResult } from 'react-query';

interface KeyspaceActionProps {
    isOpen: boolean;
    mutation: UseMutationResult<any, any, any>;
    title: string;
    confirmText: string;
    successText: string;
    errorText: string;
    loadingText: string;
    description?: string;
    body?: JSX.Element;
    successBody?: JSX.Element;
    closeDialog: () => void;
}

const KeyspaceAction: React.FC<KeyspaceActionProps> = ({
    isOpen,
    closeDialog,
    mutation,
    title,
    confirmText,
    description,
    successText,
    successBody,
    loadingText,
    errorText,
    body,
}) => {
    const onCloseDialog = () => {
        setTimeout(mutation.reset, 500);
        closeDialog();
    };

    const hasRun = mutation.data || mutation.error;
    return (
        <div>
            <Dialog
                isOpen={isOpen}
                confirmText={hasRun ? 'Close' : confirmText}
                cancelText="Cancel"
                onConfirm={hasRun ? onCloseDialog : mutation.mutate}
                loadingText={loadingText}
                loading={mutation.isLoading}
                onCancel={onCloseDialog}
                onClose={onCloseDialog}
                hideCancel={hasRun}
                title={hasRun ? undefined : title}
                description={hasRun ? undefined : description}
            >
                <div className="w-full">
                    {!hasRun && body}
                    {mutation.data && !mutation.error && (
                        <div className="w-full flex flex-col justify-center items-center">
                            <span className="flex h-12 w-12 relative items-center justify-center">
                                <Icon className="fill-current text-green-500" icon={Icons.checkSuccess} />
                            </span>
                            <div className="text-lg mt-3 font-bold text-center">{successText}</div>
                            {successBody}
                        </div>
                    )}
                    {mutation.error && (
                        <div className="w-full flex flex-col justify-center items-center">
                            <span className="flex h-12 w-12 relative items-center justify-center">
                                <Icon className="fill-current text-red-500" icon={Icons.alertFail} />
                            </span>
                            <div className="text-lg mt-3 font-bold text-center">{errorText}</div>
                        </div>
                    )}
                </div>
            </Dialog>
        </div>
    );
};

export default KeyspaceAction;
