import React from 'react';
import { Icon, Icons } from '../../Icon';
import Dialog from '../../dialog/Dialog';
import { UseMutationResult } from 'react-query';

interface WorkflowActionProps {
    isOpen: boolean;
    mutation: UseMutationResult<any, any, any>;
    title: string;
    confirmText: string;
    successText?: string;
    errorText: string;
    errorDescription?: string;
    loadingText: string;
    description?: string;
    className?: string;
    body?: JSX.Element;
    successBody?: JSX.Element;
    hideSuccessDialog?: boolean;
    refetchWorkflows: Function;
    closeDialog: () => void;
}

const WorkflowAction: React.FC<WorkflowActionProps> = ({
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
    errorDescription,
    refetchWorkflows,
    hideSuccessDialog,
    body,
    className,
}) => {
    const onCloseDialog = () => {
        setTimeout(mutation.reset, 500);
        closeDialog();
    };

    const hasRun = mutation.data || mutation.error;
    const onConfirm = () => {
        mutation.mutate(
            {},
            {
                onSuccess: () => {
                    if (hideSuccessDialog) {
                        closeDialog();
                    }
                    refetchWorkflows();
                },
            }
        );
    };
    return (
        <Dialog
            isOpen={isOpen}
            confirmText={hasRun ? 'Close' : confirmText}
            cancelText="Cancel"
            onConfirm={hasRun ? onCloseDialog : onConfirm}
            loadingText={loadingText}
            loading={mutation.isLoading}
            onCancel={onCloseDialog}
            onClose={onCloseDialog}
            hideCancel={hasRun}
            title={hasRun ? undefined : title}
            description={hasRun ? undefined : description}
            className={className}
        >
            <div className="w-full">
                {!hasRun && body}
                {!hideSuccessDialog && mutation.data && !mutation.error && (
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
                        {errorDescription && <div className="text-sm mt-1">{errorDescription}</div>}
                    </div>
                )}
            </div>
        </Dialog>
    );
};

export default WorkflowAction;
