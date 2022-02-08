import React from 'react'
import { Icon, Icons } from '../../Icon';
import Dialog from '../../dialog/Dialog'
import { UseMutationResult } from 'react-query';

interface KeyspaceActionProps {
    isOpen: boolean
    mutation: UseMutationResult<any, any, any>
    title: string
    confirmText: string
    successText: string
    errorText: string
    loadingText: string
    description: string
    body: JSX.Element
    successBody?: JSX.Element
    closeDialog: () => void
}

const KeyspaceAction: React.FC<KeyspaceActionProps> = ({ isOpen, closeDialog, mutation, title, confirmText, description, successText, successBody, loadingText, errorText, body }) => {
    const onCloseDialog = () => {
        setTimeout(mutation.reset, 500)
        closeDialog()
    }
    return (
        <div>
            <Dialog
                isOpen={isOpen}
                confirmText={Boolean(mutation.data) ? "Close" : confirmText}
                cancelText="Cancel"
                onConfirm={Boolean(mutation.data) ? onCloseDialog : mutation.mutate}
                loadingText={loadingText}
                loading={mutation.isLoading}
                onCancel={onCloseDialog}
                onClose={onCloseDialog}
                hideCancel={Boolean(mutation.data)}
                title={mutation.data ? undefined : title}
                description={mutation.data ? undefined : description}
            >
                <div className="w-full">
                    {!mutation.error && !mutation.data && body}
                    {mutation.data && !mutation.error && (
                        <div className="w-full flex flex-col justify-center items-center">
                            <span className="flex h-12 w-12 relative items-center justify-center">
                                <Icon className="fill-current text-green-500" icon={Icons.checkSuccess} />
                            </span>
                            <div className="text-lg mt-3 font-bold">{successText}</div>
                            {successBody}
                        </div>
                    )
                    }
                    {mutation.error &&
                        <div className="w-full flex flex-col justify-center items-center">
                            <span className="flex h-12 w-12 relative items-center justify-center">
                                <Icon className="fill-current text-green-500" icon={Icons.alertFail} />
                            </span>
                            <div className="text-lg mt-3 font-bold">{errorText}</div>
                        </div>}
                </div>
            </Dialog>
        </div >
    )
}

export default KeyspaceAction