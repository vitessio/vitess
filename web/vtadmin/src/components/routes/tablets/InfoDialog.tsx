import React, { useEffect, useState } from 'react';
import { Transition } from '@headlessui/react';
import Dialog from '../../dialog/Dialog';
import { Icon, Icons } from '../../Icon';
import { UseQueryResult } from 'react-query';

export interface BaseInfoDialogProps {
    loadingTitle?: string;
    loadingDescription: string;
    successTitle?: string;
    successDescription: string;
    errorTitle?: string;
    errorDescription: string;
    useHook: () => UseQueryResult<any, Error>;
}

interface InfoDialogProps extends BaseInfoDialogProps {
    isOpen: boolean;
    onClose: () => void;
}

const InfoDialog: React.FC<InfoDialogProps> = ({
    loadingTitle,
    loadingDescription,
    successTitle,
    successDescription,
    errorTitle,
    errorDescription,
    useHook,
    isOpen,
    onClose,
}) => {
    const { data, error, isLoading, refetch } = useHook();

    // Animate loading briefly in case useHook is very fast
    // to give UX sense of work being done
    const [animationDone, setAnimationDone] = useState(false);
    useEffect(() => {
        let timeout: NodeJS.Timeout;
        if (isOpen) {
            timeout = setTimeout(() => {
                refetch();
                setAnimationDone(true);
            }, 300);
        }
        return () => timeout && clearTimeout(timeout);
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [isOpen]);

    const loading = !animationDone || isLoading;

    const SuccessState: React.FC = () => (
        <div className="w-full flex flex-col justify-center items-center">
            <span className="flex h-12 w-12 relative items-center justify-center">
                <Icon className="fill-current text-green-500" icon={Icons.checkSuccess} />
            </span>
            <div className="text-lg mt-3 font-bold">{successTitle || 'Success!'}</div>
            <div className="text-sm">{successDescription}</div>
        </div>
    );

    const FailState: React.FC = () => (
        <div className="w-full flex flex-col justify-center items-center">
            <span className="flex h-12 w-12 relative items-center justify-center">
                <Icon className="fill-current text-red-500" icon={Icons.alertFail} />
            </span>
            <div className="text-lg mt-3 font-bold">{errorTitle || 'Error'}</div>
            <div className="text-sm">
                {errorDescription}: {error?.message}
            </div>
        </div>
    );

    return (
        <Dialog
            isOpen={isOpen}
            onClose={() => {
                setAnimationDone(false);
                onClose();
            }}
            hideCancel={true}
            confirmText="Done"
        >
            <div>
                <div className="flex justify-center items-center w-full h-40">
                    {loading && (
                        <Transition
                            className="absolute"
                            show={loading && isOpen}
                            leave="transition-opacity duration-100"
                            enter="transition-opacity duration-75"
                            enterFrom="opacity-0"
                            enterTo="opacity-100"
                            leaveFrom="opacity-100"
                            leaveTo="opacity-0"
                        >
                            <div className="w-full flex flex-col justify-center items-center">
                                <span className="flex h-6 w-6 relative items-center justify-center">
                                    <span className="animate-ping absolute inline-flex h-full w-full rounded-full bg-yellow-400 opacity-75"></span>
                                    <span className="relative inline-flex rounded-full h-6 w-6 bg-yellow-500"></span>
                                </span>
                                <div className="text-lg mt-6 font-bold">{loadingTitle || 'Loading...'}</div>
                                <div className="text-sm">{loadingDescription}</div>
                            </div>
                        </Transition>
                    )}
                    {!loading && (
                        <Transition
                            className="absolute"
                            show={!loading && isOpen}
                            enter="delay-100 transition-opacity duration-75"
                            enterFrom="opacity-0"
                            enterTo="opacity-100"
                        >
                            {data && <SuccessState />}
                            {error && <FailState />}
                        </Transition>
                    )}
                </div>
            </div>
        </Dialog>
    );
};

export default InfoDialog;
