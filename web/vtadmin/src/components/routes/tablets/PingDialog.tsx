import React, { useEffect, useState } from 'react';
import { usePingTablet } from '../../../hooks/api';
import { Transition } from '@headlessui/react';
import Dialog from '../../dialog/Dialog';
import { Icon, Icons } from '../../Icon';

const PingDialog: React.FC<{ alias: string; clusterID?: string; isOpen: boolean; onClose: () => void }> = ({
    alias,
    clusterID,
    isOpen,
    onClose,
}) => {
    // Mount content as separate component inside Dialog so internal queries are not executed unless dialog is open.
    const { data: pingResponse, isLoading, isError, error, refetch } = usePingTablet({ alias, clusterID }, { enabled: false });
    const [loading, setLoading] = useState(true)
    useEffect(() => {
        let timeout: NodeJS.Timeout
        if (isOpen) {
            setLoading(true)
            timeout = setTimeout(() => {
                refetch();
                setLoading(isLoading)
            }, 300);
        }
        return () => timeout && clearTimeout(timeout)
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [isOpen]);

    const SuccessState: React.FC = () => (
        <div className="w-full flex flex-col justify-center items-center">
            <span className="flex h-12 w-12 relative items-center justify-center">
                <Icon className="fill-current text-green-500" icon={Icons.checkSuccess} />
            </span>
            <div className="text-lg mt-3 font-bold">Success!</div>
            <div className="text-sm">Successfully reached tablet {alias} via RPC.</div>
        </div>
    );

    const FailState: React.FC = () => (
        <div className="w-full flex flex-col justify-center items-center">
            <span className="flex h-12 w-12 relative items-center justify-center">
                <Icon className="fill-current text-red-500" icon={Icons.alertFail} />
            </span>
            <div className="text-lg mt-3 font-bold">Error</div>
            <div className="text-sm">There was an issue pinging tablet {alias}: {error}</div>
        </div>
    );

    return (
        <Dialog isOpen={isOpen} onClose={onClose} hideCancel={true} confirmText="Done">
            <div>
                <div className="flex justify-center items-center w-full h-40">
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
                            <div className="text-lg mt-6 font-bold">Pinging tablet {alias}</div>
                            <div className="text-sm">Checking to see if tablet is reachable via RPC...</div>
                        </div>
                    </Transition>
                    <Transition
                        className="absolute"
                        show={!loading && isOpen}
                        enter="delay-100 transition-opacity duration-75"
                        enterFrom="opacity-0"
                        enterTo="opacity-100"
                    >
                        {!isError && pingResponse?.status === 'ok' && <SuccessState />}
                        {isError && <FailState />}
                    </Transition>
                </div>
            </div>
        </Dialog>
    );
};

export default PingDialog;
