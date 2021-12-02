import React, { useEffect, useState } from 'react'
import { usePingTablet } from '../../../hooks/api';
import { Transition } from '@headlessui/react'
import Dialog from '../../dialog/Dialog';
import { Icon, Icons } from '../../Icon';

const PingDialog: React.FC<{ alias: string, clusterID?: string, isOpen: boolean, onClose: () => void }> = ({ alias, clusterID, isOpen, onClose }) => {
    const SuccessState: React.FC = () => (
        <div className="w-full flex justify-center items-center">
            <span className="flex h-6 w-6 relative items-center justify-center">
                <Icon className="fill-current text-green-500" icon={Icons.checkSuccess} />
            </span>
            <div className="text-xl ml-2 font-bold">Sucessfully pinged tablet {alias}.</div>
        </div>
    )

    const FailState: React.FC = () => (
        <div className="w-full flex justify-center items-center">
            <span className="flex h-6 w-6 relative items-center justify-center">
                <Icon className="fill-current text-red-500" icon={Icons.alertFail} />
            </span>
            <div className="text-xl ml-2 font-bold">There was an issue pinging tablet {alias}.</div>
        </div>
    )
    // Mount content as separate component inside Dialog so internal queries are not executed unless dialog is open.
    const PingContent: React.FC = () => {
        const { data: pingResponse, isLoading, isError } = usePingTablet({ alias, clusterID });
        const [loading, setLoading] = useState(true)
        useEffect(() => {
            setTimeout(() => {
                setLoading(isLoading)
            }, 500)
        })
        return (
            <div className="my-5 flex justify-center items-center w-full h-20">
                <Transition className="absolute" show={loading && isOpen} leave="transition-opacity duration-100" enter="transition-opacity duration-75" enterFrom="opacity-0" enterTo="opacity-100" leaveFrom='opacity-100' leaveTo='opacity-0'>
                    <div className="w-full flex justify-center items-center">
                        <span className="flex h-6 w-6 relative items-center justify-center">
                            <span className="animate-ping absolute inline-flex h-full w-full rounded-full bg-yellow-400 opacity-75"></span>
                            <span className="relative inline-flex rounded-full h-6 w-6 bg-yellow-500"></span>
                        </span>
                        <div className="text-xl ml-2 font-bold">Pinging tablet {alias}...</div>
                    </div>
                </Transition>
                <Transition className="absolute" show={!loading && isOpen} enter="delay-100 transition-opacity duration-75" enterFrom="opacity-0" enterTo="opacity-100">
                    {(!isError && pingResponse?.status === "ok") && <SuccessState />}
                    {isError && <FailState />}
                </Transition>
            </div>
        )
    }

    return (
        <Dialog isOpen={isOpen} onClose={onClose} hideCancel={true} confirmText='Done'>
            <div><PingContent /></div>
        </Dialog>
    )
}

export default PingDialog