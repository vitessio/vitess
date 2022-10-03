import React, { useState } from 'react';
import { DataCell } from '../../dataTable/DataCell';
import { logutil, vtadmin as pb } from '../../../proto/vtadmin';
import Dialog from '../../dialog/Dialog';
import { Icon, Icons } from '../../Icon';
import { useValidate } from '../../../hooks/api';
import { Label } from '../../inputs/Label';
import Toggle from '../../toggle/Toggle';

interface Props {
    cluster: pb.Cluster;
}

const ClusterRow: React.FC<Props> = ({ cluster }) => {
    const [isOpen, setIsOpen] = useState(false);
    const [pingTablets, setPingTablets] = useState(false);

    const { mutate, error, data, isIdle, reset } = useValidate({ clusterID: cluster.id, pingTablets });
    const closeDialog = () => {
        setIsOpen(false);
        reset();
    };
    return (
        <tr>
            <Dialog
                isOpen={isOpen}
                confirmText={!isIdle ? 'Close' : 'Validate'}
                cancelText="Cancel"
                onConfirm={data ? closeDialog : () => mutate({ pingTablets, clusterID: cluster.id })}
                loadingText="Validating"
                onCancel={closeDialog}
                onClose={closeDialog}
                hideCancel={!isIdle}
                title={!isIdle ? undefined : 'Validate'}
                className="min-w-[400px]"
            >
                <div className="w-full">
                    {isIdle && (
                        <div>
                            <div className="my-4">
                                Validate that all nodes in the cluster are reachable from the global replication graph,
                                as well as all tablets in discoverable cells, are consistent.
                            </div>
                            <div className="flex items-center">
                                <Toggle enabled={pingTablets} onChange={() => setPingTablets(!pingTablets)} />
                                <Label className="ml-2" label="Ping Tablets" />
                            </div>
                            When set, all tablets will be pinged during the validation process.
                        </div>
                    )}
                    {!isIdle && !error && (
                        <div className="w-full">
                            <div className="flex items-center whitespace-nowrap">
                                <Icon className="fill-current text-green-500" icon={Icons.checkSuccess} />
                                <div className="ml-2 text-lg font-bold">
                                    Successfully validated cluster {cluster.name}
                                </div>
                            </div>
                            <div className="text-left w-full">
                                <table className="border-none bg-gray-100 ">
                                    <thead>
                                        <tr className="bg-gray-100">
                                            <th className="text-base text-black bg-gray-100">Keyspace</th>
                                            <th className="text-base text-black bg-gray-100">Shard</th>
                                            <th className="text-base text-black bg-gray-100">Result</th>
                                        </tr>
                                    </thead>
                                    <tbody>
                                        {data?.results_by_keyspace &&
                                            Object.entries(data?.results_by_keyspace).map(([keyspace, results]) => {
                                                return (
                                                    results.results_by_shard &&
                                                    Object.entries(results.results_by_shard).map(([shard, results]) => (
                                                        <tr className="p-1">
                                                            <td className="py-2">{keyspace}</td>
                                                            <td className="py-2">{shard}</td>
                                                            <td className="py-2">{results.results}</td>
                                                        </tr>
                                                    ))
                                                );
                                            })}
                                    </tbody>
                                </table>
                            </div>
                        </div>
                    )}
                    {!isIdle && error && (
                        <div className="w-full flex flex-col justify-center items-center">
                            <span className="flex h-12 w-12 relative items-center justify-center">
                                <Icon className="fill-current text-red-500" icon={Icons.alertFail} />
                            </span>
                            <div className="text-lg mt-3 font-bold text-center">
                                There was an issue validating nodes in cluster {cluster.name}
                            </div>
                        </div>
                    )}
                </div>
            </Dialog>
            <DataCell>{cluster.name}</DataCell>
            <DataCell>{cluster.id}</DataCell>
            <DataCell>
                <button
                    className="btn btn-secondary btn-sm"
                    onClick={() => {
                        setIsOpen(true);
                    }}
                >
                    Validate
                </button>
            </DataCell>
        </tr>
    );
};

export default ClusterRow;
