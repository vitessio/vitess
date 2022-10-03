import React from 'react';
import { vtctldata } from '../proto/vtadmin';

interface Props {
    resultsByKeyspace?: {
        [k: string]: vtctldata.IValidateKeyspaceResponse;
    };
    resultsByShard?: vtctldata.ValidateShardResponse;
    shard?: string;
}

const ValidationResults: React.FC<Props> = ({ resultsByKeyspace, resultsByShard, shard }) => {
    const hasShardResults = resultsByShard && resultsByShard.results.length > 0;
    return (
        <div className="text-left w-full">
            <table className="border-none bg-gray-100 ">
                <thead>
                    <tr className="bg-gray-100">
                        {resultsByKeyspace && <th className="text-base text-black bg-gray-100">Keyspace</th>}
                        {resultsByKeyspace && <th className="text-base text-black bg-gray-100">Shard</th>}
                        <th className="text-base text-black bg-gray-100">Result</th>
                    </tr>
                </thead>
                <tbody>
                    {resultsByKeyspace &&
                        Object.entries(resultsByKeyspace).map(([keyspace, results]) => {
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
                    {hasShardResults && (
                        <>
                            {resultsByShard.results.map((r) => (
                                <tr className="p-1">
                                    <td className="py-2">{r}</td>
                                </tr>
                            ))}
                        </>
                    )}
                    {!hasShardResults && (
                        <>
                            <tr className="p-1">
                                <td className="py-10 italic text-center">No results</td>
                            </tr>
                        </>
                    )}
                </tbody>
            </table>
        </div>
    );
};

export default ValidationResults;
