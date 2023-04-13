import React, { useState } from 'react';
import Dropdown from '../../dropdown/Dropdown';
import MenuItem from '../../dropdown/MenuItem';
import { Icons } from '../../Icon';
import Toggle from '../../toggle/Toggle';
import { useValidateKeyspace, useValidateSchemaKeyspace, useValidateVersionKeyspace } from '../../../hooks/api';
import KeyspaceAction from './KeyspaceAction';

interface KeyspaceActionsProps {
    keyspace: string;
    clusterID: string;
}

const KeyspaceActions: React.FC<KeyspaceActionsProps> = ({ keyspace, clusterID }) => {
    const [currentDialog, setCurrentDialog] = useState<string>('');
    const closeDialog = () => setCurrentDialog('');

    const [pingTablets, setPingTablets] = useState(false);
    const validateKeyspaceMutation = useValidateKeyspace({ keyspace, clusterID, pingTablets });

    const validateSchemaKeyspaceMutation = useValidateSchemaKeyspace({ keyspace, clusterID });

    const validateVersionKeyspaceMutation = useValidateVersionKeyspace({ keyspace, clusterID });

    return (
        <div className="w-min inline-block">
            <Dropdown dropdownButton={Icons.info} position="bottom-right">
                <MenuItem onClick={() => setCurrentDialog('Validate Keyspace')}>Validate Keyspace</MenuItem>
                <MenuItem onClick={() => setCurrentDialog('Validate Schema')}>Validate Schema</MenuItem>
                <MenuItem onClick={() => setCurrentDialog('Validate Version')}>Validate Version</MenuItem>
            </Dropdown>
            <KeyspaceAction
                title="Validate Keyspace"
                description={`Validates that all nodes reachable from keyspace "${keyspace}" are consistent.`}
                confirmText="Validate"
                loadingText="Validating"
                mutation={validateKeyspaceMutation}
                successText="Validated keyspace"
                errorText="Error validating keyspace"
                closeDialog={closeDialog}
                isOpen={currentDialog === 'Validate Keyspace'}
                body={
                    <div className="flex justify-between items-center w-full p-4 border border-vtblue rounded-md">
                        <div className="mr-2">
                            <h5 className="font-medium m-0 mb-2">Ping Tablets</h5>
                            <p className="m-0 text-sm">
                                If enabled, all tablets will also be pinged during the validation process.
                            </p>
                        </div>
                        <Toggle enabled={pingTablets} onChange={() => setPingTablets(!pingTablets)} />
                    </div>
                }
                successBody={
                    <div className="text-sm">
                        {validateKeyspaceMutation.data && validateKeyspaceMutation.data.results.length === 0 && (
                            <div className="text-sm">No validation errors found.</div>
                        )}
                        {validateKeyspaceMutation.data && validateKeyspaceMutation.data.results.length > 0 && (
                            <ul>
                                {validateKeyspaceMutation.data &&
                                    validateKeyspaceMutation.data.results.map((res, i) => (
                                        <li className="text-sm" key={`keyspace_validation_result_${i}`}>
                                            • {res}
                                        </li>
                                    ))}
                            </ul>
                        )}
                    </div>
                }
            />
            <KeyspaceAction
                title="Validate Schema"
                confirmText="Validate"
                loadingText="Validating"
                mutation={validateSchemaKeyspaceMutation}
                successText="Validated schemas in keyspace"
                errorText="Error validating schemas in keyspace"
                closeDialog={closeDialog}
                isOpen={currentDialog === 'Validate Schema'}
                body={
                    <div className="text-sm mt-3">
                        Validates that the schema on the primary tablet for shard 0 matches the schema on all of the
                        other tablets in the keyspace <span className="font-mono bg-gray-300">{keyspace}</span>.
                    </div>
                }
                successBody={
                    <div className="text-sm">
                        {validateSchemaKeyspaceMutation.data &&
                            validateSchemaKeyspaceMutation.data.results.length === 0 && (
                                <div className="text-sm">No schema validation errors found.</div>
                            )}
                        {validateSchemaKeyspaceMutation.data &&
                            validateSchemaKeyspaceMutation.data.results.length > 0 && (
                                <ul>
                                    {validateSchemaKeyspaceMutation.data &&
                                        validateSchemaKeyspaceMutation.data.results.map((res, i) => (
                                            <li className="text-sm" key={`schema_keyspace_validation_result_${i}`}>
                                                • {res}
                                            </li>
                                        ))}
                                </ul>
                            )}
                    </div>
                }
            />
            <KeyspaceAction
                title="Validate Version"
                confirmText="Validate"
                loadingText="Validating"
                mutation={validateVersionKeyspaceMutation}
                successText={`Validated versions of all tablets in keyspace ${keyspace}`}
                errorText={`Error validating versions in keyspace ${keyspace}`}
                closeDialog={closeDialog}
                isOpen={currentDialog === 'Validate Version'}
                body={
                    <div className="text-sm mt-3">
                        Validates that the version on the primary of shard 0 matches all of the other tablets in the
                        keyspace <span className="font-mono bg-gray-300">{keyspace}</span>.
                    </div>
                }
                successBody={
                    <div className="text-sm">
                        {validateVersionKeyspaceMutation.data &&
                            validateVersionKeyspaceMutation.data.results.length === 0 && (
                                <div className="text-sm">No version validation errors found.</div>
                            )}
                        {validateVersionKeyspaceMutation.data &&
                            validateVersionKeyspaceMutation.data.results.length > 0 && (
                                <ul>
                                    {validateVersionKeyspaceMutation.data &&
                                        validateVersionKeyspaceMutation.data.results.map((res, i) => (
                                            <li className="text-sm" key={`schema_keyspace_validation_result_${i}`}>
                                                • {res}
                                            </li>
                                        ))}
                                </ul>
                            )}
                    </div>
                }
            />
        </div>
    );
};

export default KeyspaceActions;
