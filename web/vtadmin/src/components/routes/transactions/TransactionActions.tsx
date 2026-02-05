import React, { useState } from 'react';
import Dropdown from '../../dropdown/Dropdown';
import MenuItem from '../../dropdown/MenuItem';
import { Icons } from '../../Icon';
import TransactionAction from './TransactionAction';
import { useConcludeTransaction } from '../../../hooks/api';

interface TransactionActionsProps {
    refetchTransactions: Function;
    clusterID: string;
    dtid: string;
}

const TransactionActions: React.FC<TransactionActionsProps> = ({ refetchTransactions, clusterID, dtid }) => {
    const [currentDialog, setCurrentDialog] = useState<string>('');
    const closeDialog = () => setCurrentDialog('');

    const concludeTransactionMutation = useConcludeTransaction({ clusterID, dtid });

    return (
        <div className="w-min inline-block">
            <Dropdown dropdownButton={Icons.info}>
                <MenuItem onClick={() => setCurrentDialog('Conclude Transaction')}>Conclude Transaction</MenuItem>
            </Dropdown>
            <TransactionAction
                title="Conclude Transaction"
                confirmText="Conclude"
                loadingText="Concluding"
                mutation={concludeTransactionMutation}
                successText="Concluded transaction"
                errorText={`Error occured while concluding the transaction (ID: ${dtid})`}
                closeDialog={closeDialog}
                isOpen={currentDialog === 'Conclude Transaction'}
                refetchTransactions={refetchTransactions}
                body={
                    <div className="text-sm mt-3">
                        Conclude the transaction with id: <span className="font-mono bg-gray-300">{dtid}</span>.
                    </div>
                }
            />
        </div>
    );
};

export default TransactionActions;
