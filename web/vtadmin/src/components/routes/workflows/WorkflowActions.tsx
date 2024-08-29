import React, { useState } from 'react';
import Dropdown from '../../dropdown/Dropdown';
import MenuItem from '../../dropdown/MenuItem';
import { Icons } from '../../Icon';
import WorkflowAction from './WorkflowAction';
import { useStartWorkflow, useStopWorkflow } from '../../../hooks/api';

interface WorkflowActionsProps {
    refetchWorkflows: Function;
    keyspace: string;
    clusterID: string;
    name: string;
}

const WorkflowActions: React.FC<WorkflowActionsProps> = ({ refetchWorkflows, keyspace, clusterID, name }) => {
    const [currentDialog, setCurrentDialog] = useState<string>('');
    const closeDialog = () => setCurrentDialog('');

    const startWorkflowMutation = useStartWorkflow({ keyspace, clusterID, name });

    const stopWorkflowMutation = useStopWorkflow({ keyspace, clusterID, name });

    return (
        <div className="w-min inline-block">
            <Dropdown dropdownButton={Icons.info} position="bottom-right">
                <MenuItem onClick={() => setCurrentDialog('Start Workflow')}>Start Workflow</MenuItem>
                <MenuItem onClick={() => setCurrentDialog('Stop Workflow')}>Stop Workflow</MenuItem>
            </Dropdown>
            <WorkflowAction
                title="Start Workflow"
                confirmText="Start"
                loadingText="Starting"
                mutation={startWorkflowMutation}
                successText="Started workflow"
                errorText={`Error occured while starting workflow ${name}`}
                closeDialog={closeDialog}
                isOpen={currentDialog === 'Start Workflow'}
                refetchWorkflows={refetchWorkflows}
                successBody={
                    <div className="text-sm">
                        {startWorkflowMutation.data && startWorkflowMutation.data.summary && (
                            <div className="text-sm">{startWorkflowMutation.data.summary}</div>
                        )}
                    </div>
                }
                body={
                    <div className="text-sm mt-3">
                        Starts the <span className="font-mono bg-gray-300">{name}</span> workflow.
                    </div>
                }
            />
            <WorkflowAction
                title="Stop Workflow"
                confirmText="Stop"
                loadingText="Stopping"
                mutation={stopWorkflowMutation}
                successText="Stopped workflow"
                errorText={`Error occured while stopping workflow ${name}`}
                closeDialog={closeDialog}
                isOpen={currentDialog === 'Stop Workflow'}
                refetchWorkflows={refetchWorkflows}
                successBody={
                    <div className="text-sm">
                        {stopWorkflowMutation.data && stopWorkflowMutation.data.summary && (
                            <div className="text-sm">{stopWorkflowMutation.data.summary}</div>
                        )}
                    </div>
                }
                body={
                    <div className="text-sm mt-3">
                        Stops the <span className="font-mono bg-gray-300">{name}</span> workflow.
                    </div>
                }
            />
        </div>
    );
};

export default WorkflowActions;
