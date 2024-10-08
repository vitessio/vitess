import React, { useState } from 'react';
import Dropdown from '../../dropdown/Dropdown';
import MenuItem from '../../dropdown/MenuItem';
import { Icons } from '../../Icon';
import WorkflowAction from './WorkflowAction';
import {
    useCompleteMoveTables,
    useStartWorkflow,
    useStopWorkflow,
    useWorkflowDelete,
    useWorkflowSwitchTraffic,
} from '../../../hooks/api';
import Toggle from '../../toggle/Toggle';
import { success } from '../../Snackbar';

interface WorkflowActionsProps {
    refetchWorkflows: Function;
    keyspace: string;
    clusterID: string;
    name: string;
    workflowType: string;
}

interface CompleteMoveTablesOptions {
    keepData: boolean;
    keepRoutingRoules: boolean;
    renameTables: boolean;
}

const DefaultCompleteMoveTablesOptions: CompleteMoveTablesOptions = {
    keepData: false,
    keepRoutingRoules: false,
    renameTables: false,
};

interface CancelWorkflowOptions {
    keepData: boolean;
    keepRoutingRoules: boolean;
}

const DefaultCancelWorkflowOptions: CancelWorkflowOptions = {
    keepData: false,
    keepRoutingRoules: false,
};

const WorkflowActions: React.FC<WorkflowActionsProps> = ({
    refetchWorkflows,
    keyspace,
    clusterID,
    name,
    workflowType,
}) => {
    const [currentDialog, setCurrentDialog] = useState<string>('');

    const [completeMoveTablesOptions, SetCompleteMoveTablesOptions] = useState<CompleteMoveTablesOptions>(
        DefaultCompleteMoveTablesOptions
    );

    const [cancelWorkflowOptions, SetCancelWorkflowOptions] =
        useState<CancelWorkflowOptions>(DefaultCancelWorkflowOptions);

    const closeDialog = () => setCurrentDialog('');

    const startWorkflowMutation = useStartWorkflow({ keyspace, clusterID, name });

    const stopWorkflowMutation = useStopWorkflow({ keyspace, clusterID, name });

    const switchTrafficMutation = useWorkflowSwitchTraffic({
        clusterID,
        request: {
            keyspace: keyspace,
            workflow: name,
            direction: 0,
        },
    });

    const reverseTrafficMutation = useWorkflowSwitchTraffic({
        clusterID,
        request: {
            keyspace: keyspace,
            workflow: name,
            direction: 1,
        },
    });

    const cancelWorkflowMutation = useWorkflowDelete(
        {
            clusterID,
            request: {
                keyspace: keyspace,
                workflow: name,
                keep_data: cancelWorkflowOptions.keepData,
                keep_routing_rules: cancelWorkflowOptions.keepRoutingRoules,
            },
        },
        {
            onSuccess: (data) => {
                success(data.summary, { autoClose: 1600 });
            },
        }
    );

    const completeMoveTablesMutation = useCompleteMoveTables(
        {
            clusterID,
            request: {
                workflow: name,
                target_keyspace: keyspace,
                keep_data: completeMoveTablesOptions.keepData,
                keep_routing_rules: completeMoveTablesOptions.keepRoutingRoules,
                rename_tables: completeMoveTablesOptions.renameTables,
            },
        },
        {
            onSuccess: (data) => {
                success(data.summary, { autoClose: 1600 });
            },
        }
    );

    const isMoveTablesWorkflow = workflowType === 'MoveTables';

    return (
        <div className="w-min inline-block">
            <Dropdown dropdownButton={Icons.info}>
                {isMoveTablesWorkflow && (
                    <MenuItem onClick={() => setCurrentDialog('Complete MoveTables')}>Complete</MenuItem>
                )}
                <MenuItem onClick={() => setCurrentDialog('Switch Traffic')}>Switch Traffic</MenuItem>
                <MenuItem onClick={() => setCurrentDialog('Reverse Traffic')}>Reverse Traffic</MenuItem>
                <MenuItem onClick={() => setCurrentDialog('Cancel Workflow')}>Cancel Workflow</MenuItem>
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
                        Start the <span className="font-mono bg-gray-300">{name}</span> workflow.
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
                        Stop the <span className="font-mono bg-gray-300">{name}</span> workflow.
                    </div>
                }
            />
            <WorkflowAction
                title="Switch Traffic"
                confirmText="Switch"
                loadingText="Switching"
                mutation={switchTrafficMutation}
                successText="Switched Traffic"
                errorText={`Error occured while switching traffic for workflow ${name}`}
                closeDialog={closeDialog}
                isOpen={currentDialog === 'Switch Traffic'}
                refetchWorkflows={refetchWorkflows}
                successBody={
                    <div className="text-sm">
                        {switchTrafficMutation.data && switchTrafficMutation.data.summary && (
                            <div className="text-sm">{switchTrafficMutation.data.summary}</div>
                        )}
                    </div>
                }
                body={
                    <div className="text-sm mt-3">
                        Switch traffic for the <span className="font-mono bg-gray-300">{name}</span> workflow.
                    </div>
                }
            />
            <WorkflowAction
                title="Reverse Traffic"
                confirmText="Reverse"
                loadingText="Reversing"
                mutation={reverseTrafficMutation}
                successText="Reversed Traffic"
                errorText={`Error occured while reversing traffic for workflow ${name}`}
                closeDialog={closeDialog}
                isOpen={currentDialog === 'Reverse Traffic'}
                refetchWorkflows={refetchWorkflows}
                successBody={
                    <div className="text-sm">
                        {reverseTrafficMutation.data && reverseTrafficMutation.data.summary && (
                            <div className="text-sm">{reverseTrafficMutation.data.summary}</div>
                        )}
                    </div>
                }
                body={
                    <div className="text-sm mt-3">
                        Reverse traffic for the <span className="font-mono bg-gray-300">{name}</span> workflow.
                    </div>
                }
            />
            <WorkflowAction
                className="sm:max-w-xl"
                title="Cancel Workflow"
                description={`Cancel the ${name} workflow.`}
                confirmText="Confirm"
                loadingText="Cancelling"
                mutation={cancelWorkflowMutation}
                successText="Cancel Workflow"
                errorText={`Error occured while cancelling workflow ${name}`}
                closeDialog={closeDialog}
                isOpen={currentDialog === 'Cancel Workflow'}
                refetchWorkflows={refetchWorkflows}
                successBody={
                    <div className="text-sm">
                        {cancelWorkflowMutation.data && cancelWorkflowMutation.data.summary && (
                            <div className="text-sm">{cancelWorkflowMutation.data.summary}</div>
                        )}
                    </div>
                }
                body={
                    <div className="flex flex-col gap-2">
                        <div className="flex justify-between items-center w-full p-4 border border-vtblue rounded-md">
                            <div className="mr-2">
                                <h5 className="font-medium m-0 mb-2">Keep Data</h5>
                                <p className="m-0 text-sm">
                                    Keep the partially copied table data from the MoveTables workflow in the target
                                    keyspace.
                                </p>
                            </div>
                            <Toggle
                                enabled={cancelWorkflowOptions.keepData}
                                onChange={() =>
                                    SetCancelWorkflowOptions((prevOptions) => ({
                                        ...prevOptions,
                                        keepData: !prevOptions.keepData,
                                    }))
                                }
                            />
                        </div>
                        <div className="flex justify-between items-center w-full p-4 border border-vtblue rounded-md">
                            <div className="mr-2">
                                <h5 className="font-medium m-0 mb-2">Keep Routing Rules</h5>
                                <p className="m-0 text-sm">
                                    Keep the routing rules created for the MoveTables workflow.
                                </p>
                            </div>
                            <Toggle
                                enabled={cancelWorkflowOptions.keepRoutingRoules}
                                onChange={() =>
                                    SetCancelWorkflowOptions((prevOptions) => ({
                                        ...prevOptions,
                                        keepRoutingRoules: !prevOptions.keepRoutingRoules,
                                    }))
                                }
                            />
                        </div>
                    </div>
                }
            />
            <WorkflowAction
                className="sm:max-w-xl"
                title="Complete MoveTables"
                description={`Complete the ${name} workflow.`}
                confirmText="Complete"
                loadingText="Completing"
                hideSuccessDialog={true}
                mutation={completeMoveTablesMutation}
                errorText={`Error occured while completing workflow ${name}`}
                errorDescription={completeMoveTablesMutation.error ? completeMoveTablesMutation.error.message : ''}
                closeDialog={closeDialog}
                isOpen={currentDialog === 'Complete MoveTables'}
                refetchWorkflows={refetchWorkflows}
                body={
                    <div className="flex flex-col gap-2">
                        <div className="flex justify-between items-center w-full p-4 border border-vtblue rounded-md">
                            <div className="mr-2">
                                <h5 className="font-medium m-0 mb-2">Keep Data</h5>
                                <p className="m-0 text-sm">
                                    Keep the original source table data that was copied by the MoveTables workflow.
                                </p>
                            </div>
                            <Toggle
                                enabled={completeMoveTablesOptions.keepData}
                                onChange={() =>
                                    SetCompleteMoveTablesOptions((prevOptions) => ({
                                        ...prevOptions,
                                        keepData: !prevOptions.keepData,
                                    }))
                                }
                            />
                        </div>
                        <div className="flex justify-between items-center w-full p-4 border border-vtblue rounded-md">
                            <div className="mr-2">
                                <h5 className="font-medium m-0 mb-2">Keep Routing Rules</h5>
                                <p className="m-0 text-sm">
                                    Keep the routing rules in place that direct table traffic from the source keyspace
                                    to the target keyspace of the MoveTables workflow.
                                </p>
                            </div>
                            <Toggle
                                enabled={completeMoveTablesOptions.keepRoutingRoules}
                                onChange={() =>
                                    SetCompleteMoveTablesOptions((prevOptions) => ({
                                        ...prevOptions,
                                        keepRoutingRoules: !prevOptions.keepRoutingRoules,
                                    }))
                                }
                            />
                        </div>
                        <div className="flex justify-between items-center w-full p-4 border border-vtblue rounded-md">
                            <div className="mr-2">
                                <h5 className="font-medium m-0 mb-2">Rename Tables</h5>
                                <p className="m-0 text-sm">
                                    Keep the original source table data that was copied by the MoveTables workflow, but
                                    rename each table to{' '}
                                    <span className="font-mono bg-gray-300">{'_<tablename>_old'}</span>.
                                </p>
                            </div>
                            <Toggle
                                enabled={completeMoveTablesOptions.renameTables}
                                onChange={() =>
                                    SetCompleteMoveTablesOptions((prevOptions) => ({
                                        ...prevOptions,
                                        renameTables: !prevOptions.renameTables,
                                    }))
                                }
                            />
                        </div>
                    </div>
                }
            />
        </div>
    );
};

export default WorkflowActions;
