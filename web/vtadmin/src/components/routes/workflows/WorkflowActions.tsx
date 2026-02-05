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
import { topodata, vtadmin, vtctldata } from '../../../proto/vtadmin';
import { getReverseWorkflow } from '../../../util/workflows';
import { Label } from '../../inputs/Label';
import { TextInput } from '../../TextInput';
import { MultiSelect } from '../../inputs/MultiSelect';
import { TABLET_TYPES } from '../../../util/tablets';

interface WorkflowActionsProps {
    streamsByState: {
        [index: string]: vtctldata.Workflow.IStream[];
    };
    workflows?: vtadmin.Workflow[];
    refetchWorkflows: Function;
    keyspace: string;
    clusterID: string;
    name: string;
    workflowType: string;
}

interface CompleteMoveTablesOptions {
    keepData: boolean;
    keepRoutingRules: boolean;
    renameTables: boolean;
}

const DefaultCompleteMoveTablesOptions: CompleteMoveTablesOptions = {
    keepData: false,
    keepRoutingRules: false,
    renameTables: false,
};

interface CancelWorkflowOptions {
    keepData: boolean;
    keepRoutingRules: boolean;
}

interface SwitchTrafficOptions {
    enableReverseReplication: boolean;
    force: boolean;
    initializeTargetSequences: boolean;
    maxReplicationLagAllowed: number;
    timeout: number;
    tabletTypes: topodata.TabletType[];
}

const TABLET_OPTIONS = [topodata.TabletType['PRIMARY'], topodata.TabletType['REPLICA'], topodata.TabletType['RDONLY']];

const DefaultSwitchTrafficOptions: SwitchTrafficOptions = {
    enableReverseReplication: true,
    force: false,
    initializeTargetSequences: false,
    maxReplicationLagAllowed: 30,
    timeout: 30,
    tabletTypes: TABLET_OPTIONS,
};

const DefaultCancelWorkflowOptions: CancelWorkflowOptions = {
    keepData: false,
    keepRoutingRules: false,
};

const WorkflowActions: React.FC<WorkflowActionsProps> = ({
    streamsByState,
    workflows,
    refetchWorkflows,
    keyspace,
    clusterID,
    name,
    workflowType,
}) => {
    const [currentDialog, setCurrentDialog] = useState<string>('');

    const [completeMoveTablesOptions, setCompleteMoveTablesOptions] = useState<CompleteMoveTablesOptions>(
        DefaultCompleteMoveTablesOptions
    );

    const [cancelWorkflowOptions, setCancelWorkflowOptions] =
        useState<CancelWorkflowOptions>(DefaultCancelWorkflowOptions);

    const [switchTrafficOptions, setSwitchTrafficOptions] = useState<SwitchTrafficOptions>(DefaultSwitchTrafficOptions);

    const closeDialog = () => setCurrentDialog('');

    const startWorkflowMutation = useStartWorkflow({ keyspace, clusterID, name });

    const stopWorkflowMutation = useStopWorkflow({ keyspace, clusterID, name });

    const switchTrafficMutation = useWorkflowSwitchTraffic({
        clusterID,
        request: {
            keyspace: keyspace,
            workflow: name,
            direction: 0,
            enable_reverse_replication: switchTrafficOptions.enableReverseReplication,
            force: switchTrafficOptions.force,
            max_replication_lag_allowed: {
                seconds: switchTrafficOptions.maxReplicationLagAllowed,
            },
            timeout: {
                seconds: switchTrafficOptions.timeout,
            },
            initialize_target_sequences: switchTrafficOptions.initializeTargetSequences,
            tablet_types: switchTrafficOptions.tabletTypes,
        },
    });

    const reverseTrafficMutation = useWorkflowSwitchTraffic({
        clusterID,
        request: {
            keyspace: keyspace,
            workflow: name,
            direction: 1,
            enable_reverse_replication: switchTrafficOptions.enableReverseReplication,
            force: switchTrafficOptions.force,
            max_replication_lag_allowed: {
                seconds: switchTrafficOptions.maxReplicationLagAllowed,
            },
            timeout: {
                seconds: switchTrafficOptions.timeout,
            },
            initialize_target_sequences: switchTrafficOptions.initializeTargetSequences,
            tablet_types: switchTrafficOptions.tabletTypes,
        },
    });

    const cancelWorkflowMutation = useWorkflowDelete(
        {
            clusterID,
            request: {
                keyspace: keyspace,
                workflow: name,
                keep_data: cancelWorkflowOptions.keepData,
                keep_routing_rules: cancelWorkflowOptions.keepRoutingRules,
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
                keep_routing_rules: completeMoveTablesOptions.keepRoutingRules,
                rename_tables: completeMoveTablesOptions.renameTables,
            },
        },
        {
            onSuccess: (data) => {
                success(data.summary, { autoClose: 1600 });
            },
        }
    );

    const supportsComplete = workflowType.toLowerCase() === 'movetables' || workflowType.toLowerCase() === 'reshard';

    const isRunning =
        !(streamsByState['Error'] && streamsByState['Error'].length) &&
        !(streamsByState['Copying'] && streamsByState['Copying'].length) &&
        !(streamsByState['Stopped'] && streamsByState['Stopped'].length);

    const isStopped =
        !(streamsByState['Error'] && streamsByState['Error'].length) &&
        !(streamsByState['Copying'] && streamsByState['Copying'].length) &&
        !(streamsByState['Running'] && streamsByState['Running'].length);

    const isSwitched =
        workflows &&
        isStopped &&
        !!getReverseWorkflow(
            workflows,
            workflows.find((w) => w.workflow?.name === name && w.cluster?.id === clusterID)
        );

    const isReverseWorkflow = name.endsWith('_reverse');

    const switchTrafficDialogBody = (initializeTargetSequences: boolean = true) => {
        return (
            <div className="flex flex-col gap-2">
                <Label
                    className="block w-full"
                    label="Timeout"
                    helpText={
                        'Specifies the maximum time to wait, in seconds, for VReplication to catch up on primary tablets. The traffic switch will be cancelled on timeout.'
                    }
                >
                    <TextInput
                        onChange={(e) =>
                            setSwitchTrafficOptions((prevOptions) => ({
                                ...prevOptions,
                                timeout: Number(e.target.value),
                            }))
                        }
                        value={switchTrafficOptions.timeout || ''}
                        type="number"
                        data-testid="input-timeout"
                    />
                </Label>
                <Label
                    className="block w-full"
                    label="Max Replication Lag Allowed"
                    helpText={'Allow traffic to be switched only if VReplication lag is below this.'}
                >
                    <TextInput
                        onChange={(e) =>
                            setSwitchTrafficOptions((prevOptions) => ({
                                ...prevOptions,
                                maxReplicationLagAllowed: Number(e.target.value),
                            }))
                        }
                        value={switchTrafficOptions.maxReplicationLagAllowed || ''}
                        type="number"
                        data-testid="input-max-repl-lag-allowed"
                    />
                </Label>
                <MultiSelect
                    className="block w-full"
                    inputClassName="block w-full"
                    items={TABLET_OPTIONS}
                    itemToString={(tt) => TABLET_TYPES[tt]}
                    selectedItems={switchTrafficOptions.tabletTypes}
                    label="Tablet Types"
                    helpText={'Tablet types to switch traffic for.'}
                    onChange={(types) => {
                        setSwitchTrafficOptions({ ...switchTrafficOptions, tabletTypes: types });
                    }}
                    placeholder="Select tablet types"
                />
                <div className="flex justify-between items-center w-full">
                    <Label
                        label="Enable Reverse Replication"
                        helpText={
                            'Setup replication going back to the original source keyspace to support rolling back the traffic cutover.'
                        }
                    />
                    <Toggle
                        className="mr-2"
                        data-testid="toggle-enable-reverse-repl"
                        enabled={switchTrafficOptions.enableReverseReplication}
                        onChange={() =>
                            setSwitchTrafficOptions((prevOptions) => ({
                                ...prevOptions,
                                enableReverseReplication: !prevOptions.enableReverseReplication,
                            }))
                        }
                    />
                </div>
                {initializeTargetSequences && (
                    <div className="flex justify-between items-center w-full">
                        <Label
                            label="Initialize Target Sequences"
                            helpText={
                                'When moving tables from an unsharded keyspace to a sharded keyspace, initialize any sequences that are being used on the target when switching writes. If the sequence table is not found, and the sequence table reference was fully qualified, then we will attempt to create the sequence table in that keyspace.'
                            }
                        />
                        <Toggle
                            className="mr-2"
                            data-testid="toggle-init-target-seq"
                            enabled={switchTrafficOptions.initializeTargetSequences}
                            onChange={() =>
                                setSwitchTrafficOptions((prevOptions) => ({
                                    ...prevOptions,
                                    initializeTargetSequences: !prevOptions.initializeTargetSequences,
                                }))
                            }
                        />
                    </div>
                )}
                <div className="flex justify-between items-center w-full">
                    <Label
                        label="Force"
                        helpText={`
                        Force the traffic switch even if some potentially non-critical actions cannot be
                        performed; for example the tablet refresh fails on some tablets in the keyspace.
                        WARNING: this should be used with extreme caution and only in emergency situations!`}
                    />
                    <Toggle
                        className="mr-2"
                        enabled={switchTrafficOptions.force}
                        data-testid="toggle-force"
                        onChange={() =>
                            setSwitchTrafficOptions((prevOptions) => ({
                                ...prevOptions,
                                force: !prevOptions.force,
                            }))
                        }
                    />
                </div>
            </div>
        );
    };

    return (
        <div className="w-min inline-block">
            <Dropdown dropdownButton={Icons.info}>
                {!isReverseWorkflow && (
                    <>
                        {supportsComplete && isSwitched && (
                            <MenuItem onClick={() => setCurrentDialog('Complete Workflow')}>Complete</MenuItem>
                        )}
                        {isRunning && (
                            <MenuItem onClick={() => setCurrentDialog('Switch Traffic')}>Switch Traffic</MenuItem>
                        )}
                        {isSwitched && (
                            <MenuItem onClick={() => setCurrentDialog('Reverse Traffic')}>Reverse Traffic</MenuItem>
                        )}
                        {!isSwitched && (
                            <MenuItem onClick={() => setCurrentDialog('Cancel Workflow')}>Cancel Workflow</MenuItem>
                        )}
                    </>
                )}
                {!isRunning && <MenuItem onClick={() => setCurrentDialog('Start Workflow')}>Start Workflow</MenuItem>}
                {!isStopped && <MenuItem onClick={() => setCurrentDialog('Stop Workflow')}>Stop Workflow</MenuItem>}
            </Dropdown>
            <WorkflowAction
                title="Start Workflow"
                confirmText="Start"
                loadingText="Starting"
                mutation={startWorkflowMutation}
                successText="Started workflow"
                errorText={`Error occured while starting workflow ${name}`}
                errorDescription={startWorkflowMutation.error ? startWorkflowMutation.error.message : ''}
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
                errorDescription={stopWorkflowMutation.error ? stopWorkflowMutation.error.message : ''}
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
                className="sm:max-w-xl"
                title="Switch Traffic"
                confirmText="Switch"
                loadingText="Switching"
                mutation={switchTrafficMutation}
                description={`Switch traffic for the ${name} workflow.`}
                successText="Switched Traffic"
                errorText={`Error occured while switching traffic for workflow ${name}`}
                errorDescription={switchTrafficMutation.error ? switchTrafficMutation.error.message : ''}
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
                body={switchTrafficDialogBody(workflowType.toLowerCase() !== 'reshard')}
            />
            <WorkflowAction
                className="sm:max-w-xl"
                title="Reverse Traffic"
                confirmText="Reverse"
                loadingText="Reversing"
                mutation={reverseTrafficMutation}
                description={`Reverse traffic for the ${name} workflow.`}
                successText="Reversed Traffic"
                errorText={`Error occured while reversing traffic for workflow ${name}`}
                errorDescription={reverseTrafficMutation.error ? reverseTrafficMutation.error.message : ''}
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
                body={switchTrafficDialogBody(false)}
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
                errorDescription={cancelWorkflowMutation.error ? cancelWorkflowMutation.error.message : ''}
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
                                data-testid="toggle-keep-data"
                                onChange={() =>
                                    setCancelWorkflowOptions((prevOptions) => ({
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
                                enabled={cancelWorkflowOptions.keepRoutingRules}
                                data-testid="toggle-routing-rules"
                                onChange={() =>
                                    setCancelWorkflowOptions((prevOptions) => ({
                                        ...prevOptions,
                                        keepRoutingRules: !prevOptions.keepRoutingRules,
                                    }))
                                }
                            />
                        </div>
                    </div>
                }
            />
            <WorkflowAction
                className="sm:max-w-xl"
                title="Complete Workflow"
                description={`Complete the ${name} workflow.`}
                confirmText="Complete"
                loadingText="Completing"
                hideSuccessDialog={true}
                mutation={completeMoveTablesMutation}
                errorText={`Error occured while completing workflow ${name}`}
                errorDescription={completeMoveTablesMutation.error ? completeMoveTablesMutation.error.message : ''}
                closeDialog={closeDialog}
                isOpen={currentDialog === 'Complete Workflow'}
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
                                data-testid="toggle-keep-data"
                                onChange={() =>
                                    setCompleteMoveTablesOptions((prevOptions) => ({
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
                                enabled={completeMoveTablesOptions.keepRoutingRules}
                                data-testid="toggle-routing-rules"
                                onChange={() =>
                                    setCompleteMoveTablesOptions((prevOptions) => ({
                                        ...prevOptions,
                                        keepRoutingRules: !prevOptions.keepRoutingRules,
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
                                data-testid="toggle-rename-tables"
                                onChange={() =>
                                    setCompleteMoveTablesOptions((prevOptions) => ({
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
