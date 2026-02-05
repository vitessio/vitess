/**
 * Copyright 2025 The Vitess Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import { userEvent } from '@testing-library/user-event';
import { describe, it, expect, vi, beforeEach } from 'vitest';
import WorkflowActions from './WorkflowActions';
import { QueryClient, QueryClientProvider } from 'react-query';
import { vtadmin } from '../../../proto/vtadmin';

import * as httpAPI from '../../../api/http';

describe('WorkflowActions', () => {
    const queryClient = new QueryClient({
        defaultOptions: { queries: { retry: false } },
    });

    const stoppedWorkflowProps = {
        streamsByState: {
            Stopped: [{}],
        },
        workflows: [
            new vtadmin.Workflow({
                keyspace: 'test_keyspace',
            }),
        ],
        refetchWorkflows: vi.fn(),
        keyspace: 'test_keyspace',
        clusterID: 'test_cluster',
        name: 'test_workflow',
        workflowType: 'MoveTables',
    };

    const runningWorkflowProps = {
        streamsByState: {
            Running: [{}],
        },
        workflows: [
            new vtadmin.Workflow({
                keyspace: 'test_keyspace',
            }),
        ],
        refetchWorkflows: vi.fn(),
        keyspace: 'test_keyspace',
        clusterID: 'test_cluster',
        name: 'test_workflow',
        workflowType: 'MoveTables',
    };

    const switchedWorkflowProps = {
        streamsByState: {
            Stopped: [{}],
        },
        workflows: [
            new vtadmin.Workflow({
                keyspace: 'test_keyspace',
                cluster: {
                    id: 'test_cluster',
                },
                workflow: {
                    name: 'test_workflow',
                },
            }),
            new vtadmin.Workflow({
                keyspace: 'test_keyspace',
                cluster: {
                    id: 'test_cluster',
                },
                workflow: {
                    name: 'test_workflow_reverse',
                },
            }),
        ],
        refetchWorkflows: vi.fn(),
        keyspace: 'test_keyspace',
        clusterID: 'test_cluster',
        name: 'test_workflow',
        workflowType: 'MoveTables',
    };

    beforeEach(() => {
        // Mock ResizeObserver as a class (not arrow function) because it's called with 'new'
        // This is required for Vitest 4.x which requires constructors to use class/function keywords
        const ResizeObserverMock = vi.fn(function (this: any) {
            this.observe = vi.fn();
            this.unobserve = vi.fn();
            this.disconnect = vi.fn();
        });

        vi.stubGlobal('ResizeObserver', ResizeObserverMock);
        vi.restoreAllMocks();
    });

    it('test Start Workflow dialog and API', async () => {
        render(
            <QueryClientProvider client={queryClient}>
                <WorkflowActions {...stoppedWorkflowProps} />
            </QueryClientProvider>
        );

        fireEvent.click(screen.getByTestId('dropdown-btn'));
        fireEvent.click(screen.getByText('Start Workflow'));
        // expect workflow name to be in the modal/dialog.
        expect(screen.getByText('test_workflow')).toBeDefined();

        // verify the API parameters, if "Start" button is clicked.
        const startWorkflowSpy = vi.spyOn(httpAPI, 'startWorkflow');
        fireEvent.click(screen.getByText('Start'));

        await waitFor(() => {
            expect(startWorkflowSpy).toHaveBeenCalledTimes(1);
        });
        expect(startWorkflowSpy).toHaveBeenCalledWith({
            clusterID: 'test_cluster',
            keyspace: 'test_keyspace',
            name: 'test_workflow',
        });
    });

    it('test Stop Workflow dialog and API', async () => {
        render(
            <QueryClientProvider client={queryClient}>
                <WorkflowActions {...runningWorkflowProps} />
            </QueryClientProvider>
        );
        fireEvent.click(screen.getByTestId('dropdown-btn'));
        fireEvent.click(screen.getByText('Stop Workflow'));
        // expect workflow name to be in the modal/dialog.
        expect(screen.getByText('test_workflow')).toBeDefined();

        // verify the API parameters, if "Stop" button is clicked.
        const stopWorkflowSpy = vi.spyOn(httpAPI, 'stopWorkflow');
        fireEvent.click(screen.getByText('Stop'));

        await waitFor(() => {
            expect(stopWorkflowSpy).toHaveBeenCalledTimes(1);
        });
        expect(stopWorkflowSpy).toHaveBeenCalledWith({
            clusterID: 'test_cluster',
            keyspace: 'test_keyspace',
            name: 'test_workflow',
        });
    });

    it('test Complete Workflow dialog and API', async () => {
        render(
            <QueryClientProvider client={queryClient}>
                <WorkflowActions {...switchedWorkflowProps} />
            </QueryClientProvider>
        );
        fireEvent.click(screen.getByTestId('dropdown-btn'));
        fireEvent.click(screen.getByText('Complete'));
        // expect workflow name to be in the modal/dialog.
        expect(screen.getByText(/test_workflow/)).toBeDefined();

        // verify the API parameters, if "Complete" button is clicked.
        const completeWorkflowSpy = vi.spyOn(httpAPI, 'completeMoveTables');
        fireEvent.click(screen.getByTestId('confirm-btn'));

        await waitFor(() => {
            expect(completeWorkflowSpy).toHaveBeenCalledTimes(1);
        });
        expect(completeWorkflowSpy).toHaveBeenCalledWith({
            clusterID: 'test_cluster',
            request: {
                workflow: 'test_workflow',
                target_keyspace: 'test_keyspace',
                keep_data: false,
                keep_routing_rules: false,
                rename_tables: false,
            },
        });
    });

    it('test Complete Workflow with toggled options', async () => {
        render(
            <QueryClientProvider client={queryClient}>
                <WorkflowActions {...switchedWorkflowProps} />
            </QueryClientProvider>
        );
        fireEvent.click(screen.getByTestId('dropdown-btn'));
        fireEvent.click(screen.getByText('Complete'));
        // expect workflow name to be in the modal/dialog.
        expect(screen.getByText(/test_workflow/)).toBeDefined();

        // verify the API parameters, if "Complete" button is clicked.
        const completeWorkflowSpy = vi.spyOn(httpAPI, 'completeMoveTables');

        // Toggle keep_data.
        fireEvent.click(screen.getByTestId('toggle-keep-data'));

        // Toggle keep_routing_rules.
        fireEvent.click(screen.getByTestId('toggle-routing-rules'));

        // Toggle rename_tables.
        fireEvent.click(screen.getByTestId('toggle-rename-tables'));

        fireEvent.click(screen.getByTestId('confirm-btn'));

        await waitFor(() => {
            expect(completeWorkflowSpy).toHaveBeenCalledTimes(1);
        });
        expect(completeWorkflowSpy).toHaveBeenCalledWith({
            clusterID: 'test_cluster',
            request: {
                workflow: 'test_workflow',
                target_keyspace: 'test_keyspace',
                // Toggled options.
                keep_data: true,
                keep_routing_rules: true,
                rename_tables: true,
            },
        });
    });

    it('test Cancel Workflow dialog and API', async () => {
        render(
            <QueryClientProvider client={queryClient}>
                <WorkflowActions {...runningWorkflowProps} />
            </QueryClientProvider>
        );
        fireEvent.click(screen.getByTestId('dropdown-btn'));
        fireEvent.click(screen.getByText('Cancel Workflow'));
        // expect workflow name to be in the modal/dialog.
        expect(screen.getByText(/test_workflow/)).toBeDefined();

        // verify the API parameters, if "Confirm" button is clicked.
        const cancelWorkflowSpy = vi.spyOn(httpAPI, 'workflowDelete');
        fireEvent.click(screen.getByTestId('confirm-btn'));

        await waitFor(() => {
            expect(cancelWorkflowSpy).toHaveBeenCalledTimes(1);
        });
        expect(cancelWorkflowSpy).toHaveBeenCalledWith({
            clusterID: 'test_cluster',
            request: {
                workflow: 'test_workflow',
                keyspace: 'test_keyspace',
                keep_data: false,
                keep_routing_rules: false,
            },
        });
    });

    it('test Cancel Workflow dialog with toggled options', async () => {
        render(
            <QueryClientProvider client={queryClient}>
                <WorkflowActions {...runningWorkflowProps} />
            </QueryClientProvider>
        );
        fireEvent.click(screen.getByTestId('dropdown-btn'));
        fireEvent.click(screen.getByText('Cancel Workflow'));
        // expect workflow name to be in the modal/dialog.
        expect(screen.getByText(/test_workflow/)).toBeDefined();

        // verify the API parameters, if "Confirm" button is clicked.
        const cancelWorkflowSpy = vi.spyOn(httpAPI, 'workflowDelete');

        // Toggle keep_data.
        fireEvent.click(screen.getByTestId('toggle-keep-data'));

        // Toggle keep_routing_rules.
        fireEvent.click(screen.getByTestId('toggle-routing-rules'));

        fireEvent.click(screen.getByTestId('confirm-btn'));

        await waitFor(() => {
            expect(cancelWorkflowSpy).toHaveBeenCalledTimes(1);
        });
        expect(cancelWorkflowSpy).toHaveBeenCalledWith({
            clusterID: 'test_cluster',
            request: {
                workflow: 'test_workflow',
                keyspace: 'test_keyspace',
                // Toggled options.
                keep_data: true,
                keep_routing_rules: true,
            },
        });
    });

    it('test Switch Traffic dialog with default options', async () => {
        render(
            <QueryClientProvider client={queryClient}>
                <WorkflowActions {...runningWorkflowProps} />
            </QueryClientProvider>
        );
        fireEvent.click(screen.getByTestId('dropdown-btn'));
        fireEvent.click(screen.getByText('Switch Traffic'));
        // expect workflow name to be in the modal/dialog.
        expect(screen.getByText(/Switch traffic for the test_workflow workflow/)).toBeDefined();

        // verify the API parameters, if "Switch" button is clicked.
        const switchTrafficWorkflowSpy = vi.spyOn(httpAPI, 'workflowSwitchTraffic');
        fireEvent.click(screen.getByText('Switch'));

        await waitFor(() => {
            expect(switchTrafficWorkflowSpy).toHaveBeenCalledTimes(1);
        });
        expect(switchTrafficWorkflowSpy).toHaveBeenCalledWith({
            clusterID: 'test_cluster',
            request: {
                keyspace: 'test_keyspace',
                workflow: 'test_workflow',
                direction: 0,
                enable_reverse_replication: true,
                force: false,
                max_replication_lag_allowed: { seconds: 30 },
                timeout: { seconds: 30 },
                initialize_target_sequences: false,
                tablet_types: [1, 2, 3],
            },
        });
    });

    it('test Switch Traffic with different options', async () => {
        render(
            <QueryClientProvider client={queryClient}>
                <WorkflowActions {...runningWorkflowProps} />
            </QueryClientProvider>
        );
        fireEvent.click(screen.getByTestId('dropdown-btn'));
        fireEvent.click(screen.getByText('Switch Traffic'));
        // expect workflow name to be in the modal/dialog.
        expect(screen.getByText(/Switch traffic for the test_workflow workflow/)).toBeDefined();

        // Change timeout input.
        await userEvent.clear(screen.getByTestId('input-timeout'));
        await userEvent.type(screen.getByTestId('input-timeout'), '32');

        // Change max_replication_lag_allowed input.
        await userEvent.clear(screen.getByTestId('input-max-repl-lag-allowed'));
        await userEvent.type(screen.getByTestId('input-max-repl-lag-allowed'), '54');

        // Toggle enable_reverse_replication.
        fireEvent.click(screen.getByTestId('toggle-enable-reverse-repl'));

        // Toggle force.
        fireEvent.click(screen.getByTestId('toggle-force'));

        // Toggle initialize_target_sequences.
        fireEvent.click(screen.getByTestId('toggle-init-target-seq'));

        const switchTrafficWorkflowSpy = vi.spyOn(httpAPI, 'workflowSwitchTraffic');
        fireEvent.click(screen.getByText('Switch'));

        await waitFor(() => {
            expect(switchTrafficWorkflowSpy).toHaveBeenCalledTimes(1);
        });
        expect(switchTrafficWorkflowSpy).toHaveBeenCalledWith({
            clusterID: 'test_cluster',
            request: {
                keyspace: 'test_keyspace',
                workflow: 'test_workflow',
                direction: 0,
                // Expect enable_reverse_replication to be toggled.
                enable_reverse_replication: false,
                // Expect force to be toggled.
                force: true,
                // Expect timeout to be changed to '54' sec.
                max_replication_lag_allowed: { seconds: 54 },
                // Expect timeout to be changed to '32' sec.
                timeout: { seconds: 32 },
                // Expect initialize_target_sequences to be changed to true.
                initialize_target_sequences: true,
                tablet_types: [1, 2, 3],
            },
        });
    });

    it('test Reverse Traffic dialog with default options', async () => {
        render(
            <QueryClientProvider client={queryClient}>
                <WorkflowActions {...switchedWorkflowProps} />
            </QueryClientProvider>
        );
        fireEvent.click(screen.getByTestId('dropdown-btn'));
        fireEvent.click(screen.getByText('Reverse Traffic'));
        // expect workflow name to be in the modal/dialog.
        expect(screen.getByText(/Reverse traffic for the test_workflow workflow/)).toBeDefined();

        // verify the API parameters, if "Switch" button is clicked.
        const switchTrafficWorkflowSpy = vi.spyOn(httpAPI, 'workflowSwitchTraffic');
        fireEvent.click(screen.getByText('Reverse'));

        await waitFor(() => {
            expect(switchTrafficWorkflowSpy).toHaveBeenCalledTimes(1);
        });
        expect(switchTrafficWorkflowSpy).toHaveBeenCalledWith({
            clusterID: 'test_cluster',
            request: {
                keyspace: 'test_keyspace',
                workflow: 'test_workflow',
                direction: 1,
                enable_reverse_replication: true,
                force: false,
                max_replication_lag_allowed: { seconds: 30 },
                timeout: { seconds: 30 },
                initialize_target_sequences: false,
                tablet_types: [1, 2, 3],
            },
        });
    });

    it('test Reverse Traffic with different options', async () => {
        render(
            <QueryClientProvider client={queryClient}>
                <WorkflowActions {...switchedWorkflowProps} />
            </QueryClientProvider>
        );
        fireEvent.click(screen.getByTestId('dropdown-btn'));
        fireEvent.click(screen.getByText('Reverse Traffic'));
        // expect workflow name to be in the modal/dialog.
        expect(screen.getByText(/Reverse traffic for the test_workflow workflow/)).toBeDefined();

        // Change timeout input.
        await userEvent.clear(screen.getByTestId('input-timeout'));
        await userEvent.type(screen.getByTestId('input-timeout'), '32');

        // Change max_replication_lag_allowed input.
        await userEvent.clear(screen.getByTestId('input-max-repl-lag-allowed'));
        await userEvent.type(screen.getByTestId('input-max-repl-lag-allowed'), '54');

        // Toggle enable_reverse_replication.
        fireEvent.click(screen.getByTestId('toggle-enable-reverse-repl'));

        // Toggle force.
        fireEvent.click(screen.getByTestId('toggle-force'));

        const switchTrafficWorkflowSpy = vi.spyOn(httpAPI, 'workflowSwitchTraffic');
        fireEvent.click(screen.getByText('Reverse'));

        await waitFor(() => {
            expect(switchTrafficWorkflowSpy).toHaveBeenCalledTimes(1);
        });
        expect(switchTrafficWorkflowSpy).toHaveBeenCalledWith({
            clusterID: 'test_cluster',
            request: {
                keyspace: 'test_keyspace',
                workflow: 'test_workflow',
                direction: 1,
                // Expect enable_reverse_replication to be toggled.
                enable_reverse_replication: false,
                // Expect force to be toggled.
                force: true,
                // Expect timeout to be changed to '54' sec.
                max_replication_lag_allowed: { seconds: 54 },
                // Expect timeout to be changed to '32' sec.
                timeout: { seconds: 32 },
                initialize_target_sequences: false,
                tablet_types: [1, 2, 3],
            },
        });
    });
});
