import React, { useState } from 'react';
import { useParams, Link, useHistory } from 'react-router-dom';
import {
    useDeleteShard,
    useEmergencyFailoverShard,
    useKeyspace,
    usePlannedFailoverShard,
    useReloadSchemaShard,
    useTabletExternallyPromoted,
    useTablets,
    useValidateShard,
    useValidateVersionShard,
} from '../../../hooks/api';
import { useDocumentTitle } from '../../../hooks/useDocumentTitle';
import ActionPanel from '../../ActionPanel';
import { success, warn } from '../../Snackbar';
import { UseMutationResult } from 'react-query';
import Toggle from '../../toggle/Toggle';
import { Label } from '../../inputs/Label';
import { TextInput } from '../../TextInput';
import { NumberInput } from '../../NumberInput';
import { Select } from '../../inputs/Select';
import { formatAlias, formatDisplayType } from '../../../util/tablets';
import { logutil, vtadmin, vtctldata } from '../../../proto/vtadmin';
import Dialog from '../../dialog/Dialog';
import EventLogEntry from './EventLogEntry';
import ValidationResults from '../../ValidationResults';
interface RouteParams {
    clusterID: string;
    keyspace: string;
    shard: string;
}

const Advanced: React.FC = () => {
    const params = useParams<RouteParams>();
    const history = useHistory();

    const shardName = `${params.keyspace}/${params.shard}`;

    useDocumentTitle(`${shardName} (${params.clusterID})`);

    const { data: keyspace, ...kq } = useKeyspace({ clusterID: params.clusterID, name: params.keyspace });
    const { data: tablets = [] } = useTablets();

    // dialog parameters
    const [failoverDialogIsOpen, setFailoverDialogIsOpen] = useState(false);
    const [dialogTitle, setDialogTitle] = useState('');
    const [dialogDescription, setDialogDescription] = useState('');
    const [events, setEvents] = useState<logutil.IEvent[]>([]);

    const tabletsInCluster = tablets.filter(
        (t) =>
            t.cluster?.id === params.clusterID &&
            t.tablet?.keyspace === params.keyspace &&
            t.tablet?.shard === params.shard
    );

    // deleteShard parameters
    const [evenIfServing, setEvenIfServing] = useState(false);
    const [recursive, setRecursive] = useState(false);
    const deleteShardMutation = useDeleteShard(
        { clusterID: params.clusterID, keyspaceShard: shardName, recursive, evenIfServing },
        {
            onSuccess: (result) => {
                success(`Successfully deleted shard ${shardName}`, { autoClose: 7000 });
                history.push(`/keyspace/${params.keyspace}/${params.clusterID}/shards`);
            },
            onError: (error) => warn(`There was an error deleting shard ${shardName}: ${error}`),
        }
    );

    // reloadSchemaShard parameters
    const [waitPosition, setWaitPosition] = useState<string | undefined>(undefined);
    const [includePrimary, setIncludePrimary] = useState(false);
    const [concurrency, setConcurrency] = useState<number | undefined>(undefined);
    const reloadSchemaShardMutation = useReloadSchemaShard(
        {
            clusterID: params.clusterID,
            keyspace: params.keyspace,
            shard: params.shard,
            includePrimary,
            waitPosition,
            concurrency,
        },
        {
            onSuccess: (result) => {
                success(`Successfully reloaded shard ${shardName}`, { autoClose: 7000 });
            },
            onError: (error) => warn(`There was an error reloading shard ${shardName}: ${error}`),
        }
    );

    // externallyReparent parameters
    const [tablet, setTablet] = useState<vtadmin.Tablet | null>(null);
    const externallyPromoteMutation = useTabletExternallyPromoted(
        { alias: formatAlias(tablet?.tablet?.alias) as string, clusterIDs: [params.clusterID] },
        {
            onSuccess: (result) => {
                success(`Successfully promoted tablet ${formatAlias(tablet?.tablet?.alias)}`, { autoClose: 7000 });
            },
            onError: (error) =>
                warn(`There was an error promoting tablet ${formatAlias(tablet?.tablet?.alias)}: ${error}`),
        }
    );

    // plannedReparent parameters
    const [plannedReparentTablet, setPlannedReparentTablet] = useState<vtadmin.Tablet | null>(null);
    const plannedReparentMutation = usePlannedFailoverShard(
        {
            clusterID: params.clusterID,
            keyspace: params.keyspace,
            shard: params.shard,
            new_primary: plannedReparentTablet as vtadmin.Tablet,
        },
        {
            onSuccess: (result) => {
                setDialogTitle(`Planned Failover`);
                setDialogDescription(
                    `Successfully failed over shard ${params.shard} to tablet ${formatAlias(
                        plannedReparentTablet?.tablet?.alias
                    )}.`
                );
                setFailoverDialogIsOpen(true);
                setEvents(result.events);
            },
            onError: (error) =>
                warn(`There was an error failing over shard ${params.shard} to ${plannedReparentTablet}: ${error}`),
        }
    );

    // emergencyReparent parameters
    const [emergencyReparentTablet, setEmergencyReparentTablet] = useState<vtadmin.Tablet | null>(null);
    const emergencyReparentMutation = useEmergencyFailoverShard(
        {
            clusterID: params.clusterID,
            keyspace: params.keyspace,
            shard: params.shard,
            new_primary: emergencyReparentTablet as vtadmin.Tablet,
        },
        {
            onSuccess: (result) => {
                setDialogTitle(`Emergency Failover`);
                setDialogDescription(
                    `Successfully failed over ${params.shard} to tablet ${formatAlias(
                        emergencyReparentTablet?.tablet?.alias
                    )}.`
                );
                setFailoverDialogIsOpen(true);
                setEvents(result.events);
            },
            onError: (error) =>
                warn(
                    `There was an error failing over shard ${params.shard} to ${formatAlias(
                        emergencyReparentTablet?.tablet?.alias
                    )}: ${error}`
                ),
        }
    );

    // validation dialog parameters
    const [validateDialogTitle, setValidateDialogTitle] = useState('');
    const [validateDialogDescription, setValidateDialogDescription] = useState('');
    const onCloseValidateDialog = () => {
        setValidateDialogIsOpen(false);
        setValidateDialogTitle('');
        setValidateDialogDescription('');
        setValidateShardResponse(null);
        setValidateVersionShardResponse(null);
    };

    // validateShard parameters
    const [pingTablets, setPingTablets] = useState(false);
    const [validateDialogIsOpen, setValidateDialogIsOpen] = useState(false);
    const [validateShardResponse, setValidateShardResponse] = useState<vtctldata.ValidateShardResponse | null>(null);

    const validateShardMutation = useValidateShard(
        {
            clusterID: params.clusterID,
            keyspace: params.keyspace,
            shard: params.shard,
            pingTablets,
        },
        {
            onSuccess: (result) => {
                setValidateDialogTitle('Validate Shard');
                setValidateDialogDescription(`Successfully validated ${params.shard}.`);
                setValidateDialogIsOpen(true);
                setValidateShardResponse(result);
            },
            onError: (error) => warn(`There was an error validating shard ${params.shard}: ${error}`),
        }
    );

    const [
        validateVersionShardResponse,
        setValidateVersionShardResponse,
    ] = useState<vtctldata.ValidateVersionShardResponse | null>(null);

    const validateVersionShardMutation = useValidateVersionShard(
        {
            clusterID: params.clusterID,
            keyspace: params.keyspace,
            shard: params.shard,
        },
        {
            onSuccess: (result) => {
                setValidateDialogTitle('Validate Version Shard');
                setValidateDialogDescription(`Successfully validated versions on ${params.shard}.`);
                setValidateDialogIsOpen(true);
                setValidateShardResponse(result);
            },
            onError: (error) => warn(`There was an error validating versions on shard ${params.shard}: ${error}`),
        }
    );

    if (kq.error) {
        return (
            <div className="items-center flex flex-column text-lg justify-center m-3 text-center max-w-[720px]">
                <span className="text-xl">😰</span>
                <h1>An error occurred</h1>
                <code>{(kq.error as any).response?.error?.message || kq.error?.message}</code>
                <p>
                    <Link to="/keyspaces">← All keyspaces</Link>
                </p>
            </div>
        );
    }

    return (
        <>
            <Dialog
                className="min-w-[500px]"
                isOpen={failoverDialogIsOpen}
                onClose={() => setFailoverDialogIsOpen(false)}
                onConfirm={() => setFailoverDialogIsOpen(false)}
                title={dialogTitle}
                hideCancel
                confirmText="Dismiss"
            >
                <>
                    <div className="mt-8 mb-4">{dialogDescription}</div>
                    <div className="mb-2 font-bold">Log</div>
                    <div className="bg-gray-100 p-4 overflow-scroll max-h-[200px]">
                        {events.map((e, i) => (
                            <EventLogEntry event={e} key={`${i}_event_log`} />
                        ))}
                    </div>
                </>
            </Dialog>
            <Dialog
                className="min-w-[500px]"
                isOpen={validateDialogIsOpen}
                onClose={onCloseValidateDialog}
                onConfirm={onCloseValidateDialog}
                title={validateDialogTitle}
                hideCancel
                confirmText="Dismiss"
            >
                <>
                    <div className="mt-8 mb-4">{validateDialogDescription}</div>
                    {validateShardResponse && (
                        <ValidationResults
                            resultsByShard={validateShardResponse || validateVersionShardResponse}
                            shard={shardName}
                        />
                    )}
                </>
            </Dialog>
            <div className="pt-4">
                <div className="my-8">
                    <h3 className="mb-4">Status</h3>
                    <div>
                        <ActionPanel
                            description={
                                <>
                                    Validates that all nodes reachable from the specified shard{' '}
                                    <span className="font-bold">{shardName}</span> are consistent.
                                </>
                            }
                            documentationLink="https://vitess.io/docs/reference/programs/vtctldclient/vtctldclient_validateshard/#vtctldclient-validateshard"
                            loadingText="Validating shard..."
                            loadedText="Validate"
                            mutation={validateShardMutation as UseMutationResult}
                            title="Validate Shard"
                            body={
                                <>
                                    <div className="mt-2">
                                        <div className="flex items-center">
                                            <Toggle
                                                className="mr-2"
                                                enabled={pingTablets}
                                                onChange={() => setPingTablets(!pingTablets)}
                                            />
                                            <Label label="Ping Tablets" />
                                        </div>
                                        When set, all tablets are pinged during the validation process.
                                    </div>
                                </>
                            }
                        />
                        <ActionPanel
                            description={
                                <>
                                    Validates that the version on the primary matches all of the replicas on shard{' '}
                                    <span className="font-bold">{shardName}</span>.
                                </>
                            }
                            documentationLink="https://vitess.io/docs/reference/programs/vtctl/schema-version-permissions/#validateversionshard"
                            loadingText="Validating shard versions..."
                            loadedText="Validate"
                            mutation={validateVersionShardMutation as UseMutationResult}
                            title="Validate Version Shard"
                        />
                    </div>
                </div>
            </div>
            <div className="pt-4">
                <div className="my-8">
                    <h3 className="mb-4">Reload</h3>
                    <div>
                        <ActionPanel
                            description={
                                <>
                                    Reloads the schema on all tablets in shard{' '}
                                    <span className="font-bold">{shardName}</span>. This is done on a best-effort basis.
                                </>
                            }
                            documentationLink="https://vitess.io/docs/reference/programs/vtctldclient/vtctldclient_reloadschemashard/"
                            loadingText="Reloading schema shard..."
                            loadedText="Reload"
                            mutation={reloadSchemaShardMutation as UseMutationResult}
                            title="Reload Schema Shard"
                            body={
                                <>
                                    <p className="text-base">
                                        <strong>Wait Position</strong> <br />
                                        Allows scheduling a schema reload to occur after a given DDL has replicated to
                                        this server, by specifying a replication position to wait for. Leave empty to
                                        trigger the reload immediately.
                                    </p>
                                    <div className="w-1/3">
                                        <TextInput
                                            value={waitPosition}
                                            onChange={(e) => setWaitPosition(e.target.value)}
                                        />
                                    </div>
                                    <div className="mt-2">
                                        <Label label="Concurrency" /> <br />
                                        Number of tablets to reload in parallel. Set to zero for unbounded concurrency.
                                        (Default 10)
                                        <div className="w-1/3 mt-4">
                                            <NumberInput
                                                value={concurrency}
                                                onChange={(e) => setConcurrency(parseInt(e.target.value))}
                                            />
                                        </div>
                                    </div>
                                    <div className="mt-2">
                                        <div className="flex items-center">
                                            <Toggle
                                                className="mr-2"
                                                enabled={includePrimary}
                                                onChange={() => setIncludePrimary(!includePrimary)}
                                            />
                                            <Label label="Include Primary" />
                                        </div>
                                        When set, also reloads the primary tablet.
                                    </div>
                                </>
                            }
                        />
                    </div>
                </div>
            </div>
            <div className="pt-4">
                <div className="my-8">
                    <h3 className="mb-4">Change</h3>
                    <div>
                        <ActionPanel
                            description={
                                <>
                                    Changes metadata in the topology service to acknowledge a shard primary change
                                    performed by an external tool.
                                </>
                            }
                            disabled={!tablet}
                            documentationLink="https://vitess.io/docs/reference/programs/vtctl/shards/#tabletexternallyreparented"
                            loadingText="Failing over..."
                            loadedText="Failover"
                            mutation={externallyPromoteMutation as UseMutationResult}
                            title="External Failover"
                            body={
                                <>
                                    <div className="mt-2">
                                        <div className="flex items-center">
                                            <Select
                                                onChange={(t) => setTablet(t as vtadmin.Tablet)}
                                                label="Tablet"
                                                renderItem={(t: vtadmin.Tablet) =>
                                                    `${formatAlias(t.tablet?.alias)} (${formatDisplayType(t)})`
                                                }
                                                items={tabletsInCluster}
                                                selectedItem={tablet}
                                                placeholder="Tablet"
                                                description="This chosen tablet will be considered the shard primary (but Vitess won't change the replication setup)."
                                                required
                                            />
                                        </div>
                                    </div>
                                </>
                            }
                        />
                        <ActionPanel
                            confirmationValue={shardName}
                            description={
                                <>
                                    Delete shard <span className="font-bold">{shardName}</span>. In recursive mode, it
                                    also deletes all tablets belonging to the shard. Otherwise, there must be no tablets
                                    left in the shard.
                                </>
                            }
                            documentationLink="https://vitess.io/docs/reference/programs/vtctl/shards/#deleteshard"
                            loadingText="Deleting..."
                            loadedText="Delete"
                            mutation={deleteShardMutation as UseMutationResult}
                            title="Delete Shard"
                            body={
                                <>
                                    <div className="mt-2">
                                        <div className="flex items-center">
                                            <Toggle
                                                className="mr-2"
                                                enabled={evenIfServing}
                                                onChange={() => setEvenIfServing(!evenIfServing)}
                                            />
                                            <Label label="Even If Serving" />
                                        </div>
                                        When set, removes the shard even if it is serving. Use with caution.
                                    </div>
                                    <div className="mt-2">
                                        <div className="flex items-center">
                                            <Toggle
                                                className="mr-2"
                                                enabled={recursive}
                                                onChange={() => setRecursive(!recursive)}
                                            />
                                            <Label label="Recursive" />
                                        </div>
                                        When set, also deletes all tablets belonging to the shard.
                                    </div>
                                </>
                            }
                        />
                    </div>
                </div>
            </div>
            <div className="pt-4">
                <div className="my-8">
                    <h3 className="mb-4">Reparent</h3>
                    <div>
                        <ActionPanel
                            description={
                                <>
                                    Reparents the shard to a new primary that can either be explicitly specified, or
                                    chosen by Vitess.
                                </>
                            }
                            disabled={!plannedReparentTablet}
                            documentationLink="https://vitess.io/docs/reference/programs/vtctl/shards/#plannedreparentshard"
                            loadingText="Failing over..."
                            loadedText="Failover"
                            mutation={plannedReparentMutation as UseMutationResult}
                            title="Planned Failover"
                            body={
                                <>
                                    <div className="mt-2">
                                        <div className="flex items-center">
                                            <Select
                                                required
                                                onChange={(t) => setPlannedReparentTablet(t as vtadmin.Tablet)}
                                                label="Tablet"
                                                items={tabletsInCluster}
                                                renderItem={(t: vtadmin.Tablet) =>
                                                    `${formatAlias(t.tablet?.alias)} (${formatDisplayType(t)})`
                                                }
                                                selectedItem={plannedReparentTablet}
                                                placeholder="Tablet"
                                                description="This tablet will be the new primary for this shard."
                                            />
                                        </div>
                                    </div>
                                </>
                            }
                        />
                        <ActionPanel
                            description={
                                <>
                                    Reparents the shard to the new primary. Assumes the old primary is dead and not
                                    responding.
                                </>
                            }
                            disabled={!emergencyReparentTablet}
                            documentationLink="https://vitess.io/docs/reference/programs/vtctl/shards/#emergencyreparentshard"
                            loadingText="Failing over..."
                            loadedText="Failover"
                            mutation={emergencyReparentMutation as UseMutationResult}
                            title="Emergency Failover"
                            body={
                                <>
                                    <div className="mt-2">
                                        <div className="flex items-center">
                                            <Select
                                                required
                                                onChange={(t) => setEmergencyReparentTablet(t as vtadmin.Tablet)}
                                                label="Tablet"
                                                items={tabletsInCluster}
                                                renderItem={(t: vtadmin.Tablet) =>
                                                    `${formatAlias(t.tablet?.alias)} (${formatDisplayType(t)})`
                                                }
                                                selectedItem={emergencyReparentTablet}
                                                placeholder="Tablet"
                                                description="This tablet will be the new primary for this shard."
                                            />
                                        </div>
                                    </div>
                                </>
                            }
                        />
                    </div>
                </div>
            </div>
        </>
    );
};

export default Advanced;
