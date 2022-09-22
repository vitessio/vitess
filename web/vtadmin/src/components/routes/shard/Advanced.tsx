import React, { useState } from 'react'
import { useParams, useRouteMatch, Link } from 'react-router-dom';
import { useDeleteShard, useKeyspace } from '../../../hooks/api';
import { useDocumentTitle } from '../../../hooks/useDocumentTitle';
import ActionPanel from '../../ActionPanel'
import { success, warn } from '../../Snackbar';
import { UseMutationResult } from 'react-query';
import Toggle from '../../toggle/Toggle';
import { Label } from '../../inputs/Label';

interface RouteParams {
  clusterID: string;
  keyspace: string;
  shard: string;
}

const Advanced: React.FC = () => {
  const params = useParams<RouteParams>();

  const shardName = `${params.keyspace}/${params.shard}`;

  useDocumentTitle(`${shardName} (${params.clusterID})`);

  const { data: keyspace, ...kq } = useKeyspace({ clusterID: params.clusterID, name: params.keyspace });

  const [evenIfServing, setEvenIfServing] = useState(false)
  const [recursive, setRecursive] = useState(false)
  const deleteShardMutation = useDeleteShard(
    { clusterID: params.clusterID, keyspaceShard: shardName, recursive, evenIfServing },
    {
      onSuccess: (result) => {
        success(
          `Successfully deleted shard ${shardName}`,
          { autoClose: 7000 }
        );
      },
      onError: (error) => warn(`There was an error deleting shard ${shardName}: ${error}`),
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
      <div className="pt-4">
        <div className="my-8">
          <h3 className="mb-4">Status</h3>
        </div>
      </div>
      <div className="pt-4">
        <div className="my-8">
          <h3 className="mb-4">Change</h3>
          <div>
            <ActionPanel
              confirmationValue={shardName}
              description={
                <>
                  Delete shard <span className="font-bold">{shardName}</span>. In recursive mode, it also deletes all tablets belonging to the shard. Otherwise, there must be no tablets left in the shard.
                </>
              }
              documentationLink="https://vitess.io/docs/14.0/reference/programs/vtctl/shards/#deleteshard"
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
        </div>
      </div>
    </>

  )
}

export default Advanced