import React, { useState } from 'react'
import { DataCell } from '../../dataTable/DataCell'
import { logutil, vtadmin as pb } from '../../../proto/vtadmin';
import Dialog from '../../dialog/Dialog';
import { Icon, Icons } from '../../Icon';
import { useValidate } from '../../../hooks/api';
import { Label } from '../../inputs/Label';
import Toggle from '../../toggle/Toggle';

interface Props {
  cluster: pb.Cluster
}

const ClusterRow: React.FC<Props> = ({ cluster }) => {
  const [isOpen, setIsOpen] = useState(false)
  const [pingTablets, setPingTablets] = useState(false)

  const closeDialog = () => {
    setIsOpen(false)
  }

  const { mutate, error, data, isIdle } = useValidate({ clusterID: cluster.id, pingTablets })

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
      >
        <div className="w-full">
          {isIdle && (
            <div>
              <div className='my-4'>Validate that all nodes in the cluster are reachable from the global replication graph, as well as all tablets in discoverable cells, are consistent.</div>
              <div className="flex items-center">
                <Toggle enabled={pingTablets} onChange={() => setPingTablets(!pingTablets)} /><Label className="ml-2" label="Ping Tablets" />
              </div>
              When set, all tablets will be pinged during the validation process.
            </div>
          )}
          {!isIdle && !error && (
            <div className="w-full flex flex-col justify-center items-center">
              <span className="flex h-12 w-12 relative items-center justify-center">
                <Icon className="fill-current text-green-500" icon={Icons.checkSuccess} />
              </span>
              <div className="text-lg mt-3 font-bold text-center">Successfully validated cluster {cluster.name}</div>
            </div>
          )}
          {!isIdle && error && (
            <div className="w-full flex flex-col justify-center items-center">
              <span className="flex h-12 w-12 relative items-center justify-center">
                <Icon className="fill-current text-red-500" icon={Icons.alertFail} />
              </span>
              <div className="text-lg mt-3 font-bold text-center">There was an issue validating nodes in cluster {cluster.name}</div>
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
            setIsOpen(true)
          }
          }>
          Validate
        </button>
      </DataCell>
    </tr>
  )
}

export default ClusterRow