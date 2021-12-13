import React from 'react'
import { useParams } from 'react-router-dom';

interface SettingsProps {

}

interface RouteParams {
    alias: string;
    clusterID: string;
}

const Settings: React.FC<SettingsProps> = () => {
    const { clusterID, alias } = useParams<RouteParams>();

    return (
        <div>
            <p className="text-base font-bold pt-1">Ignore Health Check</p>
        </div>
    )
}

export default Settings