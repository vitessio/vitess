import React from 'react';
import { logutil } from '../../../proto/vtadmin';

interface Props {
    event: logutil.IEvent;
}

const EventLogEntry: React.FC<Props> = ({ event }) => (
    <div className="font-mono text-sm whitespace-nowrap">
        [{new Date((event.time?.seconds as number) * 1000).toISOString()} {event.file}:{event.line}] {event.value}
    </div>
);

export default EventLogEntry;
