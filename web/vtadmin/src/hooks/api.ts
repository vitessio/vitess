import { useQuery } from 'react-query';
import { fetchTablets } from '../api/http';
import { vtadmin as pb } from '../proto/vtadmin';

export const useTablets = () => {
    return useQuery<pb.Tablet[], Error>(['tablets'], async () => {
        return await fetchTablets();
    });
};
