import { useQuery } from 'react-query';
import { fetchTablets } from '../api/http';

export const useTablets = () => {
    return useQuery(['tablets'], async () => {
        return await fetchTablets();
    });
};
