import { filterNouns } from './filterNouns';

describe('filterNouns', () => {
    const tests: {
        name: string;
        needle: string;
        haystack: any[];
        expected: any[];
    }[] = [
        {
            name: 'it returns the haystack for an empty needle',
            needle: '',
            haystack: [{ goodnight: 'moon' }, { goodnight: 'stars' }],
            expected: [{ goodnight: 'moon' }, { goodnight: 'stars' }],
        },
        {
            name: 'it combines phrases and non-phrases',
            needle: '"astronomy" moon',
            haystack: [
                { keyspace: 'astronomy', name: 'moon' },
                { keyspace: 'astronomy', name: 'moonbeam' },
                { keyspace: 'gastronomy', name: 'mooncake' },
            ],
            expected: [
                { keyspace: 'astronomy', name: 'moon' },
                { keyspace: 'astronomy', name: 'moonbeam' },
            ],
        },
        {
            name: 'it matches key/values with the same key additively',
            needle: 'hello:world hello:sun',
            haystack: [{ hello: 'world' }, { hello: 'moon' }, { hello: 'sun' }],
            expected: [{ hello: 'world' }, { hello: 'sun' }],
        },
        {
            name: 'it matches key/values with different keys subtractively',
            needle: 'hello:sun goodbye:moon',
            haystack: [
                { hello: 'sun', goodbye: 'stars' },
                { hello: 'sun', goodbye: 'moon' },
                { hello: 'stars', goodbye: 'moon' },
            ],
            expected: [{ hello: 'sun', goodbye: 'moon' }],
        },
        {
            name: 'it omits nouns with empty values for a specified key',
            needle: 'goodbye:moon',
            haystack: [{ hello: 'sun', goodbye: 'moon' }, { hello: 'stars' }],
            expected: [{ hello: 'sun', goodbye: 'moon' }],
        },
        {
            name: 'it matches queries with multiple key/value tokens and also a fuzzy string for good measure',
            needle: 'cluster:cluster-1 keyspace:customers keyspace:commerce re',
            haystack: [
                { cluster: 'cluster-2', keyspace: 'customers', type: 'replica' },
                { cluster: 'cluster-1', keyspace: 'customers', type: 'readonly' },
                { cluster: 'cluster-1', keyspace: 'customers', type: 'primary' },
                { cluster: 'cluster-1', keyspace: 'loadtest', type: 'replica' },
                { cluster: 'cluster-1', keyspace: 'commerce', type: 'replica' },
                { cluster: 'cluster-1', keyspace: 'commerce', type: 'primary' },
            ],
            expected: [
                { cluster: 'cluster-1', keyspace: 'customers', type: 'readonly' },
                { cluster: 'cluster-1', keyspace: 'commerce', type: 'replica' },
            ],
        },
    ];

    test.each(tests.map(Object.values))('%s', (name: string, needle: string, haystack: any[], expected: any[]) => {
        const result = filterNouns(needle, haystack);
        expect(result).toEqual(expected);
    });
});
