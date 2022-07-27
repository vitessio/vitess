import '@testing-library/jest-dom';
import { setLogger } from 'react-query';

// Suppress network (and "network", i.e., localhost) errors
// from being logged to the console during testing.
// See https://react-query.tanstack.com/guides/testing
setLogger({
    log: console.log,
    warn: console.warn,
    error: () => {},
});
