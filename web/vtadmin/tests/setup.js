import { expect, afterEach, vi } from 'vitest';
import { cleanup } from '@testing-library/react';
import matchers from '@testing-library/jest-dom/matchers';
import { fetch } from 'cross-fetch';
import { server } from './server'

global.fetch = fetch;
const ORIGINAL_PROCESS_ENV = import.meta.env;
const TEST_PROCESS_ENV = {
    ...import.meta.env,
    VITE_VTADMIN_API_ADDRESS: 'http://test-api.com',
};
global.server = server
beforeAll(() => {
  import.meta.env = { ...TEST_PROCESS_ENV };
  server.listen({ onUnhandledRequest: `error` })
});
afterAll(() => {
  import.meta.env = { ...ORIGINAL_PROCESS_ENV }
  cleanup()
  server.close()
})
afterEach(() => {
  import.meta.env = { ...TEST_PROCESS_ENV }
  server.resetHandlers()
  
});

// extends Vitest's expect method with methods from react-testing-library
expect.extend(matchers);
