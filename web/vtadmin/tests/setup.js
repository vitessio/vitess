import { expect, afterEach, vi } from 'vitest';
import { cleanup } from '@testing-library/react';
import matchers from '@testing-library/jest-dom/matchers';
import { fetch } from 'cross-fetch';
import { server } from './server'

global.fetch = fetch;

beforeAll(() => server.listen({ onUnhandledRequest: `error` }));
afterAll(() => server.close());
afterEach(() => {
  server.resetHandlers()
  
});

// extends Vitest's expect method with methods from react-testing-library
expect.extend(matchers);
