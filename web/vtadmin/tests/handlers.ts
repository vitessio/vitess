import { graphql, rest } from 'msw'

export const handlers = [
  rest.get('/api/keyspace/some-cluster/some-keyspace', (req, res, ctx) => {
    return res(ctx.status(200), ctx.json({}))
  }),
]