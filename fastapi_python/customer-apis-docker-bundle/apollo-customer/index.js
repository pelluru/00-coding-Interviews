import { ApolloServer } from '@apollo/server';
import { startStandaloneServer } from '@apollo/server/standalone';
import DataLoader from 'dataloader';

// In-memory DB
const DB = {
  '1': { id: '1', first_name: 'Ada', last_name: 'Lovelace', email: 'ada@example.com', phone: null },
};

const typeDefs = `#graphql
  type Customer { id: ID!, first_name: String!, last_name: String!, email: String!, phone: String }
  input CustomerIn { first_name: String!, last_name: String!, email: String!, phone: String }
  type Query { customers: [Customer!]!, customer(id: ID!): Customer }
  type Mutation { createCustomer(input: CustomerIn!): Customer!, updateCustomer(id: ID!, input: CustomerIn!): Customer, deleteCustomer(id: ID!): Boolean! }
`;

// Batch loader for customer by id
const batchGet = async (ids) => ids.map(id => DB[id] || null);

const resolvers = {
  Query: {
    customers: () => Object.values(DB),
    customer: (_, { id }) => DB[id] || null,
  },
  Mutation: {
    createCustomer: (_, { input }) => {
      const id = String(Object.keys(DB).length + 1);
      const cust = { id, ...input };
      DB[id] = cust;
      return cust;
    },
    updateCustomer: (_, { id, input }) => {
      if (!DB[id]) return null;
      DB[id] = { id, ...input };
      return DB[id];
    },
    deleteCustomer: (_, { id }) => {
      if (!DB[id]) return false;
      delete DB[id];
      return true;
    },
  },
};

const server = new ApolloServer({ typeDefs, resolvers });

const { url } = await startStandaloneServer(server, {
  listen: { port: 4000 },
  context: async () => ({
    loaders: { customerById: new DataLoader(batchGet) },
  }),
});

console.log(`\n🚀 Apollo Customer GraphQL ready at ${url}`);
console.log(`Try: curl -X POST -H 'Content-Type: application/json' --data '{"query":"{ customers { id first_name } }"}' ${url}`);