# Apollo Customer GraphQL API

Minimal Customer API using Apollo Server (standalone).

## Run

```bash
npm install
npm start
```

Server starts at `http://localhost:4000/` (GraphQL over POST).

## Example operations

**Query all**
```graphql
query {
  customers { id first_name last_name email phone }
}
```

**Query one**
```graphql
query One($id: ID!) {
  customer(id: $id) { id first_name last_name email }
}
```

**Create**
```graphql
mutation Create($input: CustomerIn!) {
  createCustomer(input: $input) { id first_name email }
}
```

**Update**
```graphql
mutation Update($id: ID!, $input: CustomerIn!) {
  updateCustomer(id: $id, input: $input) { id first_name last_name }
}
```

**Delete**
```graphql
mutation Del($id: ID!) { deleteCustomer(id: $id) }
```