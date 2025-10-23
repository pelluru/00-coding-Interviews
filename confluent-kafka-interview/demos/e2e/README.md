# End-to-End Demo (Local)

Flow: **Producer → Kafka Topic `orders` → ksqlDB filter → Topic `orders_high` → Connect FileSink → `/data/orders_high.out`**

## Steps
1) Start stack (ZooKeeper-based or KRaft):
```bash
cd docker
docker compose up -d            # or: docker compose -f docker-compose-kraft.yml up -d
```
2) Seed demo folder:
```bash
mkdir -p docker/data
: > docker/data/orders_seed.in
```
3) Create ksqlDB streams (filter high amount):
```bash
./demos/e2e/ksql/post_ksql.sh
```
4) Produce test events:
```bash
python demos/e2e/producer_orders.py
```
   - Alternatively seed via FileStreamSource by writing to `docker/data/orders_seed.in`

5) Deploy Connect sink/source:
```bash
./connect/scripts/post_connect_configs.sh
```
6) Check results:
```bash
docker exec -it $(docker ps --filter name=connect -q) bash -lc "tail -n +1 /data/orders_high.out"
```

> Ports: broker `localhost:29092`, ksqlDB `8088`, Connect `8083`.
