SHELL := /bin/bash

STACK ?= zoo
COMPOSE_FILE := docker/docker-compose.yml
ifeq ($(STACK),kraft)
COMPOSE_FILE := docker/docker-compose-kraft.yml
endif

.PHONY: up up-kraft down logs ksql connect demo clean

up:
	docker compose -f $(COMPOSE_FILE) up -d

up-kraft:
	$(MAKE) up STACK=kraft

down:
	docker compose -f $(COMPOSE_FILE) down -v

logs:
	docker compose -f $(COMPOSE_FILE) logs -f --tail=200

ksql:
	./demos/e2e/ksql/post_ksql.sh

connect:
	./connect/scripts/post_connect_configs.sh

demo:
	python demos/e2e/producer_orders.py

clean:
	rm -rf docker/data/orders_high.out || true
