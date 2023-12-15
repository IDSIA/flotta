start:
	docker compose -f docker-compose.integration.yaml up -d

stop:
	docker compose -f docker-compose.integration.yaml down

clear:
	docker compose -f docker-compose.integration.yaml down
	docker volume prune
	docker volume rm integration_client-1-data integration_client-2-data integration_db-data integration_server-data

reload:
	docker compose -f docker-compose.integration.yaml down
	docker volume prune
	docker compose -f docker-compose.integration.yaml up -d

logs:
	docker compose -f docker-compose.integration.yaml logs -f

logs-server:
	docker compose -f docker-compose.integration.yaml logs -f server

logs-client1:
	docker compose -f docker-compose.integration.yaml logs -f client1
logs-client2:
	docker compose -f docker-compose.integration.yaml logs -f client2
logs-clients:
	docker compose -f docker-compose.integration.yaml logs -f client1 client2

logs-node1:
	docker compose -f docker-compose.integration.yaml logs -f node1
logs-node2:
	docker compose -f docker-compose.integration.yaml logs -f node2
logs-nodes:
	docker compose -f docker-compose.integration.yaml logs -f node1 node2

logs-workbench:
	docker compose -f docker-compose.integration.yaml logs -f workbench