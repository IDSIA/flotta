start:
	docker compose -f docker-compose.2clients.yaml up -d

stop:
	docker compose -f docker-compose.2clients.yaml down

clear:
	docker compose -f docker-compose.2clients.yaml down
	docker volume prune
	docker volume rm integration_client-1-data integration_client-2-data integration_db-data integration_server-data

reload:
	docker compose -f docker-compose.2clients.yaml down
	docker volume prune
	docker compose -f docker-compose.2clients.yaml up -d

logs:
	docker compose -f docker-compose.2clients.yaml logs -f

logs-server:
	docker compose -f docker-compose.2clients.yaml logs -f server
logs-client1:
	docker compose -f docker-compose.2clients.yaml logs -f client1
logs-client2:
	docker compose -f docker-compose.2clients.yaml logs -f client2
logs-clients:
	docker compose -f docker-compose.2clients.yaml logs -f client1 client2
logs-workbench:
	docker compose -f docker-compose.2clients.yaml logs -f workbench