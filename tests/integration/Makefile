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