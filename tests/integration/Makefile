start:
	docker-compose -f docker-compose.2clients.yaml up -d

stop:
	docker-compose -f docker-compose.2clients.yaml down

clear:
	docker-compose -f docker-compose.2clients.yaml down
	docker volume prune

reload:
	docker-compose -f docker-compose.2clients.yaml down
	docker volume prune
	docker-compose -f docker-compose.2clients.yaml up -d