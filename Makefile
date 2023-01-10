# docker build 
build-client:
	docker-compose -f docker/docker-compose.client.yaml build

build-server:
	docker-compose -f docker/docker-compose.server.yaml build

# docker management client
start-client:
	docker-compose -f docker/docker-compose.client.yaml up -d

stop-client:
	docker-compose -f docker/docker-compose.client.yaml down

reload-client:
	docker-compose -f docker/docker-compose.client.yaml build
	docker-compose -f docker/docker-compose.client.yaml up -d

logs-client:
	docker-compose -f docker/docker-compose.client.yaml logs -f 

clean-client:
	docker-compose -f docker/docker-compose.client.yaml down
	docker volume rm federated-learning-client_ferdelance-client-data
	rm -rf ./workdir/*

# docker management server
logs-server:
	docker-compose -f docker/docker-compose.client.yaml logs -f server worker

clean-server:
	docker-compose down
	docker volume rm federated-learning-server_ferdelance-db-data
	docker volume rm federated-learning-server_ferdelance-server-data

# development
create:
	python -m venv Ferdelance_env

delete:
	rm -rf Ferdelance_env

recreate:
	rm -rf Ferdelance_env
	python -m venv Ferdelance_env

dev:
	pip install -e ".[dev]"

test:
	pytest
