# folder cleanup
clean:
	rm -rf workdir/ storage/ sqlite.db tests/test_sqlite.db ferdelance*.log*

# launch client
client:
	python -m ferdelance.client -c conf/config.yaml

# launch standalone mode
standalone:
	python -m ferdelance.standalone -c conf/config.yaml

# docker build 
build:
	docker-compose -f docker-compose.build.yaml build

build-client:
	docker-compose -f docker-compose.build.yaml client build

build-server:
	docker-compose -f docker-compose.build.yaml server build

build-repo:
	docker-compose -f docker-compose.build.yaml repository build

# docker management client
client-start:
	docker-compose -f docker-compose.client.yaml -p ferdelance up -d

client-stop:
	docker-compose -f docker-compose.client.yaml -p ferdelance down

client-reload:
	docker-compose -f docker-compose.client.yaml -p ferdelance build
	docker-compose -f docker-compose.client.yaml -p ferdelance up -d

client-logs:
	docker-compose -f docker-compose.client.yaml -p ferdelance logs -f 

client-clean:
	docker-compose -f docker-compose.client.yaml -p ferdelance down
	docker volume rm ferdelance_ferdelance-client-data
	rm -rf ./workdir/*

# docker management server
server-start:
	docker-compose -f docker-compose.server.yaml -p ferdelance up -d

server-stop:
	docker-compose -f docker-compose.server.yaml -p ferdelance down

server-reload:
	docker-compose -f docker-compose.server.yaml -p ferdelance build
	docker-compose -f docker-compose.server.yaml -p ferdelance up -d

server-logs:
	docker-compose -f docker-compose.server.yaml -p ferdelance logs -f server worker

server-clean:
	docker-compose down
	docker volume rm ferdelance_ferdelance-db-data
	docker volume rm ferdelance_ferdelance-server-data

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
