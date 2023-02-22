# launch client
client:
	python -m ferdelance.client -c conf/config.yaml

standalone:
	python -m ferdelance.standalone -c conf/config.yaml

clean:
	rm -rf workdir/ storage/ sqlite.db tests/test_sqlite.db ferdelance*.log*

# docker build 
build-client:
	docker-compose -f docker-compose.client.yaml -p ferdelance build

build-server:
	docker-compose -f docker-compose.server.yaml -p ferdelance build

# docker management client
start-client:
	docker-compose -f docker-compose.client.yaml -p ferdelance up -d

stop-client:
	docker-compose -f docker-compose.client.yaml -p ferdelance down

reload-client:
	docker-compose -f docker-compose.client.yaml -p ferdelance build
	docker-compose -f docker-compose.client.yaml -p ferdelance up -d

logs-client:
	docker-compose -f docker-compose.client.yaml -p ferdelance logs -f 

clean-client:
	docker-compose -f docker-compose.client.yaml -p ferdelance down
	docker volume rm ferdelance_ferdelance-client-data
	rm -rf ./workdir/*

# docker management server
start-server:
	docker-compose -f docker-compose.server.yaml -p ferdelance up -d

stop-server:
	docker-compose -f docker-compose.server.yaml -p ferdelance down

reload-server:
	docker-compose -f docker-compose.server.yaml -p ferdelance build
	docker-compose -f docker-compose.server.yaml -p ferdelance up -d

logs-server:
	docker-compose -f docker-compose.server.yaml -p ferdelance logs -f server worker

clean-server:
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
