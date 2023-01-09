build:
	docker-compose build

start:
	docker-compose up -d

stop:
	docker-compose down

reload:
	docker-compose build
	docker-compose up -d

logs-server:
	docker-compose logs -f server worker

logs-client:
	docker-compose logs -f 

clean-server:
	docker-compose down
	docker volume rm federated-learning-server_ferdelance-db-data
	docker volume rm federated-learning-server_ferdelance-server-data

clean-client:
	docker-compose down
	docker volume rm federated-learning-client_ferdelance-client-data
	rm -rf ./workdir/*

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
	python tests/test_distributed.py
