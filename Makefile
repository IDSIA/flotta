build:
	docker-compose build

start:
	docker-compose up -d

stop:
	docker-compose down

reload:
	docker-compose build
	docker-compose up -d

#
#
#

build-dev:
	docker-compose -f docker-compose-dev.yaml build

start-dev:
	docker-compose -f docker-compose-dev.yaml up -d

stop-dev:
	docker-compose -f docker-compose-dev.yaml down

reload-dev:
	docker-compose -f docker-compose-dev.yaml build
	docker-compose -f docker-compose-dev.yaml up -d

#
#
#

logs:
	docker-compose logs -f server worker

clear:
	docker-compose down
	docker volume rm federated-learning-server_ferdelance-db-data
	docker volume rm federated-learning-server_ferdelance-server-data

create:
	python -m venv Ferdelance_env

recreate:
	rm -rf Ferdelance_env
	python -m venv Ferdelance_env

dev:
	pip install federated-learning-shared/
	pip install -e ".[test]"
