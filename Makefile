build:
	docker-compose build

start:
	docker-compose up -d

stop:
	docker-compose down

reload:
	docker-compose build
	docker-compose up -d

logs:
	docker-compose logs -f server

nuke:
	docker-compose down
	docker volume rm federated-learning-server_spearhead-db-data
	docker volume rm federated-learning-server_spearhead-server-data

venv-create:
	python -m venv Spearhead_env

venv-recreate:
	rm -rf Spearhead_env
	python -m venv Spearhead_env

venv-dev-install:
	pip install federated-learning-shared/
	pip install -e ".[test]"
