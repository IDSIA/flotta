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
	docker-compose logs -f 

nuke:
	docker-compose down
	docker volume rm federated-learning-client_Ferdelance-client-data
	rm -rf ./workdir/*

venv-create:
	python -m venv Ferdelance_env

venv-recreate:
	rm -rf Ferdelance_env
	python -m venv Ferdelance_env

venv-dev-install:
	pip install federated-learning-shared/
	pip install -e ".[test]"
