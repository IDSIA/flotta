build:
	docker-compose build

start:
	docker-compose up -d

stop:
	docker-compose down

reload:
	docker-compose build
	docker-compose up -d

nuke:
	docker-compose down
	docker volume rm federated-learning-server_spearhead-db-data
	docker volume rm federated-learning-server_spearhead-server-data

venv-recreate:
	rm -rf Spearhead_env
	python -m venv Spearhead_env

venv-install:
	pip install federated-learning-shared/
	pip install -e .
	pip install -r requirements.txt
	pip install -r requirements.test.txt
