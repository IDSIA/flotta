project:=testbench

build:
	cd federated-learning-client && $(MAKE) build
	cd federated-learning-server && $(MAKE) build

start:
	docker-compose -p ${project} up -d

stop:
	docker-compose -p ${project} down

logs:
	docker-compose -p ${project} logs -f server worker client1 client2

logs-1:
	docker-compose -p ${project} logs -f client1

logs-2:
	docker-compose -p ${project} logs -f client2

logs-clients:
	docker-compose -p ${project} logs -f client1 client2

logs-server:
	docker-compose -p ${project} logs -f server worker

clear:
	docker volume rm ${project}_ferdelance-server-data
	docker volume rm ${project}_ferdelance-client-data
	docker volume rm ${project}_ferdelance-db-data

create:
	python -m venv Ferdelance_env

dev:
	pip install federated-learning-workbench/federated-learning-shared/
	pip install federated-learning-workbench/
	pip install -e .
