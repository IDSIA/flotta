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
	docker volume rm ${project}_ferdelance-client-1-data
	docker volume rm ${project}_ferdelance-client-2-data
	docker volume rm ${project}_ferdelance-db-data

create:
	python -m venv Ferdelance_env

del:
	rm -rf Ferdelance_env

dev:
	cd federated-learning-workbench && pip install federated-learning-shared/
	cd federated-learning-workbench && pip install ".[test]" && rm -rf build
	cd federated-learning-client    && pip install ".[test]" && rm -rf build
	cd federated-learning-server    && pip install ".[test]" && rm -rf build
	pip install -e ".[test]"

test:
	python tests/test_distributed.py
