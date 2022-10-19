build:
	cd federated-learning-client && $(MAKE) build
	cd federated-learning-server && $(MAKE) build

start:
	docker-compose up -d

stop:
	docker-compose down

logs:
	docker-compose logs -f server worker client1 client2

clear:
	docker volume rm ferdelance-server-data
	docker volume rm ferdelance-client-data
	docker volume rm ferdelance-db-data
