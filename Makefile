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
