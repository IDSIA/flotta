PROJECT:=spearhead

build:
	docker-compose -p ${PROJECT} -f federated-learning-client/docker-compose.yaml build
	docker-compose -p ${PROJECT} -f federated-learning-server/docker-compose.yaml build

start:
	docker-compose -p ${PROJECT} -f federated-learning-client/docker-compose.yaml up -d
	docker-compose -p ${PROJECT} -f federated-learning-server/docker-compose.yaml up -d

stop:
	docker-compose -p ${PROJECT} -f federated-learning-client/docker-compose.yaml down
	docker-compose -p ${PROJECT} -f federated-learning-server/docker-compose.yaml down

clear:
	docker volume rm ${PROJECT}_spearhead-client-data
	docker volume rm ${PROJECT}_spearhead-server-data
	docker volume rm ${PROJECT}_spearhead-db-data

