export DOCKER_BUILDKIT=1

# folder cleanup
clean:
	rm -rf workdir/ storage/ logs/ sqlite.db tests/test_sqlite.db ferdelance*.log*


# launch standalone mode
standalone:
	python -m ferdelance.standalone -c conf/config.yaml

# docker build 
build:
	docker compose -f docker-compose.build.yaml build

build-node:
	docker compose -f docker-compose.build.yaml build ferdelance

build-repo:
	docker compose -f docker-compose.build.yaml build repository

# development
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
	pytest
