export DOCKER_BUILDKIT=1

# folder cleanup
clean:
	rm -rf workdir/ storage/ logs/ sqlite.db tests/test_sqlite.db flotta*.log*


# launch standalone mode
standalone:
	python -m flotta.standalone -c conf/config.yaml

# docker build 
build:
	docker build -t idsia.flotta:latest .

# development
create:
	python -m venv env

delete:
	rm -rf env

recreate:
	rm -rf env
	python -m venv env

dev:
	pip install -e ".[dev]"

test:
	pytest
