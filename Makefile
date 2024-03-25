export DOCKER_BUILDKIT=1

# folder cleanup
clean:
	rm -rf workdir/ storage/ logs/ sqlite.db tests/test_sqlite.db ferdelance*.log*


# launch standalone mode
standalone:
	python -m ferdelance.standalone -c conf/config.yaml

# docker build 
build:
	docker build -t idsia.ferdelance:latest .

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
