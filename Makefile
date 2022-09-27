venv-create:
	python -m venv Ferdelance_env

venv-recreate:
	rm -rf Ferdelance_env
	python -m venv Ferdelance_env

venv-dev-install:
	pip install federated-learning-shared/
	pip install -e ".[test]"
