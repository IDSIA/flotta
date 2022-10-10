create:
	python -m venv Ferdelance_env

recreate:
	rm -rf Ferdelance_env
	python -m venv Ferdelance_env

dev:
	pip install federated-learning-shared/
	pip install -e ".[test]"
