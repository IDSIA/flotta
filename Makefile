venv-create:
	python -m venv Spearhead_env

venv-recreate:
	rm -rf Spearhead_env
	python -m venv Spearhead_env

venv-dev-install:
	pip install -e ".[test]"
