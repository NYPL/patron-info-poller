.DEFAULT: help

help:
	@echo "make help"
	@echo "    display this help statement"
	@echo "make run"
	@echo "    run the application in development mode"
	@echo "make test"
	@echo "    run associated test suite with pytest"
	@echo "make lint"
	@echo "    lint project files using the flake8 linter"

run:
	export ENVIRONMENT=devel; \
	python3 main.py

test:
	pytest

lint:
	flake8 --exclude *env