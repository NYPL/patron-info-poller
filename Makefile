.DEFAULT: help

help:
	@echo "make help"
	@echo "    display this help statement"
	@echo "make run"
	@echo "    run the application in development mode"
	@echo "make test"
	@echo "    run associated test suite with pytest"
	@echo "make lint"
	@echo "    lint project files using the black linter"

run:
	docker image build -t patron-info-poller:local .; \
	docker container run -e ENVIRONMENT=devel patron-info-poller:local

test:
	pytest

lint:
	black ./ --check --exclude="(env/)|(tests/)"