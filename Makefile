.PHONY: install install-dev lint format all

install:
	pip install .

install-dev:
	pip install .[dev]

lint:
	pylint ams_background_tasks

format:
	isort .
	black .

all:
	pylint ams_background_tasks
	isort .
	black .
	pip install .[dev]
	