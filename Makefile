all: install install-pipenv test

install:
	sudo apt-get update
	sudo apt-get install postgresql \
			postgresql-client \
			libhdf5-serial-dev \
			libnetcdf-dev \
			libspatialite-dev \
			postgresql-12-postgis-3

install-pipenv:
	pip install pipenv
	pipenv install
	pipenv install --dev

test: install
	pipenv run pytest -v