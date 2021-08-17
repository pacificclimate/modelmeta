all: install install-pipenv test

install:
	sudo apt-get install postgresql-13 \
		postgresql-client-13 \
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