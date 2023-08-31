all: install install-pipenv test

install:
	sudo apt-get install postgresql-14 \
		postgresql-client-14 \
		libhdf5-serial-dev \
		libnetcdf-dev \
		libspatialite-dev \
		postgresql-14-postgis-3

install-pipenv:
	sudo apt-get install pipenv
	pipenv install
	pipenv install --dev

test: install
	pipenv run pytest -v
