all: install install-poetry test

install:
	sudo apt-get install postgresql-16 \
		postgresql-client-16 \
		libhdf5-serial-dev \
		libnetcdf-dev \
		libspatialite-dev \
		postgresql-16-postgis-3

install-poetry:
	pip install poetry
	poetry install --extras test 

test:
	poetry run pytest -v
