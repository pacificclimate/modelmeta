=========
modelmeta
=========

The `modelmeta` package is a Python package that provides an `Object Relational Mapping (ORM) <http://en.wikipedia.org/wiki/Object-relational_mapping>`_ layer for accessing the `Pacific Climate Impacts Consortium (PCIC) <http://www.pacificclimate.org/>`_ (PCIC)'s database of `coverage data <http://en.wikipedia.org/wiki/Coverage_data>`_ metadata. The pacakge provides model classes for each of the tables in the database in order to provide a ... querying 

With this package, one can recreate the database schema in `PostgreSQL <http://www.postgresql.org>`_ or `SQLite <http://www.sqlite.org>`_ and/or use the package as an object mapper for programmatic database access. `modelmeta` uses `SQLAlchemy <http://www.sqlalchemy.org>`_ to provide the ORM layer.

The intent of the database itself is to separate the small, inexpensive, structured metadata and attribute information (stored in the database) from the expensive-to-access bulk spatiotemporal data (stored on disk in multidimensional files).

--------------
How to Install
--------------

One can install `modelmeta` using the standard methods of any other Python package.

1. clone our repository and run the setup script

    $ git clone https://github.com/pacificclimate/modelmeta
    $ cd modelmeta
    $ python setup.py install

2. or just point `pip` to our `GitHub repo <https://github.com/pacificclimate/modelmeta>`_:

    $ pip install git+https://github.com/pacificclimate/modelmeta

------------------------------
What is Climate Coverage Data?
------------------------------

Climate coverage data (or "raster data" or "spatiotemporal data") consist of large data fields, typically over two or three dimensions in space plus a time dimension. Depending on the resolution in each axis, the data can typically be quite large in size. Typically there are several-to-many output quantities (e.g. temperature, precipiation, wind speed/direction) and often there can be multiple scenarios, multiple model implementations, and multiple runs of each model further exacerbating the size of the data.
