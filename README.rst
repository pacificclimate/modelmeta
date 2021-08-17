=========
modelmeta
=========

.. image:: https://github.com/pacificclimate/modelmeta/workflows/Python%20CI/badge.svg
   :target: https://github.com/pacificclimate/modelmeta

.. image:: https://github.com/pacificclimate/modelmeta/workflows/Pypi%20Publishing/badge.svg
   :target: https://github.com/pacificclimate/modelmeta

.. image:: https://codeclimate.com/github/pacificclimate/modelmeta/badges/gpa.svg
   :target: https://codeclimate.com/github/pacificclimate/modelmeta
   :alt: Code Climate

Overview
========

``modelmeta`` is a Python package that provides an
`Object Relational Mapping (ORM) <http://en.wikipedia.org/wiki/Object-relational_mapping>`_ layer
for accessing the `Pacific Climate Impacts Consortium (PCIC) <http://www.pacificclimate.org/>`_'s
database of `coverage data <http://en.wikipedia.org/wiki/Coverage_data>`_ metadata.
The package provides model classes for each of the tables in the database.

With this package, one can recreate the database schema in `PostgreSQL <http://www.postgresql.org>`_
or `SQLite <http://www.sqlite.org>`_ and/or use the package as an object mapper for programmatic database access.

``modelmeta`` uses `SQLAlchemy <http://www.sqlalchemy.org>`_ to provide the ORM layer, and
`Alembic <http://alembic.zzzcomputing.com/en/latest/>`_ to manage database creation and migration (see section
below).

The intent of the database itself is to separate the small, inexpensive, structured metadata and attribute information
(stored in the database) from the expensive-to-access bulk spatiotemporal data (stored on disk in multidimensional
files). It provides an efficiently searchable index of the bulk data files, and separates storage from indexing.

Installation
============

Installation is fully automated through ``make``::

    $ make

If you wish to install ``modelmeta`` using the standard methods follow the steps below.

#. Clone the repository::

    $ git clone https://github.com/pacificclimate/modelmeta

#. Create a virtual environment::

    $ cd modelmeta
    $ pipenv install # --dev for development packages

What is climate coverage data?
==============================

Climate coverage data (or "raster data" or "spatiotemporal data") consist of large data fields, typically over
two or three dimensions in space plus a time dimension. Depending on the resolution in each axis, the data can
typically be quite large in size. Typically there are several-to-many output quantities (e.g. temperature,
precipiation, wind speed/direction) and often there can be multiple scenarios, multiple model implementations,
and multiple runs of each model further exacerbating the size of the data.

Managing database migrations
============================

Introduction
------------

Modifications to ``modelmeta``'s schema definition are now managed using
`Alembic`_, a database migration tool based on SQLAlchemy.

In short, Alembic supports and disciplines two processes of database schema change:

- Creation of database migration scripts (Python programs) that modify the schema of a database.

- Application of migrations to specific database instances.

  - In particular, Alembic can be used to *create* a new instance of a ``modelmeta`` database by migrating an
    empty database to the current state. This is described in detail below.

For more information, see the `Alembic tutorial <http://alembic.zzzcomputing.com/en/latest/tutorial.html>`_.

History
-------

The existing instance of a ``modelmeta`` database (``monsoon/pcic_meta``) was created prior to the adoption of
Alembic, and therefore the timeline for Alembic database migrations is slightly confusing.

Timeline:

- *the distant past*: ``pcic_meta`` is created by mysterious primeval processes.

- *somewhat later*: ``modelmeta`` is defined using SQLAlchemy, mapping most (but not all) features of the existing
  ``pcic_meta`` database into an ORM.

- 2017-07-18:

  - Alembic is introduced.
  - Alembic is used to create migration ``614911daf883`` that adds item ``seasonal`` to ``timescale`` Enum.

- 2017-08-01:

  - The SQLAlchemy ORM is updated to reflect all features of the ``pcic_meta`` database.
    This mainly involves adding some missing indexes and constraints.

  - Alembic is used to create a logically-previous migration ``7847aa3c1b39`` that creates the initial
    database schema from an empty database.

  - The add-seasonal migration is modified to logically follow the initial-create migration.

Creating a new database
~~~~~~~~~~~~~~~~~~~~~~~

For a Postgres database
+++++++++++++++++++++++

A Postgres database is somewhat more elaborate to set up, but it is also the foundation of a production
database, not least because we use PostGIS.

Instructions:

#. Choose a name for your new database/schema, e.g., ``ce_meta``.

#. On the server of your choice (e.g., ``monsoon``):

   **Note**: These operations must be performed with high-level permissions.
   See the System Administrator to have these done or obtain permissions.

   For a record of such a creation, see `Redmine Issue 696 <https://redmine.pacificclimate.org/issues/696>`_.
   Permission setup was more complicated than anticipated.

   a. Create a new database with the chosen name, e.g., ``ce_meta``.

   #. Within that database, create a new schema with the chosen name, e.g., ``ce_meta``.

   #. Create new users, with the following permissions:

      - ``ce_meta`` (database owner): full permissions for table creation and read-write permissions
        in schemas ``ce_meta`` and ``public``
      - ``ce_meta_rw`` (database writer): read-write permissions in schemas ``ce_meta`` and ``public``
      - ``ce_meta_ro`` (database reader): read-only permissions in schemas ``ce_meta`` and ``public``

      and for each of them

      - ``search_path = ce_meta,public``

   #. `Enable PostGIS in the new database <http://postgis.net/install/>`_.

      - ``CREATE EXTENSION postgis;``
      - This creates the table ``spatial_ref_sys`` in schema ``public``. Check that.

#. Add a DSN for your new database, including the appropriate user name, to ``alembic.ini``. For example::

    [prod_ce_meta]
    sqlalchemy.url = postgresql://ce_meta@monsoon.pcic.uvic.ca/ce_meta

#. Create your new database with Alembic by ugrading the empty database to ``head``::

    alembic -x db=prod_ce_meta upgrade head

#. Have a beer.

For a SQLite database
+++++++++++++++++++++

A SQLite database is very simple to set up, but is normally used only for testing.

#. Add a DSN for your new database to ``alembic.ini``. This database need not exist yet (although the path does).
   For example::

    [my_test_database]
    sqlalchemy.url = sqlite:///path/to/test.sqlite

#. Create your new database with Alembic by ugrading the non-existent database to ``head``::

    alembic -x db=my_test_database upgrade head

#. Have a beer. Or at least a soda.

Updating the existing ``pcic_meta`` database
--------------------------------------------

**DEPRECATED**: `Decision taken not to modify pcic_meta <https://pcic.uvic.ca/confluence/display/CSG/pcic_meta%3A+Current+contents+and+update+plan+2017-Jul>`_
This content is retained in case that decision is revised in future.

This section is only of interest to PCIC.

Initialization
~~~~~~~~~~~~~~

Status: NOT DONE

The following things need to be done ONCE in order to bring ``pcic_meta`` under management by Alembic.

#. The table ``pcic_meta.alembic_version`` has already been created in ``pcic_meta`` by earlier operations.
   Its content is currently ``null``.

#. Place the value ``7847aa3c1b39`` in the single row and column of table ``pcic_meta.alembic_version`` in ``pcic_meta``.

   - This fakes the migration from an empty database to its nominal initial state (before add-seasonal migration).

Ongoing migrations
~~~~~~~~~~~~~~~~~~

Once the initialization steps have been completed, ongoing migrations are simple and standard:

#. Apply later migrations: ``alembic -x db=prod_pcic_meta upgrade head``

   - At the time of this writing (2017-08-01), that would be migration ``614911daf883``.
