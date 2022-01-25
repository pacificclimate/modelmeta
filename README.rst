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

If you wish to install ``modelmeta`` manually, follow the steps below.

#. Clone the repository::

    $ git clone https://github.com/pacificclimate/modelmeta

#. Create a virtual environment::

    $ cd modelmeta
    $ pipenv install # --dev for development packages


Scripts to populate a PCIC modelmeta database
===========================================

This repository contains two convenient scripts that add data files to an existing modelmeta database so that our websites can access data from them. They are installed when the package is installed.

Indexing new files with index_netcdf
------------------------------------
``index_netcdf`` adds one or more netCDF climate data files to a PCIC modelmeta-format database::

  index_netcdf -d postgresql://username:password@monsoon.pcic.uvic.ca/database /path/to/files/*.nc

Usernames and passwords can be found in Team Password Manager. To add files to the data portal, use database ``pcic_meta``; to add files to PCEX or Plan2adapt, use database ``ce_meta_12f290b63791``.

In order to determine the metadata of the file, the ``index_metadata`` script scans its netCDF attributes. If the file does not have all the `required attributes <https://pcic.uvic.ca/confluence/display/CSG/PCIC+metadata+standard+for+downscaled+data+and+hydrology+modelling+data>`_ specified, the ``index_metadata`` script will be unable to proceed. You can look at a file's attributes with the command::

  ncdump -h file.nc


and update attributes using the ``update_metadata`` script in the `climate-explorer-data-prep <https://github.com/pacificclimate/climate-explorer-data-prep>`_ respository. If you update file attributes, log your update YAML and a list of which files you used with in in the `data-prep-actions <https://github.com/pacificclimate/data-prep-actions>`_ repository, in case you need to reprocess or check the files later.

Making files accessible to PCIC projects with associate_ensemble
----------------------------------------------------------------

Once files have been indexed into the database, they need to be added to individual ensembles; each ensemble is associated with a particular project or website and contains all data files needed to support the functioning of that component. In most cases, a file will be added to more than one ensemble::

  associate_ensemble -n ensemble_name -v 1 -d postgresql://username:password@monsoon.pcic.uvic.ca/database /path/to/files/*.nc

**Available ensembles, or where should I put this data anyway?**

Most ensembles represent groupings of related files that users can interact with (view maps, download data, create graphs, etc) using a specific PCIC tool. Plan2adapt, the data portal, and PCEX all use ensembles in this way.

Plan2adapt uses a single ensemble which represents a list of all the files a user can view. The name of this ensemble is set when plan2adapt is deployed, as the environment variable ``REACT_APP_ENSEMBLE_NAME``. You can see the environment variables for a docker container running plan2adapt with ``docker exec container_name env``.

The data portal uses a separate ensemble for each portal, which represents a list of the data files a user can download from that portal. Each portal's ensemble is hard-coded in that portal's definition file, in `pdp/portals <https://github.com/pacificclimate/pdp/tree/master/pdp/portals>`_ .

PCEX is flexible about which ensembles it uses. A PCEX URL encodes both a UI and an ensemble which specifies which data is to be viewed with that UI. In theory you can look at any ensemble with any UI, but in practice, UIs make assumptions about the type of data available and most combinations won't work. In most cases, users access the various PCEX UIs pages via the `link bar at the top of the page <https://github.com/pacificclimate/climate-explorer-frontend/blob/master/src/components/DataTool.js>`_. The ``navSubpath`` variable has the UI before the slash and the ensemble after it. PCEX UIs that display hydrological data for a watershed also have an additional ensemble that contains files that describe the geography of the watershed; this data cannot be directly viewed by the user but is required for some calculations. A list of these geographic ensembles can be `found <https://github.com/pacificclimate/climate-explorer-frontend/blob/master/src/data-services/ce-backend.js>`_ in ``getWatershedGeographyName()``.

There are also three special ensembles used by PCIC internal tools, not web portals.

* The ``all_files`` ensemble in the ``ce_meta_12f290b63791`` contains every file in the database, with the exception of time-invariant files. It is used with various scripts that `test <https://github.com/pacificclimate/data-prep-actions/blob/master/actions/test-ncwms-instance/DESCRIPTION.md>`_ new functionality across all files.

* The ``p2a_rules`` ensemble in the ``ce_meta_12f290b63791`` database contains all the information needed by plan2adapt's rules engine; it is used by the `scripts <https://github.com/pacificclimate/data-prep-actions/blob/master/actions/precalculate-p2a-regions/DESCRIPTION.md>`_ which pre-generate rules engine results for plan2adapt, which are too slow to process in real time.

* The ``downscaled_canada`` ensemble in the ``pcic_meta`` database contains all datafiles for which map images can be generated for the portal website. It is used by `the ncWMS proxy <https://github.com/pacificclimate/ncWMS-mm-rproxy>`_ to generate map images for the data portal. When you add new files to this ensemble, you will need to reboot the ncWMS proxy to load the new files, or their maps will not be available.

Deleting files from the databases
---------------------------------

Unfortunately, we don't currently have a script that can delete files from the databases. If you accidentally index a file with bad metadata and need to get rid of it, at present the only way is to log on to the database directly with ``psql`` or ``pgadmin``.


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
