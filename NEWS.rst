News / Release Notes
====================

0.3.0
-----

*Release Date: 2018-Aug-22*

* Indexer handles discrete sampling geometry (station-based, non-gridded)
  data files, such as streamflow output files. Requires nchelpers>=5.5.0.

0.2.0
-----

*Release Date: 2017-Nov-23*

* Indexer handles non-CMIP*/climate data files. Requires nchelpers>=5.2.0.
  This is a workaround pending a more complete solution to the problem of storing metadata
  for non-CMIP*/climate data derived files.

0.1.2
-----

*Release Date: 2017-Nov-03*

* Indexer handles files with 360-day calendars. Requires nchelpers>=5.1.2.

0.1.1
-----

*Release Date: 2017-Oct-06*

* Make ``index_netcdf``, ``associate_ensemble``, ``list``, and ``list-csv`` executable on the command line.

0.1.0
-----

*Release Date: 2017-Oct-06*

OK, so we shouldn't have waited so long to make a new release.

Key changes:

* ``index_netcdf``
  * Meet revised PCIC metadata standard
  * Prevent errors during processing from leaving a mess in the database: Handle each file's processing in one transaction
  * Fix bug in converting numpy types to Postgres database types: Define psycopg2 adapters for numpy types
  * Store full path (realpath) to indexed file
* ``associate_ensemble``:
  * Associate by regex or perfect match to DataFile.filename
  * Allow specifying variable(s) to associate

Other changes:

* Use a Postgres test database
* Add flag in ``ncwms_configurator`` to output ncWMS version 2
* Make mm_cataloguer code meet PEP8 standards
* Migrate utility functions ``get_climatology_bounds``, ``get_extended_time_range`` to nchelpers
* Add ``ce_meta`` database to ``alembic.ini``; update related documentation
* Add Alembic migration to create initial v2 database
* Add database content listing scripts
* Add UML diagram (in Dia) of ORM v2 view of database

0.0.7
-----

*Release Date: 2017-Jul-24*

* Adds value 'seasonal' to enum type timescale for TimeSet.time_resolution
* Introduces Alembic database migration manager.
* Defines Alembic migration for enum type as above.
* Translate indexer to Python: ``index_netcdf.py`` - better stronger faster
* Other things it is taking too long to untangle


0.0.6
-----

*Release Date: 2015-Aug-26*

Description not available

0.0.5
-----

*Release Date: 2014-Oct-22*

Description not available

0.0.4
-----

*Release Date: 2014-Oct-22*

Description not available
