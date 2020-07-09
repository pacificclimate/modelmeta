import string
from setuptools import setup

__version__ = (1, 0, 0)

setup(
    name="modelmeta",
    description="An ORM representation of the model metadata database",
    keywords="sql database climate",
    packages=['modelmeta', 'mm_cataloguer', 'ncwms_configurator'],
    version='.'.join(str(d) for d in __version__),
    url="http://www.pacificclimate.org/",
    author="James Hiebert",
    author_email="hiebert@uvic.ca",
    zip_safe=False,
    install_requires = '''
        sqlalchemy
        alembic
        psycopg2
        numpy
        netCDF4
        nchelpers>=5.5.0
        python-dateutil
        sqlparse
        PyCRS
        lxml
    '''.split(),
    package_data = {
        'modelmeta': '''
            data/mddb-v1.sqlite
            data/mddb-v2.sqlite
            data/bad_tiny_gcm.nc
            data/tiny_gcm.nc
            data/tiny_downscaled.nc
            data/tiny_hydromodel_gcm.nc
            data/tiny_gcm_climo_yearly.nc
            data/tiny_gcm_climo_seasonal.nc
            data/tiny_gcm_climo_monthly.nc
            data/tiny_gridded_obs.nc
            data/tiny_streamflow.nc
        '''.split()
    },
    include_package_data = True,
    scripts = '''
        scripts/copyproddb.py
        scripts/mktestdb.py
        scripts/list
        scripts/list-csv
        scripts/index_netcdf
        scripts/associate_ensemble
        scripts/ncwms_configurator
        scripts/generate_manifest
    '''.split(),
    classifiers=['Development Status :: 5 - Production/Stable',
                 'Environment :: Console',
                 'Intended Audience :: Developers',
                 'Intended Audience :: Science/Research',
                 'License :: OSI Approved :: GNU General Public License v3 (GPLv3)',
                 'Operating System :: OS Independent',
                 'Programming Language :: Python :: 3.6',
                 'Programming Language :: Python :: 3.7',
                 'Topic :: Scientific/Engineering',
                 'Topic :: Database',
                 'Topic :: Software Development :: Libraries :: Python Modules'
                 ]
)
