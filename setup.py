import string
from setuptools import setup

__version__ = (0, 0, 7)

setup(
    name="modelmeta",
    description="An ORM representation of the model metadata database",
    keywords="sql database climate",
    packages=['modelmeta', 'mm_cataloguer'],
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
        nchelpers>=4.0.0
        python-dateutil
        sqlparse
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
        '''.split()
    },
    include_package_data = True,
    scripts = '''
        scripts/copyproddb.py
        scripts/mktestdb.py
        scripts/list.py
        mm_cataloguer/index_netcdf.py
        mm_cataloguer/associate_ensemble.py
    '''.split(),
    classifiers=['Development Status :: 5 - Production/Stable',
                 'Environment :: Console',
                 'Intended Audience :: Developers',
                 'Intended Audience :: Science/Research',
                 'License :: OSI Approved :: GNU General Public License v3 (GPLv3)',
                 'Operating System :: OS Independent',
                 'Programming Language :: Python :: 2.7',
                 'Programming Language :: Python :: 3.4',
                 'Programming Language :: Python :: 3.5',
                 'Programming Language :: Python :: 3.6',
                 'Topic :: Scientific/Engineering',
                 'Topic :: Database',
                 'Topic :: Software Development :: Libraries :: Python Modules'
                 ]
)
