import string
from setuptools import setup

__version__ = (0, 0, 6)

setup(
    name="modelmeta",
    description="An ORM representation of the model metadata database",
    keywords="sql database climate",
    packages=['modelmeta', 'mm_cataloguer'],
    version='.'.join(str(d) for d in __version__),
    url="http://www.pacificclimate.org/",
    author="James Hiebert",
    author_email="hiebert@uvic.ca",
    zip_safe=True,
    install_requires = '''
        sqlalchemy
        psycopg2
        numpy
        netCDF4
        nchelpers>=1.0.3
    '''.split(),
    package_data = {'modelmeta': ['data/mddb-v1.sqlite', 'data/mddb-v2.sqlite', 'data/tiny_gcm.nc']},
    include_package_data = True,
    scripts = ['scripts/mkblankdb.py', 'scripts/mktestdb.py'],
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
