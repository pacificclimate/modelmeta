import string
from setuptools import setup

__version__ = (0, 0, 3)

setup(
    name="modelmeta",
    description="An ORM representation of the model metadata database",
    keywords="sql database climate",
    packages=['modelmeta'],
    version='.'.join(str(d) for d in __version__),
    url="http://www.pacificclimate.org/",
    author="James Hiebert",
    author_email="hiebert@uvic.ca",
    zip_safe=True,
    install_requires = ['sqlalchemy', 'psycopg2'],
    package_data = {'modelmeta': ['data/mddb-v1.sqlite', 'data/mddb-v2.sqlite']},
    include_package_data = True,
    classifiers=['Development Status :: 2 - Pre-Alpha',
                 'Environment :: Console',
                 'Intended Audience :: Developers',
                 'Intended Audience :: Science/Research',
                 'License :: OSI Approved :: GNU General Public License (GPL)',
                 'Operating System :: OS Independent',
                 'Programming Language :: Python',
                 'Topic :: Scientific/Engineering',
                 'Topic :: Software Development :: Libraries :: Python Modules'
                 ]
)
