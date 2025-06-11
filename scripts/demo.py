# -*- coding: utf-8 -*-
# <nbformat>3.0</nbformat>

# <headingcell level=1>

# Simple example of how to use the ORM

# <headingcell level=2>

# Import necessary stuff

# <codecell>

from modelmeta import v1 as modelmeta
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from optparse import OptionParser

# <headingcell level=2>

# Create the database session

# <codecell>

engine = create_engine(
    "postgresql://hiebert:mypass@windy.pcic.uvic.ca/pcic_meta?sslmode=require"
)
Session = sessionmaker(bind=engine)
session = Session()

# <headingcell level=2>

# Query all of some type of object

# <codecell>

q = session.query(modelmeta.DataFile)
q.count()


# <headingcell level=2>

# Query related objects simply by accessing the object properties

# <codecell>

myfile = q[0]

# <codecell>

myfile.run.files[0].timeset.start_date

# <codecell>

myfile.run.driving_run, myfile.run.initialized_from_run

# <codecell>

myfile.data_file_variables[0].variable
