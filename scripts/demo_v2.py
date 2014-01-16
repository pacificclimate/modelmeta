# -*- coding: utf-8 -*-
# <nbformat>3.0</nbformat>

# <headingcell level=1>

# Simple example of how to use the ORM

# <headingcell level=2>

# Import necessary stuff

# <codecell>

from modelmeta import v2 as modelmeta
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from optparse import OptionParser

# <headingcell level=2>

# Create the database session

# <codecell>

engine = create_engine('postgresql://httpd_meta@monsoon.pcic.uvic.ca/pcic_meta?sslmode=require')
Session = sessionmaker(bind=engine)
session = Session()

# <headingcell level=2>

# Query all of some type of object

# <codecell>

q = session.query(modelmeta.Ensemble)
q.count(), [x.name for x in q.all()]

# <headingcell level=2>

# Look up objects based on query parameters

# <codecell>

q = session.query(modelmeta.DataFileVariable).\
join(modelmeta.EnsembleDataFileVariables).\
join(modelmeta.Ensemble).\
filter(modelmeta.Ensemble.name == 'canada_map')

# <headingcell level=2>

# Or use the built in relational mapping

# <codecell>

myensemble = session.query(modelmeta.Ensemble).filter(modelmeta.Ensemble.name == 'canada_map').first()
print 'Ensemble: ' + myensemble.name + ' with ' + str(len(myensemble.data_file_variables)) + ' data_file_vars'

# <headingcell level=2>

#  You can map all the way down the rabbit hole

# <codecell>

mymodel = myensemble.data_file_variables[0].file.run.model
print mymodel.short_name

# <headingcell level=3>

# And back up again

# <codecell>

mymodel.runs[0].files[0].data_file_variables[0].ensembles[0].name

# <headingcell level=3>

# And around in circles

# <codecell>

myensemble.data_file_variables[0].\
file.run.emission.runs[0].files[0].data_file_variables[0].\
grid.data_file_variables[0].variable_alias.data_files[0].\
data_file_variables[0].ensembles[0].name

# <codecell>


