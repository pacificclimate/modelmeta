# coding: utf-8

# # Simple example of how to use the ORM

# ## Import necessary stuff

# In[1]:

import modelmeta
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from optparse import OptionParser


# ## Create the database session

# In[2]:

engine = create_engine("postgresql://httpd_meta@atlas.pcic/pcic_meta?sslmode=require")
Session = sessionmaker(bind=engine)
session = Session()


# ## Query all of some type of object

# In[3]:

q = session.query(modelmeta.Ensemble)
q.count(), [x.name for x in q.all()]


# ## Look up objects based on query parameters

# In[4]:

ensemble_name = "canada_map"
mydatafilevars = (
    session.query(modelmeta.DataFileVariable)
    .join(modelmeta.EnsembleDataFileVariables)
    .join(modelmeta.Ensemble)
    .filter(modelmeta.Ensemble.name == ensemble_name)
    .all()
)
print(
    "Ensemble: "
    + ensemble_name
    + " with "
    + str(len(mydatafilevars))
    + " data_file_vars"
)


# ## Or better yet, use the built in relational mapping

# In[5]:

myensemble = (
    session.query(modelmeta.Ensemble)
    .filter(modelmeta.Ensemble.name == "canada_map")
    .first()
)
print(
    "Ensemble: "
    + myensemble.name
    + " with "
    + str(len(myensemble.data_file_variables))
    + " data_file_vars"
)


# ##  You can map all the way down the rabbit hole

# In[6]:

mymodel = myensemble.data_file_variables[0].file.run.model
print(mymodel.short_name)


# ### And back up again

# In[7]:

mymodel.runs[0].files[0].data_file_variables[0].ensembles[0].name


# ### And around in circles

# In[8]:

myensemble.data_file_variables[0].file.run.emission.runs[0].files[
    0
].data_file_variables[0].grid.data_file_variables[0].variable_alias.data_files[
    0
].data_file_variables[
    0
].ensembles[
    0
].name


# In[ ]:
