from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from modelmeta import DataFile, DataFileVariable, Ensemble, EnsembleDataFileVariables
from datetime import datetime

def list_filepaths(session, ensemble, since):
    if ensemble == ["all"]:
        ensemble = ["all_files"]
    since = datetime.strptime(since, "%Y-%m-%d")
    print(session.query(DataFile.filename).join(DataFileVariable)
                                          .join(EnsembleDataFileVariables)
                                          .join(Ensemble)
                                          .filter(Ensemble.name.in_(ensemble))
                                          .filter(DataFile.index_time >= since).all())

def Generate_Manifest(dsn, ensemble, since):
    engine = create_engine(dsn)
    session = sessionmaker(bind=engine)()
    list_filepaths(session, ensemble, since) 
