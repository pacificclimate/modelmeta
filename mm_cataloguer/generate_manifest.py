from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from modelmeta import DataFile, Ensemble
from datetime import datetime

def list_filepaths(session, ensemble, since):
    if ensemble == ["all"]:
        ensemble = session.query(Ensemble.name).all()
    since = datetime.strptime(since, "%Y-%m-%d")
    print(session.query(DataFile.filename).filter(Ensemble.name.in_(ensemble))
                                          .filter(DataFile.index_time >= since).distinct().all())

def Generate_Manifest(dsn, ensemble, since):
    engine = create_engine(dsn)
    session = sessionmaker(bind=engine)()
    list_filepaths(session, ensemble, since) 
