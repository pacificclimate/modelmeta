from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from modelmeta import DataFile, DataFileVariable, Ensemble, EnsembleDataFileVariables
import sys


def list_filepaths(session, ensembles, since, outfile):
    if ensembles == ["all"]:
        ensembles = ["all_files"]
    print(
        session.query(DataFile.filename)
        .join(DataFileVariable)
        .join(EnsembleDataFileVariables)
        .join(Ensemble)
        .filter(Ensemble.name.in_(ensembles))
        .filter(DataFile.index_time >= since)
        .all(),
        file=outfile,
    )


def generate_manifest(dsn, ensembles, since, outfile):
    engine = create_engine(dsn)
    session = sessionmaker(bind=engine)()
    if outfile != sys.stdout:
        outfile = open(outfile, "w")
    list_filepaths(session, ensembles, since, outfile)
