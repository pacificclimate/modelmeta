import sys
import logging
from argparse import ArgumentParser
from pkg_resources import resource_filename

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.schema import MetaData

import modelmeta

if __name__ == '__main__':
    parser = ArgumentParser()
    parser.add_argument("-d", "--dsn", help="Source database DSN from which to read")
    parser.add_argument("-e", "--ensemble" help="Ensemble to copy from the database")
    parser.set_defaults(dsn='postgresql://pcic_meta@monsoon.pcic/pcic_meta?sslmode=require', ensemble='bc_prism')
    args = parser.parse_args()
                        
    logger = logging.getLogger(__name__)
    logging.basicConfig(stream=sys.stdout, level=logging.INFO)
    read_engine = create_engine(args.dsn)
    test_dsn = 'sqlite+pysqlite:///{0}'.format(resource_filename('modelmeta', 'data/mddb.sqlite'))
    write_engine = create_engine(test_dsn)
    meta = MetaData(bind=write_engine)
    meta.reflect(bind=read_engine)

    sequences = [('emissions', 'emission_id'),
                 ('ensembles', 'ensemble_id'),
                 ('level_sets', 'level_set_id'),
                 ('presentations', 'presentation_id'),
                 ('time_sets', 'time_set_id'),
                 ('qc_flags', 'qc_flag'),
                 ('models', 'model_id'),
                 ('variables', 'variable_id'),
                 ('grids', 'grid_id'),
                 ('runs', 'run_id'),
                 ('levels', 'level_id'),
                 ('data_files', 'data_file_id'),
                 ('data_file_variables', 'data_file_variable_id')]

    logger.info("Unsetting all of the sequence defaults")
    for table_name, column_name in sequences:
        meta.tables[table_name].columns[column_name].server_default = None

    logger.info("Creating all of the tables")
    meta.create_all()

    rSession = sessionmaker(bind=read_engine)()
    wSession = sessionmaker(bind=write_engine)()

    logger.info("Querying the data files")
    q = rSession.query(modelmeta.Ensemble).filter(modelmeta.Ensemble.name == args.ensemble)
    ens = q.first()
    logger.info("Adding ensembles to sqlite database")
    merged_object = wSession.merge(ens)
    wSession.add(merged_object)
    wSession.commit()


