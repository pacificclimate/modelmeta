import sys
import logging
from argparse import ArgumentParser
from pkg_resources import resource_filename

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.schema import MetaData

if __name__ == '__main__':
    parser = ArgumentParser()
    parser.add_argument("-d", "--dsn", help="Source database DSN from which to read")
    parser.add_argument("-e", "--ensemble", help="Ensemble to copy from the database")
    parser.add_argument("-v", "--version", type=int, choices=[1, 2], help="Schema version the database is using")
    parser.set_defaults(
        ensemble='canada_map',
        version=2
    )
    args = parser.parse_args()

    logger = logging.getLogger(__name__)
    logging.basicConfig(stream=sys.stdout, level=logging.INFO)

    common_sequences = [('emissions', 'emission_id'),
                        ('ensembles', 'ensemble_id'),
                        ('level_sets', 'level_set_id'),
                        ('time_sets', 'time_set_id'),
                        ('models', 'model_id'),
                        ('variables', 'variable_id'),
                        ('grids', 'grid_id'),
                        ('runs', 'run_id'),
                        ('data_files', 'data_file_id'),
                        ('data_file_variables', 'data_file_variable_id')]

    if args.version == 1:
        from modelmeta import v1 as modelmeta
        if not args.dsn:
            args.dsn = 'postgresql://httpd_meta@monsoon.pcic/pcic_meta_v1?sslmode=require'
        sequences = common_sequences + [('presentations', 'presentation_id'),
                                        ('levels', 'level_id'),
                                        ('qc_flags', 'qc_flag')]
    elif args.version == 2:
        import modelmeta
        if not args.dsn:
            args.dsn = 'postgresql://httpd_meta@monsoon.pcic/pcic_meta?sslmode=require'

        sequences = common_sequences + [('variable_aliases', 'variable_alias_id'),
                                        ('level_sets', 'level_set_id'),
                                        ('qc_flags', 'qc_flag_id')]


    read_engine = create_engine(args.dsn)
    test_dsn = modelmeta.test_dsn
    logger.info("Using dsn: {}".format(test_dsn))
    write_engine = create_engine(test_dsn)
    meta = MetaData(bind=write_engine)
    meta.reflect(bind=read_engine)


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


