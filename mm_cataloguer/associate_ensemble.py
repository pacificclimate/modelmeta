from argparse import ArgumentParser
import logging

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from nchelpers import CFDataset
from mm_cataloguer.index_netcdf import find_data_file_by_id_hash_filename
from modelmeta import Ensemble, EnsembleDataFileVariables


formatter = logging.Formatter('%(asctime)s %(levelname)s: %(message)s', "%Y-%m-%d %H:%M:%S")
handler = logging.StreamHandler()
handler.setFormatter(formatter)

logger = logging.getLogger(__name__)
logger.addHandler(handler)
logger.setLevel(logging.DEBUG)


def find_ensemble(sesh, name, version):
    """Find existing Ensemble record matching name and version

    :param sesh: modelmeta database session
    :param name: (str) name of ensemble
    :param version: (float) version of ensemble
    :return: existing Ensemble record or None
    """
    q = sesh.query(Ensemble)\
        .filter(Ensemble.name == name) \
        .filter(Ensemble.version == version)
    return q.first()


def associate_ensemble_to_cf(sesh, cf, ensemble_name, ensemble_ver):
    """Associate an existing NetCDF file to an existing ensemble in the modelmeta database.
    Raise an error if no ensemble in the database matches the given name and version.
    Do nothing if the association already exists.

    :param sesh: modelmeta database session
    :param cf: CFDatafile object representing NetCDF file
    :param ensemble_name: (str) name of ensemble
    :param ensemble_ver: (float) version of ensemble
    :return: existing DataFile record or None
    """
    ensemble = find_ensemble(sesh, ensemble_name, ensemble_ver)
    if not ensemble:
        raise ValueError("No existing ensemble matches name = '{}' and version = '{}'"
                         .format(ensemble_name, ensemble_ver))

    id_match, hash_match, filename_match = find_data_file_by_id_hash_filename(sesh, cf)

    if not all((id_match, hash_match, filename_match)):
        logger.warning('Skipping file: does not perfectly match any indexed file')
        return None

    data_file = id_match or hash_match or filename_match  # Any will do; this is robust to changes to matching criteria

    for data_file_variable in data_file.data_file_variables:
        ensemble_dfv = sesh.query(EnsembleDataFileVariables)\
            .filter(EnsembleDataFileVariables.ensemble_id == ensemble.id)\
            .filter(EnsembleDataFileVariables.data_file_variable_id == data_file_variable.id)\
            .first()
        if ensemble_dfv:
            logger.info('Assocation for variable id {} to ensemble already exists'.format(data_file_variable.id))
        else:
            logger.info('Assocating variable id {} to ensemble'.format(data_file_variable.id))
            ensemble_dfv = EnsembleDataFileVariables(ensemble_id=ensemble.id, data_file_variable_id=data_file_variable.id)
            sesh.add(ensemble_dfv)

    return data_file


def associate_ensemble_to_file(filepath, session, ensemble_name, ensemble_ver):
    """Associate an existing NetCDF file in modelmeta database to a specified ensemble.

    :param filepath: filepath of NetCDF file
    :param session: database session for access to modelmeta database
    :param ensemble_name: (str) name of ensemble
    :param ensemble_ver: (float) version of ensemble
    :return: DataFile object for file indexed
    """
    logger.info('Processing file: {}'.format(filepath))
    with CFDataset(filepath) as cf:
        data_file = associate_ensemble_to_cf(session, cf, ensemble_name, ensemble_ver)
    return data_file


def associate_ensemble_to_files(filepaths, dsn, ensemble_name, ensemble_ver):
    """Associate a list of NetCDF files in modelmeta database to a specified ensemble.

    :param filepaths: list of files to index
    :param dsn: connection info for the modelmeta database to update
    :param ensemble_name: (str) name of ensemble
    :param ensemble_ver: (float) version of ensemble
    :return: list of DataFile objects; one for each file associated
    """
    engine = create_engine(dsn)
    session = sessionmaker(bind=engine)()

    result = [associate_ensemble_to_file(f, session, ensemble_name, ensemble_ver) for f in filepaths]

    return result


if __name__ == '__main__':
    parser = ArgumentParser(description='Associate an ensemble to datafiles')
    parser.add_argument("-d", "--dsn", help="DSN for metadata database")
    parser.add_argument("-n", "--ensemble-name", dest='ensemble_name', help="Name of ensemble")
    parser.add_argument("-v", "--ensemble-ver", dest='ensemble_ver', type=float, help="Version of ensemble")
    parser.add_argument('filepaths', nargs='+', help='Files to process')
    args = parser.parse_args()
    associate_ensemble_to_files(args.filepaths, args.dsn, args.ensemble_name, args.ensemble_ver)
