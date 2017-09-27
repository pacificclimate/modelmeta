from argparse import ArgumentParser
import logging
import traceback

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from nchelpers import CFDataset
from mm_cataloguer.index_netcdf import find_data_file_by_id_hash_filename
from modelmeta import DataFileVariable, Ensemble, EnsembleDataFileVariables


formatter = logging.Formatter(
    '%(asctime)s %(levelname)s: %(message)s', "%Y-%m-%d %H:%M:%S")
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


def associate_ensemble_to_cf(sesh, ensemble_name, ensemble_ver, cf, var_names):
    """Associate variables in existing NetCDF file to an existing ensemble
    in the modelmeta database.

    Arg ``var_names`` specifies variables to associate; if falsy, associate
    all variables in file.

    Raise an error if no ensemble in the database matches the given name and
    version.

    Do nothing if any given association already exists.

    :param sesh: modelmeta database session
    :param ensemble_name: (str) name of ensemble
    :param ensemble_ver: (float) version of ensemble
    :param cf: CFDatafile object representing NetCDF file
    :param var_names: list of names of variables to associate
    :return: list of associated ``DataFileVariable``s
    """
    ensemble = find_ensemble(sesh, ensemble_name, ensemble_ver)
    if not ensemble:
        raise ValueError(
            "No existing ensemble matches name = '{}' and version = '{}'"
            .format(ensemble_name, ensemble_ver))

    id_match, hash_match, filename_match = \
        find_data_file_by_id_hash_filename(sesh, cf)

    if not all((id_match, hash_match, filename_match)):
        logger.warning(
            'Skipping file: does not perfectly match any indexed file')
        return None

    # Any match will do; this is robust to changes to matching criteria
    data_file = id_match or hash_match or filename_match

    associated_dfvs = []
    for data_file_variable in data_file.data_file_variables:
        if (not var_names
            or data_file_variable.netcdf_variable_name in var_names):
            ensemble_dfv = (
                sesh.query(EnsembleDataFileVariables)
                    .filter(EnsembleDataFileVariables.ensemble_id == ensemble.id)
                    .filter(EnsembleDataFileVariables.data_file_variable_id ==
                            data_file_variable.id)
                    .first()
            )
            if ensemble_dfv:
                logger.info(
                    'Assocation for variable id {} to ensemble already exists'
                    .format(data_file_variable.id))
            else:
                logger.info('Assocating variable id {} to ensemble'
                            .format(data_file_variable.id))
                ensemble_dfv = EnsembleDataFileVariables(
                    ensemble_id=ensemble.id,
                    data_file_variable_id=data_file_variable.id
                )
                sesh.add(ensemble_dfv)
            assocated_dfv = (
                sesh.query(DataFileVariable)
                .filter_by(id=ensemble_dfv.data_file_variable_id)
                .one()
            )
            associated_dfvs.append(assocated_dfv)

    return associated_dfvs


def associate_ensemble_to_file(
        Session, ensemble_name, ensemble_ver, filepath, var_names):
    """Associate an existing NetCDF file in modelmeta database to a specified
    ensemble.

    :param Session: database session factory for access to modelmeta database
    :param ensemble_name: (str) name of ensemble
    :param ensemble_ver: (float) version of ensemble
    :param filepath: filepath of NetCDF file
    :param var_names: list of names of variables to associate
    :return: list of ids of ``DataFileVariable``s associated
    """
    logger.info('Processing file: {}'.format(filepath))
    session = Session()
    data_file_variable_ids = []
    try:
        with CFDataset(filepath) as cf:
            data_file_variables = associate_ensemble_to_cf(
                session, ensemble_name, ensemble_ver, cf, var_names)
        data_file_variable_ids.extend([dfv.id for dfv in data_file_variables])
        session.commit()
    except:
        logger.error(traceback.format_exc())
        session.rollback()
    finally:
        session.close()
    return data_file_variable_ids


def associate_ensemble_to_files(
        dsn, ensemble_name, ensemble_ver, filepaths, var_names):
    """Associate a list of NetCDF files in modelmeta database to a specified
    ensemble.

    :param dsn: connection info for the modelmeta database to update
    :param ensemble_name: (str) name of ensemble
    :param ensemble_ver: (float) version of ensemble
    :param filepaths: list of files to index
    :param var_names: list of names of variables to associate
    :return: list of list of ids of ``DataFileVariable``s associated;
        one sublist for each file processed
    """
    engine = create_engine(dsn)
    Session = sessionmaker(bind=engine)

    result = [
        associate_ensemble_to_file(
            Session, ensemble_name, ensemble_ver, filepath, var_names)
        for filepath in filepaths
    ]

    return result


if __name__ == '__main__':
    parser = ArgumentParser(description='Associate an ensemble to datafiles')
    parser.add_argument(
        '-d', '--dsn', required=True,
        help='DSN for metadata database'
    )
    parser.add_argument(
        '-n', '--ensemble-name', dest='ensemble_name', required=True,
        help="Name of ensemble"
    )
    parser.add_argument(
        '-v', '--ensemble-ver', dest='ensemble_ver', type=float, required=True,
        help='Version of ensemble'
    )
    parser.add_argument(
        '-V', '--variables', dest='var_names',
        help='Comma-separated list of names of variables to associate '
             '(unspecified: all variables in file)'
    )
    parser.add_argument(
        'filepaths', nargs='+',
        help='Files to process'
    )
    args = parser.parse_args()

    associate_ensemble_to_files(
        args.dsn, args.ensemble_name, args.ensemble_ver,
        args.filepaths, args.var_names.split(','))
