import logging
import traceback

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from modelmeta import DataFile, DataFileVariable, Ensemble, EnsembleDataFileVariables


formatter = logging.Formatter(
    "%(asctime)s %(levelname)s: %(message)s", "%Y-%m-%d %H:%M:%S"
)
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
    q = (
        sesh.query(Ensemble)
        .filter(Ensemble.name == name)
        .filter(Ensemble.version == version)
    )
    return q.first()


def associate_ensemble_to_data_file_variable(session, ensemble, data_file_variable):
    """Associate an ``Ensemble`` to a ``DataFileVariable``.

    :param session: database session for access to modelmeta database
    :param ensemble: (Ensemble) ensemble to associate
    :param data_file_variable: (DataFileVariable) dfv to associate
    :return: EnsembleDataFileVariables (association) record
    """
    ensemble_dfv = (
        session.query(EnsembleDataFileVariables)
        .filter(EnsembleDataFileVariables.ensemble_id == ensemble.id)
        .filter(
            EnsembleDataFileVariables.data_file_variable_id == data_file_variable.id
        )
        .first()
    )

    if ensemble_dfv:
        logger.info(
            "Assocation for variable id {} to ensemble already exists".format(
                data_file_variable.id
            )
        )
    else:
        logger.info(
            "Associating variable id {} to ensemble".format(data_file_variable.id)
        )
        ensemble_dfv = EnsembleDataFileVariables(
            ensemble_id=ensemble.id, data_file_variable_id=data_file_variable.id
        )
        session.add(ensemble_dfv)

    return ensemble_dfv


def associate_ensemble_to_data_file(session, ensemble, data_file, var_names):
    """Associate an ``Ensemble`` to ``DataFileVariable``s of a ``DataFile``.

    :param session: database session for access to modelmeta database
    :param ensemble: (Ensemble) ensemble to associate
    :param data_file: (DataFile) data file to associate
    :param var_names: (list) names of variables to associate
    :return: list of ``DataFileVariable``s associated
    """
    logger.info("Associating DataFile: {}".format(data_file.filename))

    associated_dfvs = []

    for data_file_variable in data_file.data_file_variables:
        if not var_names or data_file_variable.netcdf_variable_name in var_names:
            associate_ensemble_to_data_file_variable(
                session, ensemble, data_file_variable
            )
            associated_dfvs.append(data_file_variable)

    return associated_dfvs


def associate_ensemble_to_filepath(
    session, ensemble_name, ensemble_ver, regex_filepath, filepath, var_names
):
    """Associate an ensemble (specified by name and version) to
    data file variables of data file(s) matching a given filepath pattern.

    :param session: database session access to modelmeta database
    :param ensemble_name: (str) name of ensemble
    :param ensemble_ver: (float) version of ensemble
    :param regex_filepath: (bool) if True, interpret filepath as regex
    :param filepath: filepath of file, or regex for such
    :param var_names: (list) names of variables to associate
    :return: (list) tuple(``DataFile``, list of ``DataFileVariable``s
        associated); one for each matching DataFile
    """
    if regex_filepath:
        logger.info("Processing filepath regex: {}".format(filepath))
    else:
        logger.info("Processing filepath: {}".format(filepath))

    # Find the matching ``Ensemble``
    ensemble = find_ensemble(session, ensemble_name, ensemble_ver)
    if not ensemble:
        raise ValueError(
            "No existing ensemble matches name = '{}' and version = '{}'".format(
                ensemble_name, ensemble_ver
            )
        )

    # Find all matching ``DataFile``s
    df_query = session.query(DataFile)
    if regex_filepath:
        df_query = df_query.filter(DataFile.filename.op("~")(filepath))
    else:
        df_query = df_query.filter(DataFile.filename == filepath)
    data_files = df_query.all()

    if not data_files:
        logger.info("No matching DataFile records")

    # Associate matching ensemble to matching data files
    return [
        (
            data_file,
            associate_ensemble_to_data_file(session, ensemble, data_file, var_names),
        )
        for data_file in data_files
    ]


def associate_ensemble_to_filepaths(
    Session, ensemble_name, ensemble_ver, regex_filepaths, filepaths, var_names
):
    """Associate a list of NetCDF files in modelmeta database to a specified
    ensemble.

    :param dsn: connection info for the modelmeta database to update
    :param ensemble_name: (str) name of ensemble
    :param ensemble_ver: (float) version of ensemble
    :param regex_filepaths: (bool) if True, interpret filepaths as regexes
    :param filepaths: list of files to index
    :param var_names: list of names of variables to associate
    :return: (list) tuple(id of ``DataFile``,
                         list of id of ``DataFileVariable``s)
        associated; one for each matching DataFile
    """
    associated_ids = []

    for filepath in filepaths:
        session = Session()
        try:
            associated_items = associate_ensemble_to_filepath(
                session,
                ensemble_name,
                ensemble_ver,
                regex_filepaths,
                filepath,
                var_names,
            )
            associated_ids.extend(
                [
                    (data_file.id, [dfv.id for dfv in data_file_variables])
                    for data_file, data_file_variables in associated_items
                ]
            )
            session.commit()
        except:
            logger.error(traceback.format_exc())
            session.rollback()
        finally:
            session.close()

    return associated_ids


def main(dsn, ensemble_name, ensemble_ver, regex_filepaths, filepaths, var_names):
    """Associate a list of NetCDF files in modelmeta database to a specified
    ensemble.

    :param dsn: connection info for the modelmeta database to update
    :param ensemble_name: (str) name of ensemble
    :param ensemble_ver: (float) version of ensemble
    :param regex_filepaths: (bool) if True, interpret filepaths as regexes
    :param filepaths: list of files to index
    :param var_names: list of names of variables to associate
    :return: list of list of ids of ``DataFileVariable``s associated;
        one sublist for each file processed
    """
    engine = create_engine(dsn)
    Session = sessionmaker(bind=engine)

    return associate_ensemble_to_filepaths(
        Session, ensemble_name, ensemble_ver, regex_filepaths, filepaths, var_names
    )
