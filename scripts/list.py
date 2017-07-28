"""
List contents of a modelmeta database (by default db3/pcic_meta).

Output types are:

Either:

- filepaths: list filepaths from matching records
  - with or without names of associated ensembles
- dirpaths: list unique directory paths (to depth specified)

Either:

- results: (default, no option) list results
- count: print count only of results that would be listed

Outputs can be filtered by one or more of the following criteria:

- multi-variable (t/f/none)
- multi-year mean (t/f/none)
- multi-year mean, with concatenated time axes (t/f/none)
"""


from argparse import ArgumentParser

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy import func

import sqlparse

from modelmeta import DataFile, DataFileVariable, Ensemble, TimeSet


# argument parser helpers

def strtobool(string):
    return string.lower() in {'true', 't', 'yes', '1'}


main_arg_names = '''
    print_queries
    count
    ensemble
    multi_variable
    multi_year_mean
    mym_concatenated
'''.split()


def print_query(title, query):
    print('--', title)
    compiled_query = str(
        query.statement.compile(compile_kwargs={'literal_binds': True})
    )
    formatted_query = sqlparse.format(
        compiled_query, reindent=True, keyword_case='upper')
    print(formatted_query)
    print()


def list_information(query, template, count=False):
    if count:
        print(query.count())
    else:
        results = query.all()
        for row in results:
            print(template.format(row=row))


def data_file_query(
        session,
        print_queries=False,
        ensemble=None,
        multi_variable=None,
        multi_year_mean=None,
        mym_concatenated=None,
):
    """
    Select DataFiles matching selection criteria

    :param session:
    :param print_queries:
    :param ensemble:
    :param multi_variable:
    :param multi_year_mean:
    :param mym_concatenated:
    :return:
    """
    query = session.query(DataFile.id)

    if ensemble:
        query = (
            query.join(DataFile.data_file_variables)
                .join(DataFileVariable.ensembles)
                .filter(Ensemble.name == ensemble)
        )

    if multi_variable is not None:
        query = (
            query.join(DataFile.data_file_variables)
                .group_by(DataFile.id)
        )
        if multi_variable:
            query = query.having(func.count(DataFileVariable.id) > 1)
        else:
            query = query.having(func.count(DataFileVariable.id) == 1)

    if multi_year_mean is not None:
        query = (
            query.join(TimeSet)
                .filter(TimeSet.multi_year_mean == multi_year_mean)
        )

    if mym_concatenated is not None:
        query = (
            query.join(TimeSet)
                .filter(TimeSet.multi_year_mean == True)
        )
        if mym_concatenated:
            query = query.filter(TimeSet.num_times.in_((5, 13, 16, 17)))
        else:
            query = query.filter(TimeSet.num_times.in_((1, 4, 12)))

    if print_queries:
        print_query('Data file query', query)

    return query


def _list_filepaths(
        session,
        print_queries=False,
        count=False,
        ensemble=None,
        multi_variable=None,
        multi_year_mean=None,
        mym_concatenated=None,
        list_ensembles = None,
):
    df_query = data_file_query(
        session,
        print_queries=print_queries,
        ensemble=ensemble,
        multi_variable=multi_variable,
        multi_year_mean=multi_year_mean,
        mym_concatenated=mym_concatenated,
    )

    if list_ensembles:
        if count:
            info_query = (
                session.query(
                    Ensemble.name.label('ensemble_name'),
                    func.count(DataFile.id).label('number')
                )
                    .filter(DataFile.id.in_(df_query))
                    .join(DataFile.data_file_variables)
                    .join(DataFileVariable.ensembles)
                    .group_by(Ensemble.name)
            )
        else:
            info_query = (
                session.query(
                    DataFile.filename,
                    func.string_agg(Ensemble.name, ',').label('ensemble_names')
                )
                    .filter(DataFile.id.in_(df_query))
                    .join(DataFile.data_file_variables)
                    .join(DataFileVariable.ensembles)
                    .group_by(DataFile.id)
            )
    else:
        info_query = df_query.add_columns(DataFile.filename)

    if print_queries:
        print_query('Info query', info_query)

    if list_ensembles:
        template = '{row.filename}\t{row.ensemble_names}'
    else:
        template = '{row.filename}'

    list_information(info_query, template, count=count)


def _list_dirpaths(
        session,
        print_queries=False,
        count=False,
        ensemble=None,
        multi_variable=None,
        multi_year_mean=None,
        mym_concatenated=None,
        depth=999,
):
    df_query = data_file_query(
        session,
        print_queries=print_queries,
        ensemble=ensemble,
        multi_variable=multi_variable,
        multi_year_mean=multi_year_mean,
        mym_concatenated=mym_concatenated,
    )

    info_query = (
        session.query(
            func.regexp_replace(
                DataFile.filename,
                r'^((/.[^/]+){{1,{}}}/).+$'.format(depth),
                r'\1'
            ).label('dir_path'),
            func.count().label('number')
        )
            .filter(DataFile.id.in_(df_query))
            .group_by('dir_path')
            .order_by('dir_path')
    )

    if print_queries:
        print_query('Info query', info_query)

    template = '{row.dir_path} ({row.number})'

    list_information(info_query, template, count=count)


def list_filepaths(args):
    arg_names = main_arg_names + '''
        list_ensembles
    '''.split()
    engine = create_engine(args.dsn)
    session = sessionmaker(bind=engine)()
    _list_filepaths(session, **{key: getattr(args, key) for key in arg_names})


def list_dirpaths(args):
    arg_names = main_arg_names + '''
        depth
    '''.split()
    engine = create_engine(args.dsn)
    session = sessionmaker(bind=engine)()
    _list_dirpaths(session, **{key: getattr(args, key) for key in arg_names})


if __name__ == '__main__':

    main_parser = ArgumentParser()
    main_parser.add_argument(
        '-d', '--dsn',
        default='postgresql://httpd_meta@db3.pcic.uvic.ca/pcic_meta',
        help="Source database DSN from which to read"
    )
    main_parser.add_argument(
        '-q', '--print-queries', action='store_true',
        help='Print SQL of queries generated'
    )
    # Selection criteria
    main_parser.add_argument(
        '-e', '--ensemble',
        help='Filter on association to ensemble'
    )
    main_parser.add_argument(
        '--mv', '--multi-variable', dest='multi_variable', type=strtobool,
        help='Filter on whether file contains more than 1 variable'
    )
    main_parser.add_argument(
        '--mym', '--multi-year-mean', dest='multi_year_mean', type=strtobool,
        help='Filter on whether file contains multi-year means'
    )
    main_parser.add_argument(
        '--mym-c', '--mym-concatenated', dest='mym_concatenated', type=strtobool,
        help='Filter on whether file contains multi-year means with concatenated time axes'
    )
    # Display type
    main_parser.add_argument(
        '-c', '--count', action='store_true',
        help='Display count only of records'
    )

    subparsers = main_parser.add_subparsers()

    filepaths_parser = subparsers.add_parser(
        'filepaths', aliases=['f', 'fp'],
        help='Display full filepaths of files'
    )
    filepaths_parser.add_argument(
        '-E', '--list-ensembles', dest='list_ensembles', action='store_true',
        help='Display associated ensembles for each file'
    )
    filepaths_parser.set_defaults(action=list_filepaths)

    dirpaths_parser = subparsers.add_parser(
        'dirpaths', aliases=['d', 'dp'],
        help='Display (unique) directory paths of files'
    )
    dirpaths_parser.add_argument(
        'depth', type=int, nargs='?', default=99,
        help='Cutoff depth of directory paths to display'
    )
    dirpaths_parser.set_defaults(action=list_dirpaths)

    args = main_parser.parse_args()
    args.action(args)
