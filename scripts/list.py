"""
List contents of a modelmeta database (by default db3/pcic_meta).

Output types are:

Either:

- filepaths: (default, no option) list filepaths from matching records
  - with or without names of associated ensembles
- dir_paths: list unique directory paths (to depth
  specified by value of option)

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


arg_names = '''
    print_queries
    count
    ensemble
    dir_path
    list_ensembles
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


def list_contents(
        session,
        print_queries=False,
        count=False,
        dir_path=False,
        list_ensembles=False,
        ensemble=None,
        multi_variable=None,
        multi_year_mean=None,
        mym_concatenated=None,
):
    # Select DataFiles matching selection criteria

    df_query = session.query(DataFile.id)

    if ensemble:
        df_query = (
            df_query.join(DataFile.data_file_variables)
                .join(DataFileVariable.ensembles)
                .filter(Ensemble.name == ensemble)
        )

    if multi_variable is not None:
        df_query = (
            df_query.join(DataFile.data_file_variables)
                .group_by(DataFile.id)
        )
        if multi_variable:
            df_query = df_query.having(func.count(DataFileVariable.id) > 1)
        else:
            df_query = df_query.having(func.count(DataFileVariable.id) == 1)

    if multi_year_mean is not None:
        df_query = (
            df_query.join(TimeSet)
                .filter(TimeSet.multi_year_mean == multi_year_mean)
        )

    if mym_concatenated is not None:
        df_query = (
            df_query.join(TimeSet)
                .filter(TimeSet.multi_year_mean == True)
        )
        if mym_concatenated:
            df_query = df_query.filter(TimeSet.num_times.in_((5, 13, 16, 17)))
        else:
            df_query = df_query.filter(TimeSet.num_times.in_((1, 4, 12)))

    if print_queries:
        print_query('Data file query', df_query)

    # Select information to be displayed

    if dir_path:
        info_query = (
            session.query(
                func.regexp_replace(
                    DataFile.filename,
                    r'^((/.[^/]+){{1,{}}}/).+$'.format(dir_path),
                    r'\1'
                ).label('dir_path'),
                func.count().label('number')
            )
                .filter(DataFile.id.in_(df_query))
                .group_by('dir_path')
                .order_by('dir_path')
        )
    else:
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

    # Display information

    if count and not list_ensembles:
        print(info_query.count())
    else:
        results = info_query.all()
        if dir_path:
            template = '{row.dir_path} ({row.number})'
        elif list_ensembles:
            if count:
                template ='{row.ensemble_name} ({row.number})'
            else:
                template = '{row.filename}\t{row.ensemble_names}'
        else:
            template = '{row.filename}'
        for row in results:
            print(template.format(row=row))


def main(args):
    engine = create_engine(args.dsn)
    session = sessionmaker(bind=engine)()
    list_contents(session, **{key: getattr(args, key) for key in arg_names})


if __name__ == '__main__':
    parser = ArgumentParser()
    parser.add_argument(
        '-d', '--dsn',
        default='postgresql://httpd_meta@db3.pcic.uvic.ca/pcic_meta',
        help="Source database DSN from which to read"
    )
    parser.add_argument(
        '-q', '--print-queries', action='store_true',
        help='Print SQL of queries generated'
    )
    # Selection criteria
    parser.add_argument(
        '-e', '--ensemble',
        help='Filter on association to ensemble'
    )
    parser.add_argument(
        '--mv', '--multi-variable', dest='multi_variable', type=strtobool,
        help='Filter on whether file contains more than 1 variable'
    )
    parser.add_argument(
        '--mym', '--multi-year-mean', dest='multi_year_mean', type=strtobool,
        help='Filter on whether file contains multi-year means'
    )
    parser.add_argument(
        '--mym-c', '--mym-concatenated', dest='mym_concatenated', type=strtobool,
        help='Filter on whether file contains multi-year means with concatenated time axes'
    )
    # Display type
    parser.add_argument(
        '-c', '--count', action='store_true',
        help='Display count only of records'
    )
    parser.add_argument(
        '--dirp', '--dir-path', dest='dir_path',
        help='Display (unique) directory paths of files, not full filepaths'
    )
    parser.add_argument(
        '-E', '--list-ensembles', dest='list_ensembles', action='store_true',
        help='Display associated ensembles for each file'
    )
    args = parser.parse_args()
    main(args)

