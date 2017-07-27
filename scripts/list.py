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

WARNING: The combination of filepaths with associated ensembles and
multi-variable (t or f) fails, with 0 records selected. It's not a case that
will be used, so I haven't fixed it.
"""


from argparse import ArgumentParser

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy import func

from modelmeta import DataFile, DataFileVariable, Ensemble, TimeSet


# argument parser helpers

def strtobool(string):
    return string.lower() in {'true', 't', 'yes', '1'}


arg_names = '''
    count
    dir_path
    ensembles
    multi_variable
    multi_year_mean
    mym_concatenated
'''.split()


def list_contents(
        session,
        count=False,
        dir_path=False,
        ensembles=False,
        multi_variable=None,
        multi_year_mean=None,
        mym_concatenated=None,
):
    if dir_path:
        query = (
            session.query(
                func.regexp_replace(DataFile.filename, r'^((/.[^/]+){{1,{}}}/).+$'.format(dir_path), r'\1')
                    .label('dir_path'),
                func.count().label('number')
            )
                .group_by('dir_path')
                .order_by('dir_path')
        )
    else:
        if not ensembles:
            query = session.query(DataFile.id, DataFile.filename)
        else:
            query = (
                session.query(
                    DataFile.id,
                    DataFile.filename,
                    func.string_agg(Ensemble.name, ',').label('ensemble_names')
                )
                    .join(DataFile.data_file_variables)
                    .join(DataFileVariable.ensembles)
                    .group_by(DataFile.id)
            )

    if multi_variable is not None:
        if not ensembles:
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
            query.join(DataFile.timeset)
                .filter(TimeSet.multi_year_mean == multi_year_mean)
        )

    if mym_concatenated is not None:
        query = (
            query.join(DataFile.timeset)
                .filter(TimeSet.multi_year_mean == True)
        )
        if mym_concatenated:
            query = query.filter(TimeSet.num_times.in_((5, 13, 16, 17)))
        else:
            query = query.filter(TimeSet.num_times.in_((1, 4, 12)))

    print(query)

    if count:
        print(query.count())
    else:
        results = query.all()
        if dir_path:
            template = '{row.dir_path} ({row.number})'
        elif ensembles:
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
        '-c', '--count', action='store_true',
        help='Display count only of records'
    )
    parser.add_argument(
        '-e', '--ensembles', action='store_true',
        help='Display associated ensembles for each file'
    )
    parser.add_argument(
        '--dirp', '--dir-path', dest='dir_path',
        help='Display (unique) directory paths of files, not full filepaths'
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
    args = parser.parse_args()
    main(args)

