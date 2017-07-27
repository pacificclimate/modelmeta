from argparse import ArgumentParser

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy import func

from modelmeta import DataFile, DataFileVariable, TimeSet


# argument parser helpers

def strtobool(string):
    return string.lower() in {'true', 't', 'yes', '1'}


arg_names = '''
    count
    dir_path
    multi_variable
    multi_year_mean
    mym_concatenated
'''.split()


def list_contents(
        session,
        count=False,
        dir_path=False,
        multi_variable=None,
        multi_year_mean=None,
        mym_concatenated=None,
):
    if dir_path:
        query = (
            session.query(
                func.regexp_replace(DataFile.filename, r'^((/.[^/]+){{1,{}}}/).+$'.format(dir_path), r'\1')
                    .label('dir_path')
            )
                .group_by('dir_path')
                .order_by('dir_path')
        )
    else:
        query = session.query(DataFile)

    if multi_variable is not None:
        query = (
            query.join(DataFile.data_file_variables)
                .group_by(DataFile)
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

    # print(query)

    if count:
        print(query.count())
    else:
        data_files = query.all()
        if dir_path:
            for data_file in data_files:
                print(data_file.dir_path)
        else:
            for data_file in data_files:
                print(data_file.filename)


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

