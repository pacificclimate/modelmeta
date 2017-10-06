import csv

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from modelmeta import DataFile


csv_fieldnames = '''
    data_file_id
    filepath
    unique_id
    index_time
    run
    model
    emission
    variable_names
    start_date
    end_date
    multi_year_mean
    time_resolution
    num_times
'''.split()


def csv_data_file(data_file):
    return {
        'data_file_id': data_file.id,
        'filepath': data_file.filename,
        'unique_id': data_file.unique_id,
        'index_time': data_file.index_time,
        'run': data_file.run.name,
        'model': data_file.run.model.short_name,
        'emission': data_file.run.emission.short_name,
        'variable_names':
            ', '.join(dfv.netcdf_variable_name
                      for dfv in data_file.data_file_variables),
        'start_date': data_file.timeset.start_date,
        'end_date': data_file.timeset.end_date,
        'multi_year_mean': data_file.timeset.multi_year_mean,
        'time_resolution': data_file.timeset.time_resolution,
        'num_times': data_file.timeset.num_times,
    }


def csv_contents(session):
    query = session.query(DataFile)
    data_files = query.all()

    with open('modelmeta.csv', 'w') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=csv_fieldnames)
        writer.writeheader()
        for data_file in data_files:
            writer.writerow(csv_data_file(data_file)) 


def main(dsn):
    engine = create_engine(dsn)
    session = sessionmaker(bind=engine)()
    csv_contents(session)

