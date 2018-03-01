import pytest

from modelmeta import \
    ClimatologicalTime, \
    DataFile, \
    DataFileVariable, \
    DataFileVariableDSGTimeSeries, \
    DataFileVariableDSGTimeSeriesXStation, \
    DataFileVariableGridded, \
    DataFileVariable, \
    DataFileVariablesQcFlag, \
    Emission, \
    Ensemble, \
    EnsembleDataFileVariables, \
    Grid, \
    Level, \
    LevelSet, \
    Model, \
    QcFlag, \
    Run, \
    Station, \
    Time, \
    TimeSet, \
    Variable, \
    VariableAlias, \
    YCellBound, \
    SpatialRefSys

all_classes = [
    ClimatologicalTime, 
    DataFile, 
    DataFileVariable, 
    DataFileVariableDSGTimeSeries, 
    DataFileVariableDSGTimeSeriesXStation, 
    DataFileVariableGridded, 
    DataFileVariable, 
    DataFileVariablesQcFlag, 
    Emission, 
    Ensemble, 
    EnsembleDataFileVariables, 
    Grid, 
    Level, 
    LevelSet, 
    Model, 
    QcFlag, 
    Run, 
    Station, 
    Time, 
    TimeSet, 
    Variable, 
    VariableAlias, 
    YCellBound, 
    # SpatialRefSys
]


def check_db_is_empty(sesh):
    for C in all_classes:
        assert sesh.query(C).count() == 0, C.__name__


def test_dfv_gridded(
        test_session_with_empty_db, dfv_gridded_1,
        data_file_1, variable_alias_1, level_set_1, grid_1):
    sesh = test_session_with_empty_db
    check_db_is_empty(sesh)
    sesh.add(dfv_gridded_1)
    sesh.flush()
    print()
    print(dfv_gridded_1)
    assert dfv_gridded_1.geometry_type == 'gridded'
    assert data_file_1.data_file_variables == [dfv_gridded_1]
    assert variable_alias_1.data_file_variables == [dfv_gridded_1]
    assert level_set_1.data_file_variables == [dfv_gridded_1]
    assert grid_1.data_file_variables == [dfv_gridded_1]


def test_dfv_dsg_time_series(
        test_session_with_empty_db, dfv_dsg_time_series_1,
        data_file_1, variable_alias_1):
    sesh = test_session_with_empty_db
    check_db_is_empty(sesh)
    sesh.add(dfv_dsg_time_series_1)
    sesh.flush()
    print()
    print(dfv_dsg_time_series_1)
    assert dfv_dsg_time_series_1.geometry_type == 'dsg_time_series'
    assert data_file_1.data_file_variables == [dfv_dsg_time_series_1]
    assert variable_alias_1.data_file_variables == [dfv_dsg_time_series_1]


def test_station(test_session_with_empty_db, station_1):
    sesh = test_session_with_empty_db
    check_db_is_empty(sesh)
    sesh.add(station_1)
    sesh.flush()
    print()
    print(station_1)


def test_dfv_dsg_ts_x_station(
        test_session_with_empty_db,
        dfv_dsg_time_series_1, dfv_dsg_time_series_2, station_1, station_2):
    '''
    Associate each of two timeSeries geometry variables with each of two
    stations
    '''
    sesh = test_session_with_empty_db
    check_db_is_empty(sesh)

    dfvs = {dfv_dsg_time_series_1, dfv_dsg_time_series_2}
    stations = {station_1, station_2}

    sesh.add_all(dfvs)
    sesh.add_all(stations)
    sesh.flush()

    xs = [
        DataFileVariableDSGTimeSeriesXStation(
            data_file_variable_dsg_ts_id=dfv_dsg_time_series.id,
            station_id=station.id,
        )
        for dfv_dsg_time_series in dfvs
        for station in stations
    ]
    sesh.add_all(xs)
    sesh.flush()

    for dfv_dsg_time_series in dfvs:
        assert set(dfv_dsg_time_series.stations) == stations
    for station in stations:
        assert set(station.data_file_variables) == dfvs

