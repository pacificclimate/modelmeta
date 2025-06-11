import pytest

from modelmeta import (
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
    SpatialRefSys,
)

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
    """
    Sanity check to ensure that all previous test results were rolled back.
    """
    for C in all_classes:
        assert sesh.query(C).count() == 0, C.__name__


def test_dfv_gridded(
    test_session_with_empty_db,
    dfv_gridded_1,
    data_file_1,
    variable_alias_1,
    level_set_1,
    grid_1,
):
    sesh = test_session_with_empty_db
    check_db_is_empty(sesh)
    sesh.add(dfv_gridded_1)
    sesh.flush()
    print()
    print(dfv_gridded_1)
    assert dfv_gridded_1.geometry_type == "gridded"
    assert data_file_1.data_file_variables == [dfv_gridded_1]
    assert variable_alias_1.data_file_variables == [dfv_gridded_1]
    assert level_set_1.data_file_variables == [dfv_gridded_1]
    assert grid_1.data_file_variables == [dfv_gridded_1]


def test_dfv_dsg_time_series(
    test_session_with_empty_db, dfv_dsg_time_series_1, data_file_1, variable_alias_1
):
    sesh = test_session_with_empty_db
    check_db_is_empty(sesh)
    sesh.add(dfv_dsg_time_series_1)
    sesh.flush()
    print()
    print(dfv_dsg_time_series_1)
    assert dfv_dsg_time_series_1.geometry_type == "dsg_time_series"
    assert data_file_1.data_file_variables == [dfv_dsg_time_series_1]
    assert variable_alias_1.data_file_variables == [dfv_dsg_time_series_1]


def associate_dfvs_and_stations(sesh, dfvs, stations):
    """ "
    Helper. Associate each timeSeries geometry variable with each station.
    """
    sesh.add_all(dfvs)
    sesh.add_all(stations)
    sesh.flush()

    xs = [
        DataFileVariableDSGTimeSeriesXStation(
            data_file_variable_dsg_ts=dfv_dsg_time_series,
            station=station,
        )
        for dfv_dsg_time_series in dfvs
        for station in stations
    ]
    sesh.add_all(xs)
    sesh.flush()
    return xs


@pytest.mark.parametrize("del_dfv", [False, True])
@pytest.mark.parametrize("del_station", [False, True])
def test_dfv_dsg_ts_x_station_delete(
    test_session_with_empty_db,
    dfv_dsg_time_series_1,
    dfv_dsg_time_series_2,
    station_1,
    station_2,
    del_dfv,
    del_station,
):
    """
    Test associations and various combinations of cascading deletes on cross
    table, including none.
    """
    sesh = test_session_with_empty_db
    check_db_is_empty(sesh)

    dfvs = {dfv_dsg_time_series_1, dfv_dsg_time_series_2}
    stations = {station_1, station_2}
    associate_dfvs_and_stations(sesh, dfvs, stations)

    if del_dfv:
        sesh.delete(dfv_dsg_time_series_1)
        sesh.flush()
        dfvs = dfvs - {dfv_dsg_time_series_1}
    if del_station:
        sesh.delete(station_1)
        sesh.flush()
        stations = stations - {station_1}

    assert sesh.query(DataFileVariableDSGTimeSeriesXStation).count() == len(dfvs) * len(
        stations
    )
    for dfv_dsg_time_series in dfvs:
        assert set(dfv_dsg_time_series.stations) == stations
    for station in stations:
        assert set(station.data_file_variables) == dfvs
