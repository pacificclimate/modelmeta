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
    SpatialRefSys, \
    StreamflowOrder, \
    StreamflowResult

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


def add_and_flush(sesh, to_add):
    if isinstance(to_add, list):
        sesh.add_all(to_add)
    else:
        sesh.add(to_add)
    sesh.flush()


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
        data_file_2, variable_alias_1):
    sesh = test_session_with_empty_db
    check_db_is_empty(sesh)
    sesh.add(dfv_dsg_time_series_1)
    sesh.flush()
    print()
    print(dfv_dsg_time_series_1)
    assert dfv_dsg_time_series_1.geometry_type == 'dsg_time_series'
    assert data_file_2.data_file_variables == [dfv_dsg_time_series_1]
    assert variable_alias_1.data_file_variables == [dfv_dsg_time_series_1]


def add_and_associate_dfvs_and_stations(sesh, dfvs, stations):
    """"
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


@pytest.mark.parametrize('del_dfv', [False, True])
@pytest.mark.parametrize('del_station', [False, True])
def test_dfv_dsg_ts_x_station_delete(
        test_session_with_empty_db,
        dfv_dsg_time_series_1, dfv_dsg_time_series_2, station_1, station_2,
        del_dfv, del_station,
):
    '''
    Test associations and various combinations of cascading deletes on cross
    table, including none.
    '''
    sesh = test_session_with_empty_db
    check_db_is_empty(sesh)

    dfvs = {dfv_dsg_time_series_1, dfv_dsg_time_series_2}
    stations = {station_1, station_2}
    add_and_associate_dfvs_and_stations(sesh, dfvs, stations)

    if del_dfv:
        sesh.delete(dfv_dsg_time_series_1)
        sesh.flush()
        dfvs = dfvs - {dfv_dsg_time_series_1}
    if del_station:
        sesh.delete(station_1)
        sesh.flush()
        stations = stations - {station_1}

    assert sesh.query(DataFileVariableDSGTimeSeriesXStation).count() == \
        len(dfvs) * len(stations)
    for dfv_dsg_time_series in dfvs:
        assert set(dfv_dsg_time_series.stations) == stations
    for station in stations:
        assert set(station.data_file_variables) == dfvs


def streamflow_workflow_new(
        sesh, hydromodel_output, streamflow, station_1, station_2,
        remove=True):
    """
    A happy-path order fulfillment workflow where the result that fulfils
    the order must be computed (does not already exist). Steps are:

    #. (setup) Add hydromodel output DF etc. for Order parametrization
    #. Add Result (counterintuitively, this comes first) in 'queued' state.
       This fulfils the Order; it represents a computation that is at this
       moment queued.
    #. Add Order in 'accepted' state that links to Result and hydromodel
       output DF.
    #. Modify Result state to 'processing'.
    #. Add DF, DFV, Stns to represent streamflow result file.
    #. Modify Result state to 'ready', point at appropriate DF and Station;
    #. Modify Order state to 'fulfilled'.
    #. Modify Result state to 'removed'.

    """

    # (setup) Add hydromodel output DF etc. for Order parametrization
    add_and_flush(sesh, hydromodel_output)

    # Add Result (counterintuitively, this comes first)  in 'queued' state.
    # This fulfils the Order; it represents a computation that is at this
    # moment queued.
    result = StreamflowResult(
        status='queued'
    )
    add_and_flush(sesh, result)

    # Add Order in 'accepted' state that links to Result and hydromodel
    # output DF.
    order = StreamflowOrder(
        hydromodel_output=hydromodel_output,
        longitude=-123.5,
        latitude=50.5,
        notification_method='email',
        notification_address='abc@example.ca',
        status='accepted',
        result=result
    )
    add_and_flush(sesh, order)

    # Modify Result state to 'processing'.
    result.status = 'processing'
    sesh.flush()

    # Add DF, DFV, Stns to represent streamflow result file.
    dfvs = {streamflow.data_file_variables[0]}
    stations = {station_1, station_2}
    add_and_associate_dfvs_and_stations(sesh, dfvs, stations)

    # Modify Result state to 'ready', point at appropriate DF and Station.
    result.status = 'ready'
    result.data_file = streamflow
    result.station = station_1

    # Modify Order state to 'fulfilled'.
    order.status = 'fulfilled'
    sesh.flush()

    if remove:
        # Modify Result state to 'removed'; remove relations to DF and Stn
        result.status = 'removed'
        result.data_file = None
        result.station = None
        sesh.flush()


def test_streamflow_workflow_new(
        test_session_with_empty_db,
        dfv_gridded_1,
        dfv_dsg_time_series_1, station_1, station_2,
):
    """
    Test a happy-path order fulfillment workflow.
    """
    sesh = test_session_with_empty_db
    check_db_is_empty(sesh)

    hydromodel_output = dfv_gridded_1.file
    streamflow = dfv_dsg_time_series_1.file

    # Execute the new-result workflow: does it explode?
    streamflow_workflow_new(
        sesh, hydromodel_output, streamflow, station_1, station_2)

    # Do a few nominal query-based tests
    q = sesh.query(StreamflowOrder).filter_by(status='fulfilled')
    assert q.count() == 1
    order = q.one()
    assert order.hydromodel_output_id == hydromodel_output.id
    assert order.result.status == 'removed'
    assert order.result.data_file is None


def streamflow_workflow_existing(sesh, hydromodel_output):
    """
    A happy-path order fulfillment workflow where the result that fulfils
    the order already exists. Steps:

    #. Find existing Order that matches parameters.
    #. Add a new, 'fulfilled' Order that points to the existing Result.
    """

    # Params for matching query
    longitude = -123.5
    latitude = 50.5

    existing_order = (
        sesh.query(StreamflowOrder).join(StreamflowResult)
        .filter(StreamflowOrder.hydromodel_output_id == hydromodel_output.id)
        .filter(StreamflowOrder.longitude == longitude)
        .filter(StreamflowOrder.latitude == latitude)
        .filter(StreamflowOrder.status =='fulfilled')
        .filter(StreamflowResult.status == 'ready')
    ).first()

    new_order = StreamflowOrder(
        hydromodel_output=hydromodel_output,
        longitude=longitude,
        latitude=latitude,
        result=existing_order.result,
        notification_method='email',
        notification_address='foo@xyz.com',
        status='fulfilled'
    )
    add_and_flush(sesh, new_order)


def test_streamflow_workflow_existing(
        test_session_with_empty_db,
        dfv_gridded_1,
        dfv_dsg_time_series_1, station_1, station_2,
):
    """
    Test order fulfillment workflow where there is a pre-existing result
    that fulfils the order.
    """
    sesh = test_session_with_empty_db
    check_db_is_empty(sesh)

    hydromodel_output = dfv_gridded_1.file
    streamflow = dfv_dsg_time_series_1.file

    # Add "pre-existing" Order, Result, and supporting records.
    # This is just the new-result workflow, which we reuse here.
    streamflow_workflow_new(
        sesh, hydromodel_output, streamflow, station_1, station_2, remove=False)

    # Execute the existing-result workflow
    streamflow_workflow_existing(sesh, hydromodel_output)

    # Test that everything is as it should be
    q = sesh.query(StreamflowOrder).filter_by(status='fulfilled')
    assert q.count() == 2
    for order in q.all():
        assert order.hydromodel_output_id == hydromodel_output.id
        assert order.result.status == 'ready'
        assert order.result.data_file_id == streamflow.id
