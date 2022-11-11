import json
import pytest
from typing import Union

from datapoints_fixtures import blood_pressure_data_fix, blood_glucose_data_fix, pulse_data_fix, body_weight_data_fix, \
    body_temp_data_fix


import biometrics_tracker.model.datapoints as dp
import biometrics_tracker.model.json_handler as jh


metric_union = Union[dp.BloodGlucose, dp.BloodPressure, dp.Pulse, dp.BodyTemperature, dp.BodyWeight]


def json_test(data: metric_union, dp_type: dp.DataPointType) -> None:
    data_json: str = json.dumps(data, cls=jh.BiometricsJSONEncoder)
    check_data: metric_union = json.loads(eval(data_json), object_hook=jh.biometrics_object_hook)
    assert data.__class__ == check_data.__class__, \
        f'DP Type: {dp_type.name} Object Class mismatch Expected: {data.__class__.__name__} Observed: ' \
        f'{check_data.__class__.__name__}'
    data_len = len(data.__dict__)
    check_data_len = len(check_data.__dict__)
    if data_len >= check_data_len:
        for name, value in data.__dict__.items():
            assert name in check_data.__dict__, f'DP Type: {dp_type.name} Property {name} not found'
            assert value == check_data.__dict__[name], f'DP Type: {dp_type.name} Property {name} Expected {value} '\
                                                       f'Observed {check_data.__dict__[name]}'
    else:
        for name, value in check_data.__dict__.items():
            for name, value in data.__dict__.items():
                assert name in data.__dict__, f'DP Type: {dp_type.name} Property {name} not found'
                assert value == data.__dict__[name], f'DP Type: {dp_type.name} Property {name} Expected {value} ' \
                                                     f'Observed {check_data.__dict__[name]}'
        assert False, f'Number of Properties Mismatch Expected: {data_len} Observed: {check_data_len}'


@pytest.mark.DataPoint
@pytest.mark.BloodGlucose
def test_blood_glucose_json(blood_glucose_data_fix):
    for datum in blood_glucose_data_fix:
        blood_glucose: dp.BloodGlucose = dp.BloodGlucose(value=datum.value, uom=datum.uom)
        json_test(blood_glucose, dp.DataPointType.BG)


@pytest.mark.DataPoint
@pytest.mark.BloodPressure
def test_blood_pressure_json(blood_pressure_data_fix):
    for datum in blood_pressure_data_fix:
        blood_pressure: dp.BloodPressure = dp.BloodPressure(systolic=datum.systolic, diastolic=datum.diastolic,
                                                            uom=datum.uom)
        json_test(blood_pressure, dp.DataPointType.BP)


@pytest.mark.DataPoint
@pytest.mark.Pulse
def test_pulse_json(pulse_data_fix):
    for datum in pulse_data_fix:
        pulse: dp.Pulse = dp.Pulse(value=datum.value, uom=datum.uom)
        json_test(pulse, dp.DataPointType.BP)


@pytest.mark.DataPoint
@pytest.mark.BodyTemperature
def test_body_temperature_json(body_temp_data_fix):
    for datum in body_temp_data_fix:
        body_temperature: dp.BodyTemperature = dp.BodyTemperature(value=datum.value, uom=datum.uom)
        json_test(body_temperature, dp.DataPointType.BP)


@pytest.mark.DataPoint
@pytest.mark.BodyWeight
def test_body_temperature_json(body_weight_data_fix):
    for datum in body_weight_data_fix:
        body_temperature: dp.BodyWeight = dp.BodyWeight(value=datum.value, uom=datum.uom)
        json_test(body_temperature, dp.DataPointType.BP)



