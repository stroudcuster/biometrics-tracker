from decimal import Decimal
import json
import pathlib
from typing import Any

import biometrics_tracker.model.datapoints as dp
import biometrics_tracker.model.uoms as uoms


class BiometricsJSONEncoder(json.JSONEncoder):
    """
    Implements a custom json.JSONEncoder to encode instances of model.BodyWeight, model.Pulse, etc.  The result
    will be stored in the data property of an instance of one the classes that extend model.DataPoint.  The purpose
    of this encoding is to allow the various types of readings to be  stored in a single SQL table, rather
    than requiring a table for each type of reading

    :param obj: the object to be converted to JSON format
    :type obj: Any

    """
    def default(self, obj: Any):
        # handle Enums form model.uoms
        try:
            if obj.name in uoms.uom_map:
                return json.JSONEncoder().encode({'type': obj.name, 'abbreviation': obj.abbreviation(),
                                                 'class': obj.__class__.__name__})
        except AttributeError:
            # handle other application classes
            match obj.__class__.__name__:
                case dp.BodyWeight.__name__:
                    json_str = json.JSONEncoder().encode({'value': f'{obj.value:.1f}',
                                                          'uom': 'xxx',
                                                          'class': obj.__class__.__name__})
                    json_str = json_str.replace('"xxx"', BiometricsJSONEncoder().default(obj.uom))
                    return json_str
                case dp.BloodGlucose.__name__:
                    json_str = json.JSONEncoder().encode({'value': obj.value,
                                                          'uom': 'xxx',
                                                          'class': obj.__class__.__name__})
                    json_str = json_str.replace('"xxx"', BiometricsJSONEncoder().default(obj.uom))
                    return json_str
                case dp.Pulse.__name__:
                    json_str = json.JSONEncoder().encode({'value': obj.value,
                                                          'uom': 'xxx',
                                                          'class': obj.__class__.__name__})
                    json_str = json_str.replace('"xxx"', BiometricsJSONEncoder().default(obj.uom))
                    return json_str
                case dp.BodyTemperature.__name__:
                    json_str = json.JSONEncoder().encode({'value': f'{obj.value:.1f}',
                                                          'uom': 'xxx',
                                                          'class': obj.__class__.__name__})
                    json_str = json_str.replace('"xxx"', BiometricsJSONEncoder().default(obj.uom))
                    return json_str
                case dp.BloodPressure.__name__:
                    json_str = json.JSONEncoder().encode({'systolic': obj.systolic,
                                                          'diastolic': obj.diastolic,
                                                          'uom':  'xxx',
                                                          'class': obj.__class__.__name__})
                    json_str = json_str.replace('"xxx"', BiometricsJSONEncoder().default(obj.uom))
                    return json_str
                #  no match, revert to the standard encoder
                case _:
                    return json.JSONEncoder.default(self, obj)


def biometrics_object_hook(obj_dict: dict):
    """
    A custom object hook function to handle the decoding of JSON representations of model.BodyWeight,
    model.Pulse, etc. instances

    :param obj_dict: a dict containing JSON names and values
    :type obj_dict: dict[str, str]
    :return: an instance of the appropriate class (e.g. biometrics_tracker.model.BodyWeight, model.Pulse)
    :rtype: biometrics_tracker.model.datapoints.dp_type_union


"""
    try:
        if 'class' in obj_dict:
            if obj_dict['class'] in ('Weight', 'Pressure', 'Temperature', 'BG', 'Rate'):
                return uoms.uom_map[obj_dict['type']]
            else:
                match obj_dict['class']:
                    case dp.BodyWeight.__name__:
                        return dp.BodyWeight(Decimal(obj_dict['value']),
                                             biometrics_object_hook(obj_dict['uom']))
                    case dp.BloodGlucose.__name__:
                        return dp.BloodGlucose(int(obj_dict['value']),
                                               biometrics_object_hook(obj_dict['uom']))
                    case dp.Pulse.__name__:
                        return dp.Pulse(int(obj_dict['value']),
                                        biometrics_object_hook(obj_dict['uom']))
                    case dp.BodyTemperature.__name__:
                        return dp.BodyTemperature(Decimal(obj_dict['value']),
                                                  biometrics_object_hook(obj_dict['uom']))
                    case dp.BloodPressure.__name__:
                        return dp.BloodPressure(int(obj_dict['systolic']), int(obj_dict['diastolic']),
                                                biometrics_object_hook(obj_dict['uom']))
                    case _:
                        return obj_dict
        else:
            return obj_dict
    except TypeError:
        return obj_dict
