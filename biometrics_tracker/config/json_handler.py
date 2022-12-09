from decimal import Decimal
import json
import pathlib
from typing import Any

import biometrics_tracker.config.createconfig as config
import biometrics_tracker.config.logging_config as logging_config


def decode_bool(bool_str: str) -> bool:
    return bool_str in ['true', 'True']


class ConfigJSONEncoder(json.JSONEncoder):
    """
    Implements a custom json.JSONEncoder to encode instances of ConfigInfo, ImportSpecs and ExportSpecs

    """
    def default(self, obj: Any):
        """
        This method is invoked to handle data types that can't be handled by the standard JSONEncoder

        :param obj:
        :return: Any
        """
        match obj.__class__.__name__:
            case config.ConfigInfo.__name__:
                json_str = json.JSONEncoder().encode({'config_file_path': obj.config_file_path.__str__(),
                                                      'db_dir_path': obj.db_dir_path.__str__(),
                                                      'menu_font_size': obj.menu_font_size,
                                                      'default_font_size': obj.default_font_size,
                                                      'text_font_size': obj.text_font_size,
                                                      'logging_config': obj.logging_config,
                                                      'help_url': obj.help_url,
                                                      'plugin_dir_path': obj.plugin_dir_path.__str__(),
                                                      'class': obj.__class__.__name__})
                return json_str
            #  no match, revert to the standard encoder
            case _:
                return json.JSONEncoder.default(self, obj)


def config_object_hook(obj_dict: dict):
    """
    A custom object hook function to handle the decoding of JSON representations of ConfigInfo, ImportSpecs and
    ExportSpecs

    :param obj_dict: a dict containing JSON names and values
    :type obj_dict: dict[str, str]
    :return: an instance of the appropriate class (e.g. ConfigInfo, ImportSpecs, etc)
    :rtype: Union[ConfigInfo, ImportSpecs, ExportSpecs, PluginMenu, PluginMenuItem]


"""
    try:
        if 'class' in obj_dict:
            match obj_dict['class']:
                case config.ConfigInfo.__name__:
                    return config.ConfigInfo(config_file_path=pathlib.Path(obj_dict['config_file_path']),
                                             db_dir_path=pathlib.Path(obj_dict['db_dir_path']),
                                             menu_font_size=int(obj_dict['menu_font_size']),
                                             default_font_size=int(obj_dict['default_font_size']),
                                             text_font_size=int(obj_dict['text_font_size']),
                                             log_config=logging_config.LoggingConfig(obj_dict['logging_config']),
                                             help_url=obj_dict['help_url'],
                                             plugin_dir_path=pathlib.Path(obj_dict['plugin_dir_path']))
                case _:
                    return obj_dict
        else:
            return obj_dict
    except TypeError:
        return obj_dict
