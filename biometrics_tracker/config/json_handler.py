from decimal import Decimal
import json
import pathlib
from typing import Any

import biometrics_tracker.config.createconfig as config
import biometrics_tracker.config.logging_config as logging_config
import biometrics_tracker.config.plugin_config as plugin_config


def retrieve_plugins(plugin_path: pathlib.Path) -> list[plugin_config.Plugin]:
    """
    Deserialize the Plugin objects encoded in JSON file in the plugins folder

    :param plugin_path: a Path object pointing to the folder containing the Plugin JSON files
    :type: pathlib.Path
    :return: a list of de-serialized Plugin objects
    :rtype: list[plugin_config.PluginMenu]

    """
    plugins: list[plugin_config.Plugin] = []
    for plugin_json in plugin_path.glob('*.json'):
        with plugin_json.open(mode='r') as pj:
            json_str = pj.read()
            plugin: plugin_config.Plugin = json.loads(eval(json_str), object_hook=config_object_hook)
            plugins.append(plugin)
    return plugins


def save_plugins(plugins: list[plugin_config.Plugin], plugin_path: pathlib.Path):
    """
    Serialize a list of Plugin instances to JSON files.  The file names are based on the Plugin.name and author_name properties

    :param plugins: a list of Plugin objects
    :type plugins: list[biometrics_tracker.config.Plugin]
    :param plugin_path: a Path object pointing to the plugin folder
    :type plugin_path: pathlib.Path
    :return: None
    """
    for plugin in plugins:
        json_str = json.dumps(plugin, cls=ConfigJSONEncoder)
        with pathlib.Path(plugin_path,
                          f'{plugin.author_name.replace(" ", "_")}-{plugin.name.replace(" ","_")}.json').open(mode='w') as pj:
            pj.write(json_str)


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
            case plugin_config.PluginMenuItem.__name__:
                json_str = json.JSONEncoder().encode({'title': obj.title,
                                                      'entry_point': obj.entry_point,
                                                      'select_person': str(obj.select_person),
                                                      'select_date_range': str(obj.select_date_range),
                                                      'select_dp_type': str(obj.select_dp_type),
                                                      'class': plugin_config.PluginMenuItem.__name__})
                return json_str
            case plugin_config.PluginMenu.__name__:
                list_str = f'[{",".join([ConfigJSONEncoder().default(item) for item in obj.items])}]'
                json_str = json.JSONEncoder().encode({'title': obj.title,
                                                      'module': obj.module,
                                                      'items': list_str,
                                                      'class': obj.__class__.__name__})
                return json_str
            case plugin_config.Plugin.__name__:
                menu_list_str: str = f'[{",".join([ConfigJSONEncoder().default(menu) for menu in obj.menus])}]'
                json_str = json.JSONEncoder().encode({'name': obj.name,
                                                      'description': obj.description,
                                                      'author_name': obj.author_name,
                                                      'author_email': obj.author_email,
                                                      'menus': menu_list_str,
                                                      'class': obj.__class__.__name__})
                return json_str
            case config.ConfigInfo.__name__:
                json_str = json.JSONEncoder().encode({'config_file_path': obj.config_file_path.__str__(),
                                                      'db_dir_path': obj.db_dir_path.__str__(),
                                                      'menu_font_size': obj.menu_font_size,
                                                      'default_font_size': obj.default_font_size,
                                                      'text_font_size': obj.text_font_size,
                                                      'logging_config': obj.logging_config,
                                                      'help_url': obj.help_url,
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
                case plugin_config.PluginMenuItem.__name__:
                    return plugin_config.PluginMenuItem(title=obj_dict['title'],
                                                        entry_point=obj_dict['entry_point'],
                                                        select_person=decode_bool(obj_dict['select_person']),
                                                        select_date_range=decode_bool(obj_dict['select_date_range']),
                                                        select_dp_type=decode_bool(obj_dict['select_dp_type']))
                case plugin_config.PluginMenu.__name__:
                    items: list[plugin_config.PluginMenuItem] = []
                    for item in eval(obj_dict['items']):
                        items.append(config_object_hook(item))
                        """
                        items.append(plugin_config.PluginMenuItem(title=item['title'],
                                                                  entry_point=item['entry_point'],
                                                                  select_person=decode_bool(item['select_person']),
                                                                  select_date_range=decode_bool(item['select_date_range']),
                                                                  select_dp_type=decode_bool(item['select_dp_type'])))
                        """
                    return plugin_config.PluginMenu(title=obj_dict['title'],
                                                    module=obj_dict['module'],
                                                    items=items)
                case plugin_config.Plugin.__name__:
                    menus: list[plugin_config.PluginMenu] = []
                    menus.append(config_object_hook(obj_dict['menus']))
                    return plugin_config.Plugin(name=obj_dict['name'],
                                                description=obj_dict['description'],
                                                author_name=obj_dict['author_name'],
                                                author_email=obj_dict['author_email'],
                                                menus=menus)
                case config.ConfigInfo.__name__:
                    return config.ConfigInfo(config_file_path=pathlib.Path(obj_dict['config_file_path']),
                                             db_dir_path=pathlib.Path(obj_dict['db_dir_path']),
                                             menu_font_size=int(obj_dict['menu_font_size']),
                                             default_font_size=int(obj_dict['default_font_size']),
                                             text_font_size=int(obj_dict['text_font_size']),
                                             log_config=logging_config.LoggingConfig(obj_dict['logging_config']),
                                             help_url=obj_dict['help_url'])
                case _:
                    return obj_dict
        else:
            return obj_dict
    except TypeError:
        return obj_dict
