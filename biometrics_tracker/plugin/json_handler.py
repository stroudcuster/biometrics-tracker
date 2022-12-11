import json
import pathlib
import sys
from typing import Any, Union

import biometrics_tracker.plugin.plugin as plugin_model


def retrieve_plugins(plugin_path: pathlib.Path) -> list[plugin_model.Plugin]:
    """
    Deserialize the Plugin objects encoded in JSON file in the plugins folder

    :param plugin_path: a Path object pointing to the folder containing the Plugin JSON files
    :type: pathlib.Path
    :return: a list of de-serialized Plugin objects
    :rtype: list[plugin.PluginMenu]

    """
    plugins: list[plugin_model.Plugin] = []
    for plugin_json in plugin_path.glob('*.json'):
        try:
            plugins.append(retrieve_plugin(plugin_json))
        except json.JSONDecodeError:
            pass
    return plugins


def retrieve_plugin(plugin_json: pathlib.Path) -> plugin_model.Plugin:
    """
    Deserialize the Plugin object from a JSON file

    :param plugin_json: a Path object associated with the JSON file
    :type plugin_json: pathlib.Path
    :return: the deserialized Plugin object
    :rtype: biometrics_tracker.plugin.plugin.Plugin

    """
    try:
        with plugin_json.open(mode='r') as pj:
            json_str = pj.read()
            plugin: plugin_model.Plugin = json.loads(eval(json_str), object_hook=plugin_object_hook)
            return plugin
    except json.JSONDecodeError:
        raise json.JSONDecodeError(sys.exc_info)


def save_plugins(plugins: list[plugin_model.Plugin], plugin_path: pathlib.Path):
    """
    Serialize a list of Plugin instances to JSON files.  The file names are based on the Plugin.name and author_name properties

    :param plugins: a list of Plugin objects
    :type plugins: list[biometrics_tracker.plugin.plugin.Plugin]
    :param plugin_path: a Path object pointing to the plugin folder
    :type plugin_path: pathlib.Path
    :return: None
    """
    for plugin in plugins:
        if isinstance(plugin, plugin_model.Plugin):
            json_str = json.dumps(plugin, cls=PluginJSONEncoder)
            with pathlib.Path(plugin_path,
                              f'{plugin.author_name.replace(" ", "_")}-{plugin.name.replace(" ","_")}.json').open(mode='w') as pj:
                pj.write(json_str)
        else:
            raise TypeError(f'{plugin.__str__()} is not a valid biometrics_tracker.plugin.plugin.Plugin object')


def decode_bool(bool_str: str) -> bool:
    return bool_str in ['true', 'True']


class PluginJSONEncoder(json.JSONEncoder):
    """
    Implements a custom json.JSONEncoder to encode instances of Plugin, PluginMenu and PluginMenuItem instances

    """
    def default(self, obj: Any):
        """
        This method is invoked to handle data types that can't be handled by the standard JSONEncoder

        :param obj:
        :return: Any
        """
        match obj.__class__.__name__:
            case plugin_model.PluginMenuItem.__name__:
                json_str = json.JSONEncoder().encode({'title': obj.title,
                                                      'entry_point': obj.entry_point_name,
                                                      'select_person': str(obj.select_person),
                                                      'select_date_range': str(obj.select_date_range),
                                                      'select_dp_type': str(obj.select_dp_type),
                                                      'class': plugin_model.PluginMenuItem.__name__})
                return json_str
            case plugin_model.PluginMenu.__name__:
                list_str = f'[{",".join([PluginJSONEncoder().default(item) for item in obj.items])}]'
                json_str = json.JSONEncoder().encode({'title': obj.title,
                                                      'module': obj.module_name,
                                                      'items': list_str,
                                                      'class': obj.__class__.__name__})
                return json_str
            case plugin_model.Plugin.__name__:
                menu_list_str: str = f'[{",".join([PluginJSONEncoder().default(menu) for menu in obj.menus])}]'
                json_str = json.JSONEncoder().encode({'name': obj.name,
                                                      'description': obj.description,
                                                      'author_name': obj.author_name,
                                                      'author_email': obj.author_email,
                                                      'menus': menu_list_str,
                                                      'class': obj.__class__.__name__})
                return json_str
            #  no match, revert to the standard encoder
            case _:
                return json.JSONEncoder.default(self, obj)


def plugin_object_hook(obj_dict: dict) -> Union[dict, plugin_model.Plugin, plugin_model.PluginMenu,
                                                plugin_model.PluginMenuItem]:
    """
    A custom object hook function to handle the decoding of JSON representations of ConfigInfo, ImportSpecs and
    ExportSpecs

    :param obj_dict: a dict containing JSON names and values
    :type obj_dict: dict[str, str]
    :return: an instance of the appropriate class (e.g. ConfigInfo, ImportSpecs, etc)
    :rtype: Union[dict, plugin.Plugin, plugin.PluginMenu, plugin.PluginMenuItem]


    """
    try:
        if 'class' in obj_dict:
            match obj_dict['class']:
                case plugin_model.PluginMenuItem.__name__:
                    return plugin_model.PluginMenuItem(title=obj_dict['title'],
                                                       entry_point_name=obj_dict['entry_point'],
                                                       select_person=decode_bool(obj_dict['select_person']),
                                                       select_date_range=decode_bool(obj_dict['select_date_range']),
                                                       select_dp_type=decode_bool(obj_dict['select_dp_type']))
                case plugin_model.PluginMenu.__name__:
                    items: list[plugin_model.PluginMenuItem] = []
                    for item in eval(obj_dict['items']):
                        items.append(plugin_object_hook(item))
                        """
                        items.append(plugin.PluginMenuItem(title=item['title'],
                                                                  entry_point=item['entry_point'],
                                                                  select_person=decode_bool(item['select_person']),
                                                                  select_date_range=decode_bool(item['select_date_range']),
                                                                  select_dp_type=decode_bool(item['select_dp_type'])))
                        """
                    return plugin_model.PluginMenu(title=obj_dict['title'], module_name=obj_dict['module'], items=items)
                case plugin_model.Plugin.__name__:
                    menus: list[plugin_model.PluginMenu] = []
                    for menu in eval(obj_dict['menus']):
                        menus.append(plugin_object_hook(menu))
                    return plugin_model.Plugin(name=obj_dict['name'],
                                               description=obj_dict['description'],
                                               author_name=obj_dict['author_name'],
                                               author_email=obj_dict['author_email'],
                                               menus=menus)
                case _:
                    return obj_dict
        else:
            return obj_dict
    except TypeError:
        return obj_dict
