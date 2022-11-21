import decimal
import json
import pathlib
import pytest

import biometrics_tracker.config.json_handler as jh
import biometrics_tracker.config.plugin_config as plugin_config
import biometrics_tracker.config.createconfig as config

from tests.config.config_fixtures import plugin_menus_fixture, plugin_fixture

from tests.test_tools import compare_object, attr_error


def menu_error_preamble(title: str) -> str:
    return f'Menu {title}:'


def plugin_error_preamble(name: str) -> str:
    return f'Plugin {name}:'


def compare_menu(menu: plugin_config.PluginMenu, check_menu: plugin_config.PluginMenu):
    if not compare_object(menu, check_menu):
        assert menu.title == check_menu.title, \
            f'{menu_error_preamble(menu.title)} {attr_error("Title", menu.title, check_menu.title)}'
        assert menu.module == check_menu.module, \
            f'{menu_error_preamble(menu.title)} {attr_error("Package", menu.module, check_menu.module)}'
    for item, check_item in zip(menu.items, check_menu.items):
        if not compare_object(item, check_item):
            assert item.title == check_item.title, \
                f'{menu_error_preamble(menu.title)} Item: {item.title} {attr_error("Title", item.title, check_item.title)}'

            assert item.entry_point == check_item.entry_point, \
                f'{menu_error_preamble(menu.title)} Item: {item.title} ' \
                f'{attr_error("Entry Point", item.entry_point, check_item.title_entry_point)}'
            assert item.select_person == check_item.select_person, \
                f'{menu_error_preamble(menu.title)} Item: {item.title} ' \
                f'{attr_error("Select Person", item.select_person, check_item.select_person)}'
            assert item.select_date_range == check_item.select_date_range, \
                f'{menu_error_preamble(menu.title)} Item: {item.title} ' \
                f'{attr_error("Select Date Range", item.select_date_range, check_item.select_date_range)}'
            assert item.select_dp_type == check_item.select_dp_type, \
                f'{menu_error_preamble(menu.title)} Item: {item.title} ' \
                f'{attr_error("Select DataPoint Type", item.select_dp_type, check_item.select_dp_type)}'


def compare_plugin(plugin: plugin_config.Plugin, check_plugin: plugin_config.Plugin):
    if not compare_object(plugin, check_plugin):
        assert plugin.name == check_plugin.name, \
            f'{plugin_error_preamble(plugin.name)} {attr_error("Name", plugin.name, check_plugin.name)}'
        assert plugin.description == check_plugin.description, \
            f'{plugin_error_preamble(plugin.description)} {attr_error("Name", plugin.description, check_plugin.description)}'
        assert plugin.author_name == check_plugin.author_name, \
            f'{plugin_error_preamble(plugin.author_name)} {attr_error("Name", plugin.author_name, check_plugin.author_name)}'
        assert plugin.author_email == check_plugin.author_email, \
            f'{plugin_error_preamble(plugin.author_email)} {attr_error("Name", plugin.author_email, check_plugin.author_email)}'
        for menu, check_menu in zip(plugin.menus, check_plugin.menus):
            compare_menu(menu, check_menu)


@pytest.mark.Plugins
def test_plugin_config_json(tmpdir, plugin_menus_fixture, plugin_fixture):
    for menu in plugin_menus_fixture:
        json_str = json.dumps(menu, cls=jh.ConfigJSONEncoder)
        assert json_str is not None and len(json_str) > 0, f'No JSON Output for Menu {menu.title}'
        check_menu = json.loads(eval(json_str), object_hook=jh.config_object_hook)
        assert check_menu is not None, "JSON PluginMenu de-serialization failed"
        compare_menu(menu, check_menu)

    for plugin in plugin_fixture:
        json_str = json.dumps(plugin, cls=jh.ConfigJSONEncoder)
        assert json_str is not None and len(json_str) > 0, f'No JSON Output for Plugin {plugin.name}'
        check_plugin = json.loads(eval(json_str), object_hook=jh.config_object_hook)
        assert check_plugin is not None, "JSON Plugin de-serialization failed"
        compare_plugin(plugin, check_plugin)


@pytest.mark.Plugins
def test_save_and_retrieve_plugin(tmpdir, plugin_fixture):
    plugin_path = pathlib.Path(tmpdir, 'plugins')
    plugin_path.mkdir(exist_ok=True)
    jh.save_plugins(plugin_fixture, plugin_path)
    check_plugins: list[plugin_config.Plugin] = jh.retrieve_plugins(plugin_path)
    assert len(check_plugins) == len(plugin_fixture), f'Wrote {len(plugin_fixture)} files, Read {len(check_plugins)} files'
    for plugin, check_plugin in zip(plugin_fixture, check_plugins):
        compare_plugin(plugin, check_plugin)

