from dataclasses import dataclass, field
from importlib import import_module
from typing import Callable, Optional


class PluginNotFoundError(ModuleNotFoundError):
    """
    This exception is thrown if the PluginMenuItem.import_entry_point method can not find a module specified
    in a PluginMenu entry

    """
    def __init__(self, *args, name: Optional[str], path: Optional[str]):
        ModuleNotFoundError.__init__(self, args, name, path)


class PluginImportError(ImportError):
    """
    This exception is thrown if the PluginMenuItem.import_entry_point module importation fails due to a
    problem with the module code, such as a syntax error or undefined symbol

    """
    def __init__(self, *args, name: Optional[str], path: Optional[str]):
        ImportError.__init__(self, args, name, path)


@dataclass
class PluginMenuItem:
    """
    A data class that holds the information necessary to create a menu item for a plugin entry point. The module
    name to be imported is supplied by the parent PluginMenu instance.

    :param title: the title for the menu item
    :type title: str
    :param entry_point: the name of the callback for the menu item
    :type entry_point: str
    :param select_person: should the menu item present a Person selection list prior to invoking the entry point
    :type select_person: bool
    :param select_date_range: should the menu item present a date range selection list prior to invoking the entry point
    :type select_date_range: bool
    :param select_dp_type: should the menu item present a DataPointType selection list prior to invoking the entry point
    :type select_dp_type: bool

    """
    title: str
    entry_point: str
    select_person: bool
    select_date_range: bool
    select_dp_type: bool

    def import_entry_point(self, module: str, not_found_action: Callable) -> tuple[bool, Callable]:
        """
        Imports the module specified in a PluginMenu instance, then checks for the existance of the entry point
        specified in the PluginMenuItem instance.  If the module can not be loaded, a custom exception is raised
        If the module can be imported, a check is made to see if the entry point exists.  If it does not exist,
        then then the provided not_found_actions callback is used.

        :param not_found_action: a Callable to be used as the menu item callback if the entry_point doesnt exist
        :type not_found_action: Callable
        :return: a Callable to be used as a callback for the menu item
        :rtype: Callable

        """
        try:
            module = import_module(module)
            if self.entry_point in module.__dict__ and isinstance(module.__dict__[self.entry_point], Callable):
                return True, module.__dict__[self.entry_point]
            else:
                return False, \
                       lambda msg=f'Entry Point {self.entry_point} not found in module {module}': not_found_action(msg)
        except ModuleNotFoundError:
            raise PluginNotFoundError(f'Module {module} not found')
            # return lambda msg=f'Module {module} not found': not_found_action(msg)
        except ImportError:
            raise PluginImportError('Module {module} could not be imported.')
            # return lambda msg=f'Module {module} could not be imported.': not_found_action(msg)


@dataclass
class PluginMenu:
    """
    A dataclass that hold the information necessary to specify an application menu.  This class is GUI framework
    agnostic.  An example of an tkinter based implementation which used composition to access the functionality
    of this class can be found in the biometrics_tracker.gui.widgets module.

    """
    title: str
    module: str
    items: list[PluginMenuItem]

    def add_item(self, item: PluginMenuItem):
        """
        Add a PluginMenuItem instance to the list maintained by this instance

        :param item: the PluginMenuItem instance to be added
        :type item: biometrics_tracker.config.PluginMenuItem
        :return: None
        """
        self.items.append(item)

    def create_menu(self, not_found_action: Callable, selection_action: Callable, add_menu_item: Callable,
                    add_menu: Callable):
        """
        Create a menu from a PluginMenu instance and it's PluginMenuItem children, using callbacks provided the
        the GUI implementation to create the menus and menu items.

        :param not_found_action: the callback to be used when an entry point can not be found
        :type not_found_action: Callable
        :param selection_action: the callback to be used for Person, date range and DataPointType selections
        :type selection_action: Callable
        :param add_menu_item: the callback to be used to add menu items to the menu
        :type add_menu_item: Callable
        :param add_menu: the callback to be used to associate the menu created with a parent menu
        :type add_menu: Callable
        :return: None

        """
        for item in self.items:
            try:
                found, entry_point = item.import_entry_point(self.module, not_found_action)
                if found and selection_action:
                    sel_lambda: Callable = \
                        lambda sp=item.select_person, sd=item.select_date_range, st=item.select_dp_type, \
                               ep=entry_point: selection_action(sp, sd, st, ep)
                    add_menu_item(label=item.title, action=sel_lambda)
                else:
                    add_menu_item(label=item.title, action=entry_point)
            except PluginNotFoundError:
                add_menu_item(label=f'{item.title} not found', action=not_found_action)
            except PluginImportError:
                add_menu_item(label=f'{item.title} import error', action=not_found_action)
        add_menu(label=self.title)


@dataclass
class Plugin:
    """
    Holds general info and menu specs for a plugin

    """
    name: str
    description: str
    author_name: str
    author_email: str
    menus: list[PluginMenu]

    def __post_init__(self):
        """
        Invoked after __init__, initiallizes the menus property to an empty list

        :return: None

        """
        if self.menus is None:
            self.menus = []

    def add_menu(self, menu: PluginMenu):
        """
        Add a PluginMeu instance to the menus list property

        :param menu: the menu object to be added
        :type menu: biometrics_tracking.config.plugin_config.PluginMenu
        :return: None

        """
        self.menus.append(menu)

