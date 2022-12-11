from dataclasses import dataclass
from importlib import import_module
from typing import Callable, Optional


class PluginNotFoundError(ModuleNotFoundError):
    """
    This exception is thrown if the PluginMenuItem.import_entry_point method can not find a module specified
    in a PluginMenu entry

    """
    def __init__(self, *args, name: Optional[str] = None, path: Optional[str] = None):
        ModuleNotFoundError.__init__(self, args, name, path)


class PluginImportError(ImportError):
    """
    This exception is thrown if the PluginMenuItem.import_entry_point module importation fails due to a
    problem with the module code, such as a syntax error or undefined symbol

    """
    def __init__(self, *args, name: Optional[str] = None, path: Optional[str] = None):
        ImportError.__init__(self, args, name, path)


@dataclass
class PluginMenuItem:
    """
    A data class that holds the information necessary to create a menu item for a plugin entry point. The module
    name to be imported is supplied by the parent PluginMenu instance.

    :param title: the title for the menu item
    :type title: str
    :param entry_point_name: the name of the callback for the menu item
    :type entry_point_name: str
    :param select_person: should the menu item present a Person selection list prior to invoking the entry point
    :type select_person: bool
    :param select_date_range: should the menu item present a date range selection list prior to invoking the entry point
    :type select_date_range: bool
    :param select_dp_type: should the menu item present a DataPointType selection list prior to invoking the entry point
    :type select_dp_type: bool

    """
    title: str
    entry_point_name: str
    select_person: bool
    select_date_range: bool
    select_dp_type: bool

    def __eq__(self, other) -> bool:
        """
        Compare PluginMenuItem instances by the title property

        :param other: the PluginMenuItem instance to be compared
        :type other: biometrics_tracker.config.plugin_config.PluginMenuItem
        :return: True is two instances title properties are equal
        :rtype: bool

        """
        if self.title == other.title:
            return True
        else:
            return False

    def import_entry_point(self, module_name: str, not_found_action: Callable) -> tuple[bool, Callable]:
        """
        Imports the module specified in a PluginMenu instance, then checks for the existence of the entry point
        specified in the PluginMenuItem instance.  If the module can not be loaded, a custom exception is raised
        If the module can be imported, a check is made to see if the entry point exists.  If it does not exist,
        then then the provided not_found_actions callback is used.

        :param module_name: the name of the plugin module to be imported
        :type module_name: str
        :param not_found_action: a Callable to be used as the menu item callback if the entry_point doesnt exist
        :type not_found_action: Callable
        :return: a Callable to be used as a callback for the menu item
        :rtype: Callable

        """
        try:
            module = import_module(module_name)
            if self.entry_point_name in module.__dict__ and isinstance(module.__dict__[self.entry_point_name], Callable):
                return True, module.__dict__[self.entry_point_name]
            else:
                return False, \
                       lambda msg=f'Entry Point {self.entry_point_name} not found in module {module}': not_found_action(msg)
        except ModuleNotFoundError:
            raise PluginNotFoundError(f'Module {module_name} not found', name=module_name)
            # return lambda msg=f'Module {module} not found': not_found_action(msg)
        except ImportError:
            raise PluginImportError('Module {module} could not be imported.', name=module_name)
            # return lambda msg=f'Module {module} could not be imported.': not_found_action(msg)

    def __str__(self) -> str:
        sel_person, sel_dates, sel_dp_type = ' ', ' ', ' '
        if self.select_person:
            sel_person = 'Person'
        if self.select_date_range:
            if self.select_person:
                sel_dates = ', Date Range'
            else:
                sel_dates = 'Date Range'
        if self.select_dp_type:
            if self.select_person or self.select_date_range:
                sel_dp_type = ', DataPointType'
            else:
                sel_dp_type = 'DataPointType'
        return f'Title: {self.title}, Entry Point: {self.entry_point_name} Select {sel_person}{sel_dates}{sel_dp_type}'

    def __repr__(self):
        return f'model.PluginMenuItem(title="{self.title}", entry_point_name="{self.entry_point_name}",' \
               f'select_person={str(self.select_person)}, select_date_range={str(self.select_date_range)},' \
               f'select_dp_type={str(self.select_dp_type)})'


@dataclass
class PluginMenu:
    """
    A dataclass that hold the information necessary to specify an application menu.  This class is GUI framework
    agnostic.  An example of an tkinter based implementation which used composition to access the functionality
    of this class can be found in the biometrics_tracker.gui.widgets module.

    """
    title: str
    module_name: str
    items: list[PluginMenuItem]

    def __eq__(self, other) -> bool:
        """
        Compare PluginMenu instances by the title property

        :param other: the PluginMenu instance to be compared
        :type other: biometrics_tracker.config.plugin_config.PluginMenu
        :return: True is two instances title properties are equal
        :rtype: bool

        """
        if self.title == other.title:
            return True
        else:
            return False

    def add_item(self, item: PluginMenuItem):
        """
        Add a PluginMenuItem instance to the list maintained by this instance

        :param item: the PluginMenuItem instance to be added
        :type item: biometrics_tracker.config.PluginMenuItem
        :return: None
        """
        self.items.append(item)

    def get_menu_item(self, match_title: str) -> Optional[PluginMenuItem]:
        """
        Retrieve a PluginMenuItem object that matches the provided title
        :param match_title: the title of the PluginMenuItem to be retrieved
        :type match_title: str
        :return: Optional[biometrics_tracker.plugin.plugin.PluginMenuItem

        """
        idx = self.items.index(match_title)
        if idx < 0:
            return None
        else:
            return self.items[idx]

    def remove_menu_item(self, match_title: str):
        if self.items.index(match_title):
            self.items.remove(match_title)

    def create_menu(self, not_found_action: Callable, selection_action: Callable, add_menu_item: Callable,
                    add_menu: Callable):
        """
        Create a menu from a PluginMenu instance, and it's PluginMenuItem children, using callbacks provided the
        GUI implementation to create the menus and menu items.

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
        menu: Any = add_menu(label=self.title)
        for item in self.items:
            try:
                found, entry_point = item.import_entry_point(self.module_name, not_found_action)
                if found and selection_action:
                    sel_lambda: Callable = \
                        lambda sp=item.select_person, sd=item.select_date_range, st=item.select_dp_type, \
                               ep=entry_point: selection_action(sp, sd, st, ep)
                    add_menu_item(menu=menu, label=item.title, action=sel_lambda)
                else:
                    add_menu_item(menu=menu, label=item.title, action=entry_point)
            except PluginNotFoundError:
                add_menu_item(menu=menu, label=f'{item.title} not found', action=not_found_action)
            except PluginImportError:
                add_menu_item(menu=menu, label=f'{item.title} import error', action=not_found_action)

    def __str__(self):
        return f'Title: {self.title} Module: {self.module_name}'

    def __repr__(self):
        return f'model.PluginMenu(title="{self.title}", module_name="{self.module_name}", items=[])'


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
        Invoked after __init__, initializes the menus property to an empty list

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

    def get_menu(self, match_title: str) -> Optional[PluginMenu]:
        """
        Retrieve a menu that matches the provided title

        :param match_title: the title to be match
        :type match_title: str
        :return: the retrieved PluginMenu object or None
        :rttype: Optional[biometrics_tracker.plugin.plugin.PluginMenu

        """
        idx = self.menus.index(match_title)
        if idx < 0:
            return None
        else:
            return self.menus[idx]

    def remove_menu(self, match_title: str):
        """
        Remove a menu from the menu list

        :param match_title: the title of the menu to be removed
        :type match_title: str`
        :return: None
        """
        if self.menus.index(match_title):
            self.menus.remove(match_title)
