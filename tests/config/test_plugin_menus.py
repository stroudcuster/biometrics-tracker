import pytest
from typing import Callable

from time import sleep
import tkinter as tk
import ttkbootstrap as ttkb
import ttkbootstrap.dialogs.dialogs as dialogs

from tests.config.config_fixtures import plugin_menus_fixture

import biometrics_tracker.config.plugin_config as plugin_config
import biometrics_tracker.gui.widgets as widgets

action1_sel: bool = False
action2_sel: bool = False
action3_sel: bool = False
not_found_sel: bool = False


def not_found(message: str):
    global not_found_sel
    boo = dialogs.MessageDialog(message=message, width=50)
    boo.show()
    not_found_sel = True


def selection_action(person: bool, date_range: bool, dp_type: bool, entry_point: Callable):
    check: str = ''
    if person:
        check = f'{check} Person,'
    if date_range:
        check = f'{check} Date,'
    if dp_type:
        check = f'{check} DP Type'
    yea = dialogs.MessageDialog(message=f'Select = ({check})')
    yea.show()
    entry_point()


def action1():
    global action1_sel
    yea = dialogs.MessageDialog('Action 1 Entry Point')
    yea.show()
    action1_sel = True


def action2():
    global action2_sel
    yea = dialogs.MessageDialog('Action 2 Entry Point')
    yea.show()
    action2_sel = True


def action3():
    global action3_sel
    yea = dialogs.MessageDialog('Action 3 Entry Point')
    yea.show()
    action3_sel = True


class TestWindow(ttkb.Window):
    def __init__(self, plugin_menu_fixture):
        ttkb.Window.__init__(self, title='Plugin Menu Test')
        menubar: ttkb.Menu = ttkb.Menu(master=self, title="Plugin Test")
        self.config(menu=menubar)
        menus: list[ttkb.Menu] = []
        for plugin_menu in plugin_menu_fixture:
            menus.append(widgets.PluginMenu(menubar, plugin_menu).create_menu(
                not_found_action=not_found, selection_action=selection_action))
        tk.Label(self, text='Select each of the menu options, then click Quit. Each selection will display a '
                            'Selection dialog followed by an Action dialog, except for the "Bean Counter: not found"'
                            'option, which will display a single dialog showing the error message',
                 wraplength=300).grid(column=0, row=0)
        ttkb.Button(self, text='Quit', command=self.quit).grid(column=0, row=1)
        self.after(250, sleep(1))
        self.grid()


@pytest.mark.Plugin
def test_plugin_menu(plugin_menus_fixture):
    toplevel: TestWindow = TestWindow(plugin_menus_fixture)
    toplevel.mainloop()
    assert action1_sel, 'Action 1 selection failed'
    assert action2_sel, 'Action 2 selection failed'
    assert action3_sel, 'Action 3 selection failed'
    assert not_found_sel, 'Not Found selection failed'

