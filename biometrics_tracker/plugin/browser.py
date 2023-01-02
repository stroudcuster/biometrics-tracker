from abc import abstractmethod
import json
import pathlib
import re
import zipfile

import biometrics_tracker.plugin.plugin as plugin
import biometrics_tracker.plugin.json_handler as jh


class NotPluginJSONException(Exception):
    def __init__(self, mbr_name: str, *args, **kwargs):
        Exception.__init__(self, f'File {mbr_name} is not a Plugin JSON file', args, kwargs)


class PluginBrowserBase:
    def __init__(self, wheel_path: pathlib.Path, dest_dir_path):
        self.wheel_path = wheel_path
        self.dest_dir_path = dest_dir_path
        self.plugin_list: list[str] = []
        self.plugin_re = re.compile('\\S*plugin-json/(\\S*.json)')
        self.wheel = zipfile.ZipFile(wheel_path)
        with self.wheel as zf:
            for name in zf.namelist():
                match = self.plugin_re.match(name)
                if match is not None:
                    self.plugin_list.append(name)
        self.show_gui()

    @abstractmethod
    def show_gui(self):
        ...

    def extract_json(self, mbr_name: str) -> None:
        match = self.plugin_re.match(mbr_name)
        if match is not None:
            dest_filename: str = match.groups()[0]
            dest_path: pathlib.Path = pathlib.Path(self.dest_dir_path, dest_filename)
            with self.wheel.open(name=mbr_name, mode='r') as mbr:
                content = mbr.read()
                with dest_path.open(mode='ab') as jf:
                    jf.write(content)
            try:
                plugin_json: plugin.Plugin = json.loads(content.decode(), object_hook=jh.plugin_object_hook)
                if not isinstance(plugin_json, plugin.Plugin):
                    raise NotPluginJSONException(mbr_name)
            except json.JSONDecodeError:
                raise NotPluginJSONException(mbr_name)
        else:
            raise NotPluginJSONException(mbr_name)




