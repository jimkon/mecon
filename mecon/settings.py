import datetime
import json
import pathlib

from mecon import config

JSON_FILE_FORMATING_KWARGS = {
    'indent': 4,
    'sort_keys': True
}

METADATA_JSON_FILEPATH = 'metadata.json'


class DictFile(dict):
    _default_dict = {}

    def __init__(self,
                 path: str | pathlib.Path,
                 default_dict: dict | None = None):
        self._path = pathlib.Path(path)
        if not self.path.exists():
            default_dict = default_dict or self._default_dict
            self.path.write_text(json.dumps(default_dict, **JSON_FILE_FORMATING_KWARGS))
        _dict = self._load()
        super().__init__(**_dict)

    @property
    def path(self):
        return self._path

    def _load(self):
        _dict = json.loads(self.path.read_text())
        return _dict

    def save(self):
        self.path.write_text(json.dumps(self, **JSON_FILE_FORMATING_KWARGS))

    def __setitem__(self, key, value):
        super().__setitem__(key, value)
        self.save()

    def __str__(self):
        return json.dumps(self, **JSON_FILE_FORMATING_KWARGS)


class Settings(dict):
    def __init__(self, path: str | pathlib.Path | None = None):
        if path is None:
            path = config.SETTINGS_JSON_FILENAME

        super().__init__(path=path)


class Metadata(DictFile):
    def __init__(self,
                 path: str | pathlib.Path | None = None,
                 _dict: dict | None = None):
        _dict['metadata'] = {
            'created': datetime.datetime.now().isoformat()
        }

        full_path = pathlib.Path(path) / METADATA_JSON_FILEPATH if path else pathlib.Path(METADATA_JSON_FILEPATH)

        super().__init__(path=full_path,
                         default_dict=_dict)

    def update_read_metadata(self):
        self['metadata']['read'] = datetime.datetime.now().isoformat()

    def update_write_metadata(self):
        self['metadata']['write'] = datetime.datetime.now().isoformat()


