import json
import pathlib

from mecon.config import SETTINGS_JSON_FILENAME


# TODO add saved links
class Settings(dict):
    def __init__(self, path: str | pathlib.Path | None = None):
        if path is None:
            path = SETTINGS_JSON_FILENAME

        self._json_path = pathlib.Path(path)
        if not self._json_path.exists():
            self._json_path.write_text('{}')
        _dict = self._load()
        super().__init__(**_dict)

    def _load(self):
        _dict = json.loads(self._json_path.read_text())
        return _dict

    def save(self):
        self._json_path.write_text(json.dumps(self, indent=4))

    def __setitem__(self, key, value):
        super().__setitem__(key, value)
        self.save()

    def __str__(self):
        return json.dumps(self, indent=4)
