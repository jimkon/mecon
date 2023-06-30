import pathlib


class Dataset:
    def __init__(self, path: str | pathlib.Path):
        self._path = pathlib.Path(path)
        self._data = self._path / 'data'

        self._sqlite = self._data / 'db'
        self._sqlite.mkdir(parents=True, exist_ok=True)

        self._statements = self._data / 'statements'
        self._statements.mkdir(parents=True, exist_ok=True)

    @property
    def sqlite(self):
        return self._sqlite / 'sqlite3'

    @property
    def statements(self):
        return self._statements

    def add_statement(self, bank_name: str, statement_path: str|pathlib.Path):
        statement_path = pathlib.Path(statement_path)
        filename = statement_path.name
        new_statement_path = self.statements / bank_name / filename
        new_statement_path.write_bytes(statement_path.read_bytes())


class DatasetDir:
    def __init__(self, path: str | pathlib.Path):
        self._path = pathlib.Path(path)
        self._path.mkdir(parents=True, exist_ok=True)

        self._datasets = []
        for subpath in self._path.iterdir():
            if subpath.is_dir():
                try:
                    self._datasets.append(Dataset(subpath))
                except FileNotFoundError as e:
                    print(f"Dataset {subpath} failed to initialize.")

    @property
    def path(self):
        return self._path

    def datasets(self):
        return self._datasets

    def get_dataset(self, dataset_name) -> Dataset:
        dataset_path = self.path / dataset_name
        return Dataset(dataset_path) if dataset_path.exists() else None

