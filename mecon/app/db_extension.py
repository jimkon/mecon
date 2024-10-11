import logging
import pathlib
import shutil
from datetime import datetime

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from mecon.app.models import Base


class DailyDBBackUp:
    def __init__(self, db_filepath):
        self.db_filepath = pathlib.Path(db_filepath)

        if not self.todays_backup_exist():
            self.create_backup()
        else:
            logging.info(f"Back up already exists at {self.backup_filepath}")

    @property
    def backups_directory_path(self) -> pathlib.Path:
        backups_directory_path = self.db_filepath.parent / 'backups'
        return backups_directory_path

    @property
    def current_backup_dirpath(self) -> pathlib.Path:
        current_backup_dirpath = self.backups_directory_path / datetime.now().strftime('%Y%m%d')
        current_backup_dirpath.mkdir(exist_ok=True, parents=True)
        return current_backup_dirpath

    @property
    def backup_filepath(self) -> pathlib.Path:
        backup_filepath = self.current_backup_dirpath / self.db_filepath.name
        return backup_filepath

    def create_backup(self):
        try:
            shutil.copy(self.db_filepath, self.backup_filepath)
            logging.info(f"Created a back up at {self.backup_filepath}")
        except Exception as e:
            logging.warning(f"ERROR: Creating backup for DB '{self.db_filepath}' failed: {e}")

    def todays_backup_exist(self):
        return self.backup_filepath.exists()


class DBWrapper:
    def __init__(self, db_path):
        self._path = db_path
        self._engine, self._session_maker = None, None
        self.init_db()
        DailyDBBackUp(db_path)

    def init_db(self):
        # Create the engine connected to the SQLite database
        self._engine = create_engine(f'sqlite:///{self._path}')

        # Create all tables in the engine (if they don't exist)
        Base.metadata.create_all(self.engine)

        # Return the engine and session
        self._session_maker = sessionmaker(bind=self.engine)

    @property
    def engine(self):
        return self._engine

    def new_session(self):
        return self._session_maker()
