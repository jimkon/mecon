from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base

from mecon.app.models import Base


class DBWrapper:
    def __init__(self, db_path):
        self._path = db_path
        self._engine, self._session_maker = None, None
        self.init_db()

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
