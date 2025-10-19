from pathlib import Path

from setuptools import setup


BASE_DIR = Path(__file__).resolve().parent
VERSION = (BASE_DIR / "mecon" / "version.txt").read_text(encoding="utf-8").strip()

setup(
   name='mecon',
   version=VERSION,
   description='My economics',
   packages=['mecon'],  #same as name
   package_data={'mecon': ['version.txt']},
)
