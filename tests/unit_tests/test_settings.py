import pathlib
import unittest
from unittest import mock

from mecon import settings


class SettingsTestCase(unittest.TestCase):
    def test___init__(self):
        with mock.patch.object(settings.Settings, '_load') as mck_load:
            mck_load.return_value = {"a": 1, "b": "beta"}

            sets = settings.Settings('example_path')

            mck_load.assert_called_once()
            self.assertEqual(len(sets.keys()), 2)
            self.assertTrue('a' in sets.keys())
            self.assertEqual(sets['a'], 1)
            self.assertTrue('b' in sets.keys())
            self.assertEqual(sets['b'], 'beta')

    def test__setitem__(self):
        with mock.patch.object(settings.Settings, '_load') as mck_load,\
                mock.patch.object(settings.Settings, 'save') as mck_save:
            mck_load.return_value = {"a": 1, "b": "beta"}

            sets = settings.Settings('example_path')

            self.assertEqual(mck_save.call_count, 0)

            sets['c'] = 3
            mck_save.assert_called_once()

            sets['d'] = 4
            self.assertEqual(mck_save.call_count, 2)

    def test_all(self):
        temp_file = pathlib.Path('temp_file.json')
        try:
            sets = settings.Settings(temp_file)

            sets['a'] = 1
            sets['b'] = 'beta'

            self.assertDictEqual(
                sets._load(),
                {"a": 1, "b": "beta"}
            )

            sets2 = settings.Settings(temp_file)
            self.assertDictEqual(
                sets2,
                {"a": 1, "b": "beta"}
            )
        except Exception:
            pass
        finally:
            temp_file.unlink()



