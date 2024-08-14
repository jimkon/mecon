import unittest

from services.main.blueprints.reports import graph_utils as gu


class TestGraphUtils(unittest.TestCase):
    def test_calculated_histogram_and_contributions(self):
        test_amounts = [1, 4, 2, 1, 4, 4]
        bin_centers, counts, contributions, bin_width = gu.calculated_histogram_and_contributions(test_amounts)

        self.assertListEqual(list(bin_centers), [1.375, 2.125, 2.875, 3.625])
        self.assertListEqual(list(counts), [2, 1, 0, 3])
        self.assertListEqual(list(contributions), [2, 2, 0, 12])
        self.assertEqual(bin_width, .750)


if __name__ == '__main__':
    unittest.main()
