import unittest

from ..pytails.helpers.bson_utils import bson_timestamp_to_int, int_to_bson_timestamp
from datetime import datetime
from bson import Timestamp


class TestBsonUtils(unittest.TestCase):
    def test_bson_timestamp_to_int_correct(self):
        ts = Timestamp(datetime(2019, 12, 1, 11, 12, 13), 0)
        actual = bson_timestamp_to_int(ts)
        expected = 6765427042935635968
        self.assertEqual(actual, expected)

    def test_bson_timestamp_to_int_with_int_correct(self):
        ts = Timestamp(datetime(2019, 12, 1, 11, 12, 13), 1)
        actual = bson_timestamp_to_int(ts)
        expected = 6765427042935635969
        self.assertEqual(actual, expected)

    def test_int_to_bson_timestamp_correct(self):
        expected = Timestamp(datetime(2019, 12, 1, 11, 12, 13), 0)
        actual = int_to_bson_timestamp(6765427042935635968)
        self.assertEqual(actual, expected)

    def test_int_to_bson_timestamp_with_int_correct(self):
        expected = Timestamp(datetime(2019, 12, 1, 11, 12, 13), 1)
        actual = int_to_bson_timestamp(6765427042935635969)
        self.assertEqual(actual, expected)
