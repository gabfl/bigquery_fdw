import unittest
import datetime

from ..bqclient import BqClient


class Test(unittest.TestCase):

    def setUp(self):
        self.bc = BqClient()

    def test_varToString(self):
        self.assertEqual(self.bc.varToString(1234), '1234')

    def test_varToString_2(self):
        self.assertEqual(self.bc.varToString('1234'), '1234')

    def test_varToString_3(self):
        self.assertEqual(self.bc.varToString(datetime.datetime(
            2018, 5, 27, 19, 53, 42)), '2018-05-27 19:53:42')

    def test_varToString_4(self):
        self.assertEqual(self.bc.varToString(datetime.datetime(
            2018, 5, 27, 19, 53, 42).date()), '2018-05-27')
