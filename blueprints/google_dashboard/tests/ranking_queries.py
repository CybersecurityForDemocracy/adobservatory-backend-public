import unittest
from os import environ
from datetime import date

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

import blueprints.google_dashboard.queries as queries


class TestRankingQueries(unittest.TestCase):
    def setUp(self):
        engine = create_engine(environ.get("DATABASE_URL"), echo=True)
        Session = sessionmaker(bind=engine)
        self.session = Session()

    def test_top_political_advertisers_since_date_of_US(self):
        res, start_date, end_date = queries.top_political_advertisers_since_date(
            self.session,
            region="US",
        )
        self.assertGreater(len(res), 0)
        self.assertEqual(len(res[0]), 2)
        for name, spend in res:
            self.assertIsInstance(name, str)
            self.assertIsInstance(spend, (int, float))

    def test_top_political_advertisers_since_date_of_state(self):
        res, start_date, end_date = queries.top_political_advertisers_since_date(
            self.session,
            region="GA",
        )
        self.assertGreater(len(res), 0)
        self.assertEqual(len(res[0]), 2)
        for name, spend in res:
            self.assertIsInstance(name, str)
            self.assertIsInstance(spend, (int, float))
