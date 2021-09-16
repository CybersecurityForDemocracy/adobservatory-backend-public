import unittest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from os import environ

import blueprints.google_dashboard.queries as queries


class TestAdvertiserSpecificQueries(unittest.TestCase):
    def setUp(self):
        engine = create_engine(environ.get("DATABASE_URL"), echo=True)
        Session = sessionmaker(bind=engine)
        self.session = Session()

    def test_spend_of_advertiser_by_week(self):
        res = queries.spend_of_advertiser_by_week(self.session, "JON OSSOFF FOR SENATE")
        self.assertIn("total_spend", res)
        self.assertIn("spend_by_week", res)
        self.assertIn("start_date", res)
        self.assertIn("end_date", res)
        self.assertIsNotNone(res["total_spend"])
        self.assertIsNotNone(res["spend_by_week"])
        self.assertIsNotNone(res["start_date"])
        self.assertIsNotNone(res["end_date"])
        for item in res["spend_by_week"]:
            self.assertIn("spend", item)
            self.assertIn("week_start_date", item)

    def test_spend_of_advertiser_id_by_week(self):
        res = queries.spend_of_advertiser_id_by_week(
            self.session, "AR534531769731383296"
        )
        self.assertIn("total_spend", res)
        self.assertIn("spend_by_week", res)
        self.assertIn("start_date", res)
        self.assertIn("end_date", res)
        self.assertIsNotNone(res["total_spend"])
        self.assertIsNotNone(res["spend_by_week"])
        self.assertIsNotNone(res["start_date"])
        self.assertIsNotNone(res["end_date"])
        for item in res["spend_by_week"]:
            self.assertIn("spend", item)
            self.assertIn("week_start_date", item)

    def test_spend_of_advertiser_by_week_data_validity(self):
        advertiser_name_res = queries.spend_of_advertiser_by_week(
            self.session, "JON OSSOFF FOR SENATE"
        )
        advertiser_id_res = queries.spend_of_advertiser_id_by_week(
            self.session, "AR534531769731383296"
        )

        advertiser_name_spend_by_week = {
            str(item["week_start_date"]): item["spend"]
            for item in advertiser_name_res["spend_by_week"]
        }
        advertiser_id_spend_by_week = {
            str(item["week_start_date"]): item["spend"]
            for item in advertiser_id_res["spend_by_week"]
        }
        for week, spend in advertiser_name_spend_by_week.items():
            if week in advertiser_id_spend_by_week.keys():
                self.assertGreaterEqual(spend, advertiser_id_spend_by_week[week])

    # TODO: test behavior of start_date, end date for queries.spend_of_advertiser_by_week(self.session, "JON OSSOFF FOR SENATE")
    def test_spend_of_advertiser_by_region(self):
        res = queries.spend_of_advertiser_by_region(
            self.session, "JON OSSOFF FOR SENATE"
        )
        self.assertIn("spend_by_region", res)
        self.assertIn("start_date", res)
        self.assertIn("end_date", res)
        self.assertIsNotNone(res["spend_by_region"])
        self.assertIsNotNone(res["start_date"])
        self.assertIsNotNone(res["spend_by_region"])
        for item in res["spend_by_region"]:
            self.assertIn("spend", item)
            self.assertIn("region", item)
        self.assertGreaterEqual(len(res["spend_by_region"]), 50)


if __name__ == "__main__":
    unittest.main()
