import unittest
from os import environ
from datetime import date

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

import blueprints.google_dashboard.queries as queries


class TestSearchPoliticalAds(unittest.TestCase):
    def setUp(self):
        engine = create_engine(environ.get("DATABASE_URL"), echo=True)
        Session = sessionmaker(bind=engine)
        self.session = Session()

    def test_search_by_querystring(self):
        res = queries.search_political_ads(self.session, querystring="stacey abrams")
        self.assertGreater(len(res), 0)
        for google_ad_creative in res:
            self.assertTrue(google_ad_creative.advertiser)
            self.assertTrue(google_ad_creative.creative_stat)
        # TODO actually test that the querystring gets used.

    def test_search_by_advertiser_id(self):
        advertiser_id_to_test = "AR534531769731383296"
        res = queries.search_political_ads(
            self.session, advertiser_id=advertiser_id_to_test
        )
        self.assertGreater(len(res), 0)
        for google_ad_creative in res:
            self.assertTrue(google_ad_creative.advertiser)
            self.assertTrue(google_ad_creative.creative_stat)
            self.assertEqual(google_ad_creative.advertiser_id, advertiser_id_to_test)

    def test_search_by_advertiser_name(self):
        advertiser_name_to_test = "JON OSSOFF FOR SENATE"
        res = queries.search_political_ads(
            self.session, advertiser_name=advertiser_name_to_test
        )
        self.assertGreater(len(res), 0)
        for google_ad_creative in res:
            self.assertTrue(google_ad_creative.advertiser)
            self.assertTrue(google_ad_creative.creative_stat)
            self.assertEqual(
                google_ad_creative.advertiser.advertiser_name, advertiser_name_to_test
            )

    def test_search_with_start_date_and_end_date(self):
        start_date = date(2020, 12, 11)
        end_date = date(2020, 12, 20)
        res = queries.search_political_ads(
            self.session,
            start_date=start_date,
            end_date=end_date,
            advertiser_name="JON OSSOFF FOR SENATE",
        )
        self.assertTrue(len(res) > 0)
        for google_ad_creative in res:
            self.assertTrue(google_ad_creative.advertiser)
            self.assertTrue(google_ad_creative.creative_stat)
            self.assertLessEqual(
                google_ad_creative.creative_stat.first_served_timestamp.date(),
                end_date
            )
            self.assertGreaterEqual(
                google_ad_creative.creative_stat.last_served_timestamp.date(),
                start_date
            )

    def test_pagination(self):
        res1 = queries.search_political_ads(self.session, page=1)

        res2 = queries.search_political_ads(self.session, page=2)
        self.assertGreater(len(res1), 0)
        self.assertGreater(len(res2), 0)
        res1_ad_ids = [gac.ad_id for gac in res1]
        res2_ad_ids = [gac.ad_id for gac in res2]
        for ad_id in res1_ad_ids:
            self.assertNotIn(ad_id, res2_ad_ids)
