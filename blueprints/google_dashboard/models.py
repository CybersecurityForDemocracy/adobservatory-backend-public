import dataclasses
import datetime
from typing import List

import sqlalchemy as db
from sqlalchemy import (
    ARRAY,
    BigInteger,
    Boolean,
    Column,
    Date,
    DateTime,
    Float,
    ForeignKey,
    Integer,
    Numeric,
    String,
    Table,
    Text,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


@dataclasses.dataclass
class AdvertiserRegionalSpend(Base):
    __tablename__ = "advertiser_regional_spend"
    __table_args__ = {'schema': 'googleads'}

    advertiser_id: str
    country: str
    region: str
    spend_usd: int
    report_date: datetime.date

    advertiser_id = db.Column(db.String, primary_key=True, nullable=False)
    country = db.Column(db.Text, primary_key=True, nullable=False)
    region = db.Column(db.Text, primary_key=True, nullable=False)
    spend_usd = db.Column(db.Integer, nullable=False)
    report_date = db.Column(Date, primary_key=True, nullable=False)


@dataclasses.dataclass
class AdvertiserStat(Base):
    __tablename__ = "advertiser_stats"
    __table_args__ = {'schema': 'googleads'}


    advertiser_id: str
    advertiser_name: str
    public_ids_list: str
    regions: str
    elections: str
    total_creatives: int
    spend_usd: int
    report_date: datetime.date

    advertiser_id = db.Column(db.String, primary_key=True)
    advertiser_name = db.Column(
        db.Text, nullable=False
    )  # TODO: do I need an index on this?
    public_ids_list = db.Column(db.String)
    regions = db.Column(db.String, nullable=False)
    elections = db.Column(db.String, nullable=False)
    total_creatives = db.Column(db.Integer, nullable=False)
    spend_usd = db.Column(db.Integer, nullable=False)
    report_date = db.Column(Date, nullable=False)
    creative_stats = relationship("CreativeStat", back_populates="advertiser")
    google_ad_creatives = relationship("GoogleAdCreative", back_populates="advertiser")


@dataclasses.dataclass
class AdvertiserWeeklySpend(Base):
    __tablename__ = "advertiser_weekly_spend"
    __table_args__ = {'schema': 'googleads'}


    advertiser_id: str
    advertiser_name: str
    week_start_date: datetime.date
    spend_usd: int
    election_cycle: str

    advertiser_id = db.Column(db.String, primary_key=True, nullable=False)
    advertiser_name = db.Column(db.Text, nullable=False)
    week_start_date = db.Column(Date, primary_key=True, nullable=False)
    spend_usd = db.Column(db.Integer, nullable=False)
    election_cycle = db.Column(db.String)

@dataclasses.dataclass
class CreativeStat(Base):
    __tablename__ = "creative_stats"
    __table_args__ = {'schema': 'googleads'}


    ad_id: str
    ad_type: str
    regions: str
    advertiser_id: str
    date_range_start: datetime.date
    date_range_end: datetime.date
    num_of_days: int
    spend_usd: str
    first_served_timestamp: datetime.datetime
    last_served_timestamp: datetime.datetime
    age_targeting: str
    gender_targeting: str
    geo_targeting_included: str
    geo_targeting_excluded: str
    spend_range_min_usd: int
    spend_range_max_usd: int
    impressions_min: int
    impressions_max: int
    report_date: datetime.date

    ad_id = db.Column(
        db.String, ForeignKey("googleads.google_ad_creatives.ad_id"), primary_key=True
    )
    ad_type = db.Column(db.String, nullable=False)
    regions = db.Column(db.String, nullable=False)
    advertiser_id = db.Column(
        db.String, ForeignKey("googleads.advertiser_stats.advertiser_id"), nullable=False
    )
    date_range_start = db.Column(Date, nullable=False)
    date_range_end = db.Column(Date, nullable=False)
    num_of_days = db.Column(
        db.Numeric(asdecimal=False), nullable=False
    )  # asdecimal=False here because the JSON serializer has trouble with Decimal type (and this converts it to Floats)
    spend_usd = db.Column(db.String, nullable=False)
    first_served_timestamp = db.Column(db.DateTime)
    last_served_timestamp = db.Column(db.DateTime)
    age_targeting = db.Column(db.String, nullable=False)
    gender_targeting = db.Column(db.String, nullable=False)
    geo_targeting_included = db.Column(db.String, nullable=False)
    geo_targeting_excluded = db.Column(db.String, nullable=False)
    spend_range_min_usd = db.Column(
        db.Numeric(asdecimal=False), nullable=False
    )  # asdecimal=False here because the JSON serializer has trouble with Decimal type (and this converts it to Floats)
    spend_range_max_usd = db.Column(
        db.Numeric(asdecimal=False)
    )  # asdecimal=False here because the JSON serializer has trouble with Decimal type (and this converts it to Floats)
    impressions_min = db.Column(
        db.Numeric(asdecimal=False), nullable=False
    )  # asdecimal=False here because the JSON serializer has trouble with Decimal type (and this converts it to Floats)
    impressions_max = db.Column(
        db.Numeric(asdecimal=False)
    )  # asdecimal=False here because the JSON serializer has trouble with Decimal type (and this converts it to Floats)
    report_date = db.Column(Date)
    google_ad_creative = relationship(
        "GoogleAdCreative", back_populates="creative_stat"
    )
    advertiser = relationship("AdvertiserStat", back_populates="creative_stats")

    def __repr__(self):
        return "CreativeStat(ad_type={}, spend_usd={}, first_served_timestamp={}, last_served_timestamp={}, impressions_min={}, impressions_max={})".format(
            self.ad_type,
            self.spend_usd,
            self.first_served_timestamp,
            self.last_served_timestamp,
            self.impressions_min,
            self.impressions_max,
        )


@dataclasses.dataclass
class GoogleAdCreative(Base):
    __tablename__ = "google_ad_creatives"
    __table_args__ = {'schema': 'googleads'}


    advertiser_id: str
    ad_id: str
    ad_type: str
    error: bool
    youtube_ad_id: str
    ad_text: str
    image_url: str
    image_urls: str
    destination: str
    policy_violation_date: datetime.date

    advertiser_id = db.Column(
        db.String, ForeignKey("googleads.advertiser_stats.advertiser_id"), nullable=False
    )
    ad_id = db.Column(db.String, primary_key=True)
    ad_type = db.Column(db.String, nullable=False)
    error = db.Column(db.Boolean)
    youtube_ad_id = db.Column(db.String)
    ad_text = db.Column(db.Text)
    image_url = db.Column(db.Text)
    image_urls = db.Column(db.ARRAY(db.Text()))
    destination = db.Column(db.Text)
    policy_violation_date = db.Column(Date)
    creative_stat = relationship(
        "CreativeStat", uselist=False, back_populates="google_ad_creative"
    )
    youtube_video = relationship(
        "YoutubeVideo", uselist=False, back_populates="google_ad_creative"
    )
    advertiser = relationship("AdvertiserStat", back_populates="google_ad_creatives")
    # ad = relationship("Ad", back_populates="targetings")
    # targetings = relationship("Targeting", back_populates="ad")


@dataclasses.dataclass
class YoutubeVideo(Base):
    __tablename__ = "youtube_videos"
    __table_args__ = {'schema': 'googleads'}


    id: str
    uploader: str
    uploader_id: str
    uploader_url: str
    channel_id: str
    channel_url: str
    upload_date: datetime.date
    license: str
    creator: str
    title: str
    alt_title: str
    thumbnail: str
    description: str
    categories: List[str] = dataclasses.field(default_factory=list)
    tags: str
    duration: int
    age_limit: int
    webpage_url: str
    view_count: int
    like_count: int
    dislike_count: int
    average_rating: int
    is_live: bool
    display_id: str
    format: str
    format_id: str
    width: int
    height: int
    resolution: str
    fps: int
    fulltitle: bool
    error: bool
    video_unavailable: bool
    video_private: bool
    updated_at: datetime.date

    id = db.Column(
        db.String, ForeignKey("googleads.google_ad_creatives.youtube_ad_id"), primary_key=True
    )
    uploader = db.Column(db.String)
    uploader_id = db.Column(db.String)
    uploader_url = db.Column(db.String)
    channel_id = db.Column(db.String)
    channel_url = db.Column(db.String)
    upload_date = db.Column(Date)
    license = db.Column(db.String)
    creator = db.Column(db.String)
    title = db.Column(db.String)
    alt_title = db.Column(db.String)
    thumbnail = db.Column(db.String)
    description = db.Column(db.String)
    categories = db.Column(db.ARRAY(db.Text()))
    tags = db.Column(db.String)
    duration = db.Column(
        db.Numeric(asdecimal=False)
    )  # asdecimal=False here because the JSON serializer has trouble with Decimal type (and this converts it to Floats)
    age_limit = db.Column(db.Integer)
    webpage_url = db.Column(db.String)
    view_count = db.Column(
        db.Numeric(asdecimal=False)
    )  # asdecimal=False here because the JSON serializer has trouble with Decimal type (and this converts it to Floats)
    like_count = db.Column(
        db.Numeric(asdecimal=False)
    )  # asdecimal=False here because the JSON serializer has trouble with Decimal type (and this converts it to Floats)
    dislike_count = db.Column(
        db.Numeric(asdecimal=False)
    )  # asdecimal=False here because the JSON serializer has trouble with Decimal type (and this converts it to Floats)
    average_rating = db.Column(
        db.Numeric(asdecimal=False)
    )  # asdecimal=False here because the JSON serializer has trouble with Decimal type (and this converts it to Floats)
    is_live = db.Column(db.Boolean)
    display_id = db.Column(db.String)
    format = db.Column(db.String)
    format_id = db.Column(db.String)
    width = db.Column(
        db.Numeric(asdecimal=False)
    )  # asdecimal=False here because the JSON serializer has trouble with Decimal type (and this converts it to Floats)
    height = db.Column(
        db.Numeric(asdecimal=False)
    )  # asdecimal=False here because the JSON serializer has trouble with Decimal type (and this converts it to Floats)
    resolution = db.Column(db.String)
    fps = db.Column(
        db.Numeric(asdecimal=False)
    )  # asdecimal=False here because the JSON serializer has trouble with Decimal type (and this converts it to Floats)
    fulltitle = db.Column(db.Boolean)
    error = db.Column(db.Boolean)
    updated_at = db.Column(db.DateTime(True), server_default=text("now()"))
    video_unavailable = db.Column(
        db.Boolean, nullable=False, server_default=text("false")
    )
    video_private = db.Column(db.Boolean)
    updated_at = db.Column(db.DateTime(True), server_default=text("now()"))
    google_ad_creative = relationship(
        "GoogleAdCreative", back_populates="youtube_video"
    )
    values = relationship("InferenceValue", back_populates="youtube_ad")
    observed_youtube_ad = relationship(
        "ObservedYoutubeAd", back_populates="youtube_video"
    )
    youtube_video_sub = relationship("YoutubeVideoSub", back_populates="youtube_video")

    def __repr__(self):
        return "YoutubeVideo(id={}, uploader={}, upload_date={}, title={}, description={})".format(
            self.id,
            self.uploader,
            self.upload_date,
            self.title,
            str(self.description).replace("\n", " ")[0:300]
        )

@dataclasses.dataclass
class YoutubeVideoSub(Base):
    __tablename__ = "youtube_video_subs"
    __table_args__ = {'schema': 'googleads'}

    id: str
    subs: str
    subtitle_lang: str
    asr: bool

    id = db.Column(ForeignKey("googleads.youtube_videos.id"), primary_key=True, nullable=False)
    subs = db.Column(db.Text)
    subtitle_lang = db.Column(db.String)
    asr = db.Column(db.Boolean)
    youtube_video = relationship("YoutubeVideo", back_populates="youtube_video_sub")



@dataclasses.dataclass
class Model(Base):
    __tablename__ = "models"
    __table_args__ = {'schema': 'googleads'}


    model_id: int
    created_at: datetime.datetime
    location: str
    model_name: str

    model_id = db.Column(
        db.Integer,
        primary_key=True,
        server_default=text("nextval('models_model_id_seq'::regclass)"),
    )
    created_at = db.Column(db.DateTime(True), server_default=text("now()"))
    location = db.Column(db.Text)
    model_name = db.Column(db.Text)
    vocab_path = db.Column(db.Text)
    encoder_path = db.Column(db.Text)


@dataclasses.dataclass
class InferenceValue(Base):
    __tablename__ = "inference_values"
    __table_args__ = {'schema': 'googleads'}


    youtube_ad_id: int
    model_id: int
    value: float

    youtube_ad_id = db.Column(
        ForeignKey("googleads.youtube_videos.id"), primary_key=True, nullable=False
    )
    model_id = db.Column(
        ForeignKey("googleads.models.model_id"), primary_key=True, nullable=False
    )
    value = db.Column(Float)

    model = relationship("Model")
    youtube_ad = relationship("YoutubeVideo")

    def __repr__(self):
        return "<YoutubeVideo(id='%s', uploader='%s', title='%s')>" % (
            self.id,
            self.uploader,
            " ".join(self.title.split(",")[0:15]),
        )


@dataclasses.dataclass
class ObservedYoutubeAd(Base):
    __tablename__ = "youtube_ads"
    __table_args__ = {'schema': 'observations'}

    id: str
    title: str
    paid_for_by: str
    advertiser: str
    itemtype: str
    itemid: str
    platformitemid: str
    observedat: datetime.datetime
    hostvideoid: str
    hostvideourl: str
    hostvideochannelid: str
    hostvideoauthor: str
    hostvideotitle: str
    creative: str
    reasons: str
    lang: str

    id = db.Column(db.String(16), primary_key=True)
    title = db.Column(db.Text)
    paid_for_by = db.Column(db.Text)
    targeting_on = db.Column(db.Boolean)
    advertiser = db.Column(db.Text)
    itemtype = db.Column(db.Text)
    itemid = db.Column(db.Text)
    platformitemid = db.Column(db.Text, ForeignKey("googleads.youtube_videos.id"))
    observedat = db.Column(db.DateTime)
    hostvideoid = db.Column(db.Text)
    hostvideourl = db.Column(db.Text)
    hostvideochannelid = db.Column(db.Text)
    hostvideoauthor = db.Column(db.Text)
    hostvideotitle = db.Column(db.Text)
    creative = db.Column(db.Text)
    reasons = db.Column(db.Text)
    lang = db.Column(db.Text)

    video = db.Column(db.Boolean)
    time_of_day = db.Column(db.Boolean)
    general_location = db.Column(db.Boolean)
    activity = db.Column(db.Boolean)
    similarity = db.Column(db.Boolean)
    age = db.Column(db.Boolean)
    interests_estimation = db.Column(db.Boolean)
    general_location_estimation = db.Column(db.Boolean)
    gender = db.Column(db.Boolean)
    income_estimation = db.Column(db.Boolean)
    parental_status_estimation = db.Column(db.Boolean)
    websites_youve_visited = db.Column(db.Boolean)
    approximate_location = db.Column(db.Boolean)
    activity_eg_searches = db.Column(db.Boolean)
    website_topics = db.Column(db.Boolean)
    age_estimation = db.Column(db.Boolean)
    gender_estimation = db.Column(db.Boolean)
    homeownership_status_estimation = db.Column(db.Boolean)
    company_size_estimation = db.Column(db.Boolean)
    job_industry_estimation = db.Column(db.Boolean)
    marital_status_estimation = db.Column(db.Boolean)
    education_status_estimation = db.Column(db.Boolean)
    visit_to_advertisers_website_or_app = db.Column(db.Boolean)
    search_terms = db.Column(db.Boolean)

    youtube_video = relationship("YoutubeVideo", back_populates="observed_youtube_ad")

@dataclasses.dataclass
class RegionPopulation(Base):
    __tablename__ = "region_populations"
    __table_args__ = {'schema': 'googleads'}


    region: str
    region_abbr: str
    population: int

    region = db.Column(db.Text, primary_key=True)
    region_abbr = db.Column(db.Text)
    population = db.Column(db.Integer)

@dataclasses.dataclass
class GoogleFbCrosswalk(Base):
    __tablename__ = "google_fb_crosswalk"
    __table_args__ = {'schema': 'googleads'}

    page_owner: str
    google_advertiser_name: str
    match_key: str
    match_key_info: str

    page_owner = db.Column(db.Text, primary_key=True)
    google_advertiser_name = db.Column(db.String, ForeignKey("googleads.advertiser_stats.advertiser_name"), primary_key=True)
    match_key = db.Column(db.Text)
    match_key_info = db.Column(db.Text)


@dataclasses.dataclass
class AdLibraryReport(Base):
    __tablename__ = "ad_library_reports"
    __table_args__ = {'schema': 'fb_us_ads_2020'}

    ad_library_report_id: int
    report_date: datetime.date
    kind: str
    csv_fn: str
    zip_url: str
    loaded: bool
    geography: str
    loading: bool

    ad_library_report_id = db.Column(db.Integer, primary_key=True)
    report_date = db.Column(Date)
    kind = db.Column(db.Text)
    csv_fn = db.Column(db.Text)
    zip_url = db.Column(db.Text)
    loaded = db.Column(db.Boolean)
    geography = db.Column(db.Text)
    loading = db.Column(db.Boolean)


@dataclasses.dataclass
class AdLibraryReportPage(Base):
    __tablename__ = "ad_library_report_pages"
    __table_args__ = {'schema': 'fb_us_ads_2020'}

    ad_library_report_page_id: int
    ad_library_report_id: int
    page_id: int
    disclaimer: str
    amount_spent: int
    ads_count: int
    manual_correction_amount_spent: int


    ad_library_report_page_id = db.Column(db.Integer, primary_key=True)
    ad_library_report_id = db.Column(db.Integer, ForeignKey("fb_us_ads_2020.ad_library_reports.ad_library_report_id"))
    page_id = db.Column(db.Integer)
    disclaimer = db.Column(db.Text)
    amount_spent = db.Column(db.Integer)
    ads_count = db.Column(db.Integer)
    manual_correction_amount_spent = db.Column(db.Integer)

@dataclasses.dataclass
class LatestUsLifelongAdLibraryReportPage(Base):
    """
    this is a materialized view!


    CREATE MATERIALIZED VIEW mv_latest_lifelong_US_ad_library_report_pages AS
    SELECT ad_library_report_pages.*, trim(replace(replace(replace(replace(replace(upper(disclaimer), ',', ''), '.', ''), ' LLC', ''), ' INC', ''), ' ',  '')) clean_disclaimer
     FROM            fb_us_ads_2020.ad_library_report_pages
     join            fb_us_ads_2020.ad_library_reports
     ON              fb_us_ads_2020.ad_library_reports.ad_library_report_id = fb_us_ads_2020.ad_library_report_pages.ad_library_report_id
     WHERE           fb_us_ads_2020.ad_library_reports.report_date = (
                SELECT Max(fb_us_ads_2020.ad_library_reports.report_date) AS max_2
                FROM   fb_us_ads_2020.ad_library_reports
                WHERE  fb_us_ads_2020.ad_library_reports.kind = 'lifelong'
                AND    fb_us_ads_2020.ad_library_reports.loaded = TRUE
                AND    fb_us_ads_2020.ad_library_reports.geography = 'US')
     AND             fb_us_ads_2020.ad_library_reports.kind = 'lifelong'
     AND             fb_us_ads_2020.ad_library_reports.loaded = TRUE
     AND             fb_us_ads_2020.ad_library_reports.geography = 'US'
    """
    __tablename__ = "mv_latest_lifelong_us_ad_library_report_pages"
    __table_args__ = {'schema': 'fb_us_ads_2020'}
    ad_library_report_page_id: int
    ad_library_report_id: int
    page_id: int
    disclaimer: str
    amount_spent: int
    ads_count: int
    manual_correction_amount_spent: int
    clean_disclaimer: str


    ad_library_report_page_id = db.Column(db.Integer, primary_key=True)
    ad_library_report_id = db.Column(db.Integer, ForeignKey("fb_us_ads_2020.ad_library_reports.ad_library_report_id"))
    page_id = db.Column(db.Integer)
    disclaimer = db.Column(db.Text)
    amount_spent = db.Column(db.Integer)
    ads_count = db.Column(db.Integer)
    manual_correction_amount_spent = db.Column(db.Integer)
    clean_disclaimer = db.Column(db.Text)
