from data_africa.database import db
from data_africa.core.models import BaseModel
from data_africa.attrs.consts import ALL, ADM0, ADM1
from data_africa.attrs.consts import LATEST_BY_GEO, GENDER, RURAL
from data_africa.spatial.models import PovertyXWalk
from sqlalchemy.orm import column_property

from sqlalchemy import tuple_
from sqlalchemy.sql import func


class BasePoverty(db.Model, BaseModel):
    __abstract__ = True
    __table_args__ = {"schema": "poverty", "extend_existing": True}
    source_title = 'Poverty Survey'
    source_link = 'http://www.harvestchoice.org/'
    source_org = 'IFPRI'

    sevpov_ppp1 = db.Column(db.Float)
    sevpov_ppp2 = db.Column(db.Float)
    povgap_ppp1 = db.Column(db.Float)
    povgap_ppp2 = db.Column(db.Float)
    hc_poor1 = db.Column(db.Float)
    hc_poor2 = db.Column(db.Float)
    gini = db.Column(db.Float)

    @classmethod
    def geo_filter(cls, level):
        if level == ALL:
            return True
        elif level == ADM0:
            return cls.poverty_geo.startswith("040")
        elif level == ADM1:
            return cls.poverty_geo.startswith("050")

    @classmethod
    def year_filter(cls, level):
        if level == ALL:
            return True
        elif level == LATEST_BY_GEO:
            max_year_col = func.max(cls.year)
            selector = cls.query.with_entities(max_year_col, cls.poverty_geo)
            selector = selector.group_by(cls.poverty_geo)
            return tuple_(cls.year, cls.poverty_geo).in_(selector)

    @classmethod
    def get_supported_levels(cls):
        return {
            "year": [ALL, LATEST_BY_GEO],
            "poverty_geo": [ALL, ADM0, ADM1],
            "geo": [ALL, ADM0, ADM1],
        }

    @classmethod
    def crosswalk(cls):
        cond = PovertyXWalk.poverty_geo == cls.poverty_geo
        involved_tables = (PovertyXWalk, cls)
        return [involved_tables, cond]


class Survey(BasePoverty):
    __tablename__ = "survey"
    median_moe = 0

    year = db.Column(db.Integer(), primary_key=True)
    poverty_geo = db.Column(db.String(), primary_key=True)
    geo = column_property(PovertyXWalk.geo)


class SurveyByGender(BasePoverty):
    __tablename__ = "survey_by_gender"
    median_moe = 2

    year = db.Column(db.Integer(), primary_key=True)
    gender = db.Column(db.String(), primary_key=True)
    poverty_geo = db.Column(db.String(), primary_key=True)
    geo = column_property(PovertyXWalk.geo)

    @classmethod
    def get_supported_levels(cls):
        base_levels = super(SurveyByGender, cls).get_supported_levels()
        return dict(base_levels, **{GENDER: ALL})


class SurveyUrbanRural(BasePoverty):
    __tablename__ = "survey_urban_rural"
    median_moe = 2

    year = db.Column(db.Integer(), primary_key=True)
    residence = db.Column(db.String(), primary_key=True)
    poverty_geo = db.Column(db.String(), primary_key=True)
    geo = column_property(PovertyXWalk.geo)

    @classmethod
    def get_supported_levels(cls):
        base_levels = super(SurveyUrbanRural, cls).get_supported_levels()
        return dict(base_levels, **{RURAL: ALL})


poverty_models = [Survey, SurveyByGender, SurveyUrbanRural]
