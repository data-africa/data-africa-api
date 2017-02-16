from data_africa.database import db
from data_africa.core.models import BaseModel
from data_africa.attrs.consts import ALL, ADM0, ADM1
from data_africa.attrs.consts import LATEST_BY_GEO, GENDER, RESIDENCE
from data_africa.attrs.consts import POVERTY_LEVEL, MALE, FEMALE
from data_africa.attrs.consts import PPP1, PPP2, URBAN, RURAL
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


class PovertyValues(db.Model):
    __abstract__ = True
    sevpov = db.Column(db.Float)
    povgap = db.Column(db.Float)
    hc = db.Column(db.Float)


class Survey_Yg(BasePoverty):
    __tablename__ = "survey_yg"
    median_moe = 1

    year = db.Column(db.Integer(), primary_key=True)
    poverty_geo = db.Column(db.String(), primary_key=True)
    geo = column_property(PovertyXWalk.geo)
    gini = db.Column(db.Float)


class Survey_Ygl(BasePoverty, PovertyValues):
    __tablename__ = "survey_ygl"
    median_moe = 2

    year = db.Column(db.Integer(), primary_key=True)
    poverty_geo = db.Column(db.String(), primary_key=True)
    poverty_level = db.Column(db.String(), primary_key=True)
    geo = column_property(PovertyXWalk.geo)

    @classmethod
    def get_supported_levels(cls):
        base_levels = super(Survey_Ygl, cls).get_supported_levels()
        return dict(base_levels, **{POVERTY_LEVEL: [ALL, PPP1, PPP2]})


class Survey_Ygg(BasePoverty):
    __tablename__ = "survey_ygg"
    median_moe = 2

    year = db.Column(db.Integer(), primary_key=True)
    gender = db.Column(db.String(), primary_key=True)
    poverty_geo = db.Column(db.String(), primary_key=True)
    geo = column_property(PovertyXWalk.geo)
    gini = db.Column(db.Float)

    @classmethod
    def get_supported_levels(cls):
        base_levels = super(Survey_Ygg, cls).get_supported_levels()
        return dict(base_levels, **{GENDER: [ALL, MALE, FEMALE]})


class Survey_Yggl(BasePoverty, PovertyValues):
    __tablename__ = "survey_yggl"
    median_moe = 3

    year = db.Column(db.Integer(), primary_key=True)
    gender = db.Column(db.String(), primary_key=True)
    poverty_geo = db.Column(db.String(), primary_key=True)
    poverty_level = db.Column(db.String(), primary_key=True)
    geo = column_property(PovertyXWalk.geo)

    @classmethod
    def get_supported_levels(cls):
        base_levels = super(Survey_Yggl, cls).get_supported_levels()
        return dict(base_levels, **{GENDER: [ALL, MALE, FEMALE], POVERTY_LEVEL: [ALL, PPP1, PPP2]})


class Survey_Ygr(BasePoverty):
    __tablename__ = "survey_ygr"
    median_moe = 2

    year = db.Column(db.Integer(), primary_key=True)
    residence = db.Column(db.String(), primary_key=True)
    poverty_geo = db.Column(db.String(), primary_key=True)
    geo = column_property(PovertyXWalk.geo)
    gini = db.Column(db.Float)

    @classmethod
    def get_supported_levels(cls):
        base_levels = super(Survey_Ygr, cls).get_supported_levels()
        return dict(base_levels, **{RESIDENCE: [ALL, URBAN, RURAL]})


class Survey_Ygrl(BasePoverty, PovertyValues):
    __tablename__ = "survey_ygrl"
    median_moe = 3

    year = db.Column(db.Integer(), primary_key=True)
    residence = db.Column(db.String(), primary_key=True)
    poverty_geo = db.Column(db.String(), primary_key=True)
    poverty_level = db.Column(db.String(), primary_key=True)
    geo = column_property(PovertyXWalk.geo)

    @classmethod
    def get_supported_levels(cls):
        base_levels = super(Survey_Ygrl, cls).get_supported_levels()
        return dict(base_levels, **{POVERTY_LEVEL: [ALL, PPP1, PPP2], RESIDENCE: [ALL, URBAN, RURAL]})


poverty_models = [Survey_Yg, Survey_Ygl,
                  Survey_Ygg, Survey_Yggl,
                  Survey_Ygr, Survey_Ygrl]
