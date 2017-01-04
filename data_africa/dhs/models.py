from data_africa.database import db
from data_africa.core.models import BaseModel
from data_africa.attrs.consts import ALL, ADM0, ADM1
from data_africa.attrs.consts import LATEST_BY_GEO, GENDER, RURAL
from data_africa.spatial.models import PovertyXWalk, DHSXWalk
from sqlalchemy.orm import column_property

from sqlalchemy import tuple_
from sqlalchemy.sql import func


class BaseDHS(db.Model, BaseModel):
    __abstract__ = True
    __table_args__ = {"schema": "health", "extend_existing": True}
    source_title = 'Health Survey'
    source_link = 'http://www.harvestchoice.org/'
    source_org = 'IFPRI'

    @classmethod
    def dhs_geo_filter(cls, level):
        return cls.geo_filter(level)

    @classmethod
    def geo_filter(cls, level):
        if level == ALL:
            return True
        elif level == ADM0:
            return cls.dhs_geo.startswith("040")
        elif level == ADM1:
            return cls.dhs_geo.startswith("050")

    @classmethod
    def year_filter(cls, level):
        if level == ALL:
            return True
        elif level == LATEST_BY_GEO:
            max_year_col = func.max(cls.year)
            selector = cls.query.with_entities(max_year_col, cls.dhs_geo)
            selector = selector.group_by(cls.dhs_geo)
            return tuple_(cls.year, cls.dhs_geo).in_(selector)

    @classmethod
    def get_supported_levels(cls):
        return {
            "year": [ALL, LATEST_BY_GEO],
            "dhs_geo": [ALL, ADM0, ADM1],
            "geo": [ALL, ADM0, ADM1],
        }

    @classmethod
    def crosswalk(cls):
        cond = DHSXWalk.dhs_geo == cls.dhs_geo
        involved_tables = (DHSXWalk, cls)
        return [involved_tables, cond]


class HealthSurvey(BaseDHS):
    __tablename__ = "health_survey"
    median_moe = 2

    year = db.Column(db.Integer(), primary_key=True)
    dhs_geo = db.Column(db.String(), primary_key=True)
    geo = column_property(DHSXWalk.geo)
    condition = db.Column(db.String())
    gender = db.Column(db.String())
    severity = db.Column(db.String())
    proportion_of_children = db.Column(db.Float)


dhs_models = [HealthSurvey]
