from data_africa.database import db
from data_africa.core.models import BaseModel
from data_africa.attrs.consts import ALL, ADM0, ADM1, WASTED, STUNTED, UNDERWEIGHT
from data_africa.attrs.consts import LATEST_BY_GEO, GENDER, RESIDENCE
from data_africa.attrs.consts import URBAN, RURAL, MALE, FEMALE, MODERATE, SEVERE
from data_africa.spatial.models import DHSXWalk
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
    def gender_filter(cls, level):
        if level == ALL:
            return True
        else:
            return cls.gender == level

    @classmethod
    def condition_filter(cls, level):
        if level == ALL:
            return True
        else:
            return cls.condition == level

    @classmethod
    def severity_filter(cls, level):
        if level == ALL:
            return True
        else:
            return cls.severity == level

    @classmethod
    def dhs_geo_filter(cls, level):
        if level == ALL:
            return True
        elif level == ADM0:
            return cls.dhs_geo.startswith("040")
        elif level == ADM1:
            return cls.dhs_geo.startswith("050")

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
            "severity": [ALL, MODERATE, SEVERE],
            "condition": [ALL, WASTED, STUNTED, UNDERWEIGHT],
        }

    @staticmethod
    def crosswalk_cond(api_obj):
        return "geo" in list(api_obj.shows_and_levels.keys()) + api_obj.vars_needed

    @classmethod
    def crosswalk(cls):
        return DHSXWalk


class Conditions(BaseDHS):
    __tablename__ = "conditions"
    median_moe = 1

    year = db.Column(db.Integer, primary_key=True)
    dhs_geo = db.Column(db.String, primary_key=True)
    condition = db.Column(db.String, primary_key=True)
    severity = db.Column(db.String, primary_key=True)
    geo = column_property(DHSXWalk.geo)

    proportion_of_children = db.Column(db.Float)


class ConditionsGender(BaseDHS):
    __tablename__ = "conditions_gender"
    median_moe = 2

    year = db.Column(db.Integer, primary_key=True)
    dhs_geo = db.Column(db.String, primary_key=True)
    condition = db.Column(db.String, primary_key=True)
    severity = db.Column(db.String, primary_key=True)
    gender = db.Column(db.String, primary_key=True)
    proportion_of_children = db.Column(db.Float)
    geo = column_property(DHSXWalk.geo)

    @classmethod
    def get_supported_levels(cls):
        base_levels = super(ConditionsGender, cls).get_supported_levels()
        return dict(base_levels, **{GENDER: [ALL, MALE, FEMALE]})


class ConditionsResidence(BaseDHS):
    __tablename__ = "conditions_residence"
    median_moe = 2

    year = db.Column(db.Integer, primary_key=True)
    dhs_geo = db.Column(db.String, primary_key=True)
    condition = db.Column(db.String, primary_key=True)
    severity = db.Column(db.String, primary_key=True)
    residence = db.Column(db.String, primary_key=True)

    geo = column_property(DHSXWalk.geo)
    proportion_of_children = db.Column(db.Float)

    @classmethod
    def get_supported_levels(cls):
        base_levels = super(ConditionsResidence, cls).get_supported_levels()
        return dict(base_levels, **{RESIDENCE: [ALL, URBAN, RURAL]})


dhs_models = [Conditions, ConditionsGender, ConditionsResidence]
