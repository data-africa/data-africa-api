from data_africa.database import db
from data_africa.core.models import BaseModel
from data_africa.attrs.consts import ALL, ADM0, ADM1, IRR, RFD, OVERALL
from data_africa.spatial.models import PovertyXWalk
from data_africa.attrs.models import Geo

from sqlalchemy import and_
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
            return cls.geo.startswith("040")
        elif level == ADM1:
            return cls.geo.startswith("050")

    @classmethod
    def year_filter(cls, level):
        raise Exception("figre me out!")
    #     if level == ALL:

class Survey(BasePoverty):
    __tablename__ = "survey"
    median_moe = 0

    year = db.Column(db.Integer(), primary_key=True)
    poverty_geo = db.Column(db.String(), primary_key=True)

    sevpov_ppp1 = db.Column(db.String)

    @staticmethod
    def crosswalk(api_obj, qry):
        qry = qry.join(PovertyXWalk, PovertyXWalk.poverty_geo == Survey.poverty_geo)
        if "poverty_geo" in api_obj.vars_and_vals:
            pov_geos = api_obj.vars_and_vals["poverty_geo"].split(",")
            qry = qry.filter(or_(PovertyXWalk.geo.in_(pov_geos), PovertyXWalk.poverty_geo.in_(pov_geos)))
        return qry

    @classmethod
    def get_supported_levels(cls):
        return {
            "poverty_geo": [ALL, ADM0, ADM1],
        }

poverty_models = [Survey]
