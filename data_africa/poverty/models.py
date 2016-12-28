from data_africa.database import db
from data_africa.core.models import BaseModel
from data_africa.attrs.consts import ALL, ADM0, ADM1, IRR, RFD, OVERALL
from data_africa.attrs.consts import LATEST_BY_GEO
from data_africa.spatial.models import PovertyXWalk
from data_africa.attrs.models import Geo
from sqlalchemy.orm import column_property

from sqlalchemy import and_, or_
from sqlalchemy import tuple_
from sqlalchemy.sql import func, select

# aliased_xwalk = aliased(PovertyXWalk)


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
            selector = cls.query.with_entities(func.max(cls.year), cls.poverty_geo).group_by(cls.poverty_geo)
            return tuple_(cls.year, cls.poverty_geo).in_(selector)


    @classmethod
    def get_supported_levels(cls):
        return {
            "year": [ALL, LATEST_BY_GEO],
            "poverty_geo": [ALL, ADM0, ADM1],
            "geo": [ALL, ADM0, ADM1],
        }

class Survey(BasePoverty):
    __tablename__ = "survey"
    median_moe = 0

    year = db.Column(db.Integer(), primary_key=True)
    poverty_geo = db.Column(db.String(), primary_key=True)
    geo = column_property(PovertyXWalk.geo)
    sevpov_ppp1 = db.Column(db.Float)
    sevpov_ppp2 = db.Column(db.Float)
    povgap_ppp1 = db.Column(db.Float)
    povgap_ppp2 = db.Column(db.Float)
    gini = db.Column(db.Float)
    hc_poor1 = db.Column(db.Float)
    hc_poor2 = db.Column(db.Float)

    @staticmethod
    def crosswalk():
        cond = PovertyXWalk.poverty_geo == Survey.poverty_geo
        involved_tables = (PovertyXWalk, Survey)
        return [involved_tables, cond]
        # qry = qry.join(aliased_xwalk, aliased_xwalk.poverty_geo == Survey.poverty_geo)
        # if "poverty_geo" in api_obj.vars_and_vals:
            # pov_geos = api_obj.vars_and_vals["poverty_geo"].split(",")
            # qry = qry.filter(or_(PovertyXWalk.geo.in_(pov_geos), PovertyXWalk.poverty_geo.in_(pov_geos)))
        # return qry


poverty_models = [Survey]
