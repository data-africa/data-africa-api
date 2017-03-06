from data_africa.database import db
from data_africa.core.models import BaseModel
from data_africa.attrs.consts import ALL, ADM0, ADM1
from data_africa.attrs.models import Geo, PovertyGeo
from geoalchemy2 import Geometry
from sqlalchemy.ext.hybrid import hybrid_property

class BaseSpatial(db.Model, BaseModel):
    __abstract__ = True
    __table_args__ = {"schema": "spatial", "extend_existing": True}
    source_title = 'Spatial Data'
    source_link = ''
    source_org = 'IFPRI'


class BaseXWalk(BaseSpatial):
    __abstract__ = True

    st_area = db.Column(db.Float())
    pct_overlap = db.Column(db.Float())


class PovertyXWalk(BaseXWalk):
    __tablename__ = "pov_xwalk"
    median_moe = 0

    poverty_geo = db.Column(db.String, db.ForeignKey(PovertyGeo.poverty_geo),
                            primary_key=True)
    geo = db.Column(db.String, db.ForeignKey(Geo.id), primary_key=True)

    @classmethod
    def get_supported_levels(cls):
        return {
            "poverty_geo": [ALL, ADM0, ADM1],
            "geo": [ALL, ADM0, ADM1],
        }


class DHSXWalk(BaseXWalk):
    __tablename__ = "dhs_xwalk_focus"
    median_moe = 0

    dhs_geo = db.Column(db.String, primary_key=True)
    geo = db.Column(db.String, db.ForeignKey(Geo.id), primary_key=True)

    @classmethod
    def get_supported_levels(cls):
        return {
            "dhs_geo": [ALL, ADM0, ADM1],
            "geo": [ALL, ADM0, ADM1],
        }

class Cell5M(BaseSpatial):
    __tablename__ = "cell5m_final"
    median_moe = 0

    geo = db.Column(db.String(), db.ForeignKey(Geo.id), primary_key=True)
    geom = db.Column(Geometry('POLYGON'))

class DHSGeo(BaseSpatial):
    __tablename__ = "dhs_geo_focus"
    median_moe = 0

    iso = db.Column(db.String(), primary_key=True)
    regcd = db.Column(db.String(), primary_key=True)
    svyyr = db.Column(db.Integer(), primary_key=True)
    geom = db.Column(Geometry('POLYGON'))

    @hybrid_property
    def dhs_geo(self):
        return "050HG" + self.iso + str(int(self.regcd)).zfill(3) + str(self.svyyr)
