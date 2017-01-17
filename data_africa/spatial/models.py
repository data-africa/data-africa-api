from data_africa.database import db
from data_africa.core.models import BaseModel
from data_africa.attrs.consts import ALL, ADM0, ADM1
from data_africa.attrs.models import Geo, PovertyGeo


class BaseSpatial(db.Model, BaseModel):
    __abstract__ = True
    __table_args__ = {"schema": "spatial", "extend_existing": True}
    source_title = 'Spatial Data'
    source_link = ''
    source_org = 'IFPRI'

    st_area = db.Column(db.Float())
    pct_overlap = db.Column(db.Float())


class PovertyXWalk(BaseSpatial):
    __tablename__ = "pov_geo_crosswalk"
    median_moe = 0

    poverty_geo = db.Column(db.String, db.ForeignKey(PovertyGeo.id),
                            primary_key=True)
    geo = db.Column(db.String, db.ForeignKey(Geo.id), primary_key=True)

    @classmethod
    def get_supported_levels(cls):
        return {
            "poverty_geo": [ALL, ADM0, ADM1],
            "geo": [ALL, ADM0, ADM1],
        }


class DHSXWalk(BaseSpatial):
    __tablename__ = "dhs_geo_crosswalk"
    median_moe = 0

    dhs_geo = db.Column(db.String, primary_key=True)
    geo = db.Column(db.String, db.ForeignKey(Geo.id), primary_key=True)

    @classmethod
    def get_supported_levels(cls):
        return {
            "poverty_geo": [ALL, ADM0, ADM1],
            "geo": [ALL, ADM0, ADM1],
        }
