from data_africa.database import db
from data_africa.core.models import BaseModel
from data_africa.attrs.consts import ALL, ADM0, ADM1, LATEST_BY_GEO


class BaseClimate(db.Model, BaseModel):
    __abstract__ = True
    __table_args__ = {"schema": "climate", "extend_existing": True}
    source_title = 'Climate Data'
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
    def get_supported_levels(cls):
        return {
            "geo": [ALL, ADM0, ADM1],
            "year": [ALL, LATEST_BY_GEO]
        }


class Rainfall(BaseClimate):
    __tablename__ = "rainfall"
    median_moe = 0
    year = db.Column(db.Integer, primary_key=True)
    geo = db.Column(db.String(), primary_key=True)

    start_year = db.Column(db.Integer)
    cropland_total_ha = db.Column(db.Float)
    rainfall_awa_mm = db.Column(db.Float)
    cropland_rainfallCVgt20pct_pct = db.Column(db.Float)
    cropland_rainfallCVgt20pct_ha = db.Column(db.Float)
    cropland_rainfallCVgt30pct_pct = db.Column(db.Float)
    cropland_rainfallCVgt30pct_ha = db.Column(db.Float)


climate_models = [Rainfall]
