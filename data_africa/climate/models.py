from data_africa.database import db
from data_africa.core.models import BaseModel
from data_africa.attrs.consts import ALL, ADM0, ADM1


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
        }


class Rainfall(BaseClimate):
    __tablename__ = "rainfall"
    median_moe = 0

    geo = db.Column(db.String(), primary_key=True)

    rainfall_awa_mm = db.Column(db.Float)
    cropland_rainfallCVgt20pct_pct = db.Column(db.Float)
    cropland_rainfallCVgt30pct_ha = db.Column(db.Float)


climate_models = [Rainfall]
