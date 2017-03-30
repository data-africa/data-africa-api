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
        focus_countries = ["040AF00155", "040AF00094", "040AF00253", "040AF00170", "040AF00217", "040AF00042", "040AF00152", "040AF00270", "040AF00257", "040AF00079", "040AF00205", "040AF00182", "040AF00133"];

        if level == ALL:
            return True
        elif level == ADM0:
            return cls.geo.in_(focus_countries)
        elif level == ADM1:
            adm1_conds = or_(*[cls.geo.startswith("050" + g[3:]) for g in focus_countries])
            return adm1_conds

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
