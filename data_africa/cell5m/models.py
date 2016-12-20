from data_africa.database import db
from data_africa.core.models import BaseModel
from data_africa.attrs.consts import ALL, ADM0, ADM1, IRR, RFD

from sqlalchemy.sql import func

class BaseCell5M(db.Model, BaseModel):
    __abstract__ = True
    __table_args__ = {"schema": "crops", "extend_existing": True}
    source_title = 'CELL5m'
    source_link = 'http://www.harvestchoice.org/'
    source_org = 'IFPRI'

    @classmethod
    def crop_filter(cls, level):
        # TODO tidy the data!
        if level == ALL:
            return ~cls.crop.endswith("_i_h") & ~cls.crop.endswith("_r_h")
        elif level == 'irrigated':
            return cls.crop.endswith("_i_h")
        elif level == 'rainfed':
            return cls.crop.endswith("_r_h")

class HarvestedArea(BaseCell5M):
    __tablename__ = "harvested_area"
    median_moe = 0

    year = db.Column(db.Integer(), primary_key=True)
    geo = db.Column(db.String(), primary_key=True)
    crop = db.Column(db.String(), primary_key=True)
    water_supply = db.Column(db.String(), primary_key=True)

    harvested_area = db.Column(db.Integer())

    @classmethod
    def get_supported_levels(cls):
        return {
            "geo": [ALL, ADM0, ADM1],
            "crop": [ALL, IRR, RFD],
        }

class ValueOfProduction(BaseCell5M):
    __tablename__ = "value_production"
    median_moe = 0

    year = db.Column(db.Integer(), primary_key=True)
    geo = db.Column(db.String(), primary_key=True)
    crop = db.Column(db.String(), primary_key=True)

    value_of_production = db.Column(db.Integer())

    @classmethod
    def get_supported_levels(cls):
        return {
            "geo": [ALL, ADM0, ADM1],
            "crop": [ALL],
        }

cell5m_models = [HarvestedArea, ValueOfProduction]
