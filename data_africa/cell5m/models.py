from data_africa.database import db
from data_africa.core.models import BaseModel
from data_africa.attrs.consts import ALL, ADM0, ADM1, IRR, RFD, OVERALL
from data_africa.attrs.models import Crop

from sqlalchemy.sql import func

class BaseCell5M(db.Model, BaseModel):
    __abstract__ = True
    __table_args__ = {"schema": "crops", "extend_existing": True}
    source_title = 'CELL5m'
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
    def crop_filter_join(cls, level):
        if level == ALL:
            return None
        elif level == 'lowest':
            return [Crop, Crop.internal_id != 999]

class HarvestedArea(BaseCell5M):
    __tablename__ = "harvested_area"
    median_moe = 0

    year = db.Column(db.Integer(), primary_key=True)
    geo = db.Column(db.String(), primary_key=True)
    crop = db.Column(db.String(), db.ForeignKey(Crop.id), primary_key=True)
    water_supply = db.Column(db.String(), primary_key=True)

    harvested_area = db.Column(db.Integer())

    @classmethod
    def get_supported_levels(cls):
        return {
            "geo": [ALL, ADM0, ADM1],
            "crop": [ALL, 'lowest'],
            "water_supply": [ALL, OVERALL, IRR, RFD],

        }

    @classmethod
    def water_supply_filter(cls, level):
        if level == ALL or not hasattr(cls, "water_supply"):
            return True
        else:
            return cls.water_supply == level

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
            "water_supply": [ALL, OVERALL],
        }

cell5m_models = [HarvestedArea, ValueOfProduction]
