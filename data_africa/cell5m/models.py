from sqlalchemy.orm import aliased
from sqlalchemy.ext.declarative import declared_attr

from data_africa.database import db
from data_africa.core.models import BaseModel
from data_africa.attrs.consts import ALL, ADM0, ADM1, WATER_SUPPLY, IRR, RFD
from data_africa.attrs.models import Crop, Geo


class BaseCell5M(db.Model, BaseModel):
    __abstract__ = True
    __table_args__ = {"schema": "crops", "extend_existing": True}
    source_title = 'CELL5m'
    source_link = 'http://www.harvestchoice.org/'
    source_org = 'IFPRI'

    @declared_attr
    def crop(cls):
        return db.Column(db.String(), db.ForeignKey(Crop.id), primary_key=True)

    @declared_attr
    def geo(cls):
        return db.Column(db.String(), db.ForeignKey(Geo.id), primary_key=True)

    @declared_attr
    def year(cls):
        return db.Column(db.Integer(), primary_key=True)

    @classmethod
    def geo_filter(cls, level):
        if level == ALL:
            return True
        elif level == ADM0:
            return cls.geo.startswith("040")
        elif level == ADM1:
            return cls.geo.startswith("050")

    @classmethod
    def water_supply_filter(cls, level):
        if level == ALL:
            return True
        else:
            return cls.water_supply == level

    @classmethod
    def crop_filter_join(cls, level):
        if level == ALL:
            return None
        elif level == 'lowest':
            AliasedCrop = aliased(Crop)
            return ['cropjoin', AliasedCrop, ~AliasedCrop.internal_id.in_([42, 999])]

    @classmethod
    def year_filter(cls, level):
        return True

    @classmethod
    def get_supported_levels(cls):
        return {
            "year": [ALL, 'latest_by_geo'],
            "geo": [ALL, ADM0, ADM1],
            "crop": [ALL, 'lowest']
        }


class WaterSupply(BaseCell5M):
    __abstract__ = True

    @declared_attr
    def water_supply(cls):
        return db.Column(db.String(), primary_key=True)

    @classmethod
    def water_supply_filter(cls, level):
        if level == ALL or not hasattr(cls, "water_supply"):
            return True
        else:
            return cls.water_supply == level

    @classmethod
    def get_supported_levels(cls):
        base_levels = super(WaterSupply, cls).get_supported_levels()
        return dict(base_levels, **{WATER_SUPPLY: [ALL, IRR, RFD]})


class HarvestedArea(BaseCell5M):
    __tablename__ = "area"
    median_moe = 1

    harvested_area = db.Column(db.Integer())


class HarvestedAreaBySupply(WaterSupply):
    __tablename__ = "area_by_supply"
    median_moe = 2

    harvested_area = db.Column(db.Integer())


class ValueOfProduction(BaseCell5M):
    __tablename__ = "value"
    median_moe = 1

    value_of_production = db.Column(db.Integer())


class ValueOfProductionBySupply(WaterSupply):
    __tablename__ = "value_by_supply"
    median_moe = 2

    value_of_production = db.Column(db.Integer())


cell5m_models = [HarvestedArea, HarvestedAreaBySupply,
                 ValueOfProduction, ValueOfProductionBySupply]
