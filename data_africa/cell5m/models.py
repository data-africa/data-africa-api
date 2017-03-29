from sqlalchemy.orm import aliased
from sqlalchemy.ext.declarative import declared_attr
from sqlalchemy import or_

from data_africa.database import db
from data_africa.core.models import BaseModel
from data_africa.attrs.consts import ALL, ADM0, ADM1, WATER_SUPPLY, IRR, RFD, LOWEST
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
        focus_countries = ["040AF00155", "040AF00094", "040AF00253", "040AF00170", "040AF00217", "040AF00042", "040AF00152", "040AF00270", "040AF00257", "040AF00079", "040AF00205", "040AF00182", "040AF00133"];

        if level == ALL:
            return True
        elif level == ADM0:
            return cls.geo.in_(focus_countries)
        elif level == ADM1:
            adm1_conds = or_(*[cls.geo.startswith("050" + g[3:]) for g in focus_countries])
            return adm1_conds

    @classmethod
    def crop_val_filter(cls, level):
        if level == ALL:
            return None
        elif level == LOWEST:
            return ['rest', 'bapl', 'cere', 'coff', 'frui', 'mill', 'puls']

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
        elif level == LOWEST:
            return cls.water_supply != 'overall'
        else:
            return cls.water_supply == level

    @classmethod
    def get_supported_levels(cls):
        base_levels = super(WaterSupply, cls).get_supported_levels()
        return dict(base_levels, **{WATER_SUPPLY: [ALL, IRR, RFD, LOWEST]})


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
