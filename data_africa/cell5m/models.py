from data_africa.database import db
from data_africa.core.models import BaseModel
from data_africa.attrs.consts import ALL, ADM0

from sqlalchemy.sql import func

class BaseCell5M(db.Model, BaseModel):
    __abstract__ = True
    __table_args__ = {"schema": "crops", "extend_existing": True}
    source_title = 'CELL5m'
    source_link = 'http://www.harvestchoice.org/'
    source_org = 'IFPRI'

class HarvestedArea_Adm0(BaseCell5M):
    __tablename__ = "harvested_area_adm0"
    median_moe = 0

    year = db.Column(db.Integer(), primary_key=True)
    adm0_id = db.Column(db.String(), primary_key=True)
    crop = db.Column(db.String(), primary_key=True)

    harvested_area = db.Column(db.Integer())

    @classmethod
    def get_supported_levels(cls):
        return {
            "adm0_id": [ALL],
            "crop": [ALL, 'irrigated', 'rainfed'],
        }

    @classmethod
    def crop_filter(cls, level):
        # TODO tidy the data!
        if level == ALL:
            return ~cls.crop.endswith("_i_h") & ~cls.crop.endswith("_r_h")
        elif level == 'irrigated':
            return cls.crop.endswith("_i_h")
        elif level == 'rainfed':
            return cls.crop.endswith("_r_h")
