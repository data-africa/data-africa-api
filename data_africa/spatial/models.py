from data_africa.database import db
from data_africa.core.models import BaseModel
from data_africa.attrs.consts import ALL

from sqlalchemy.sql import func

class BaseSpatial(db.Model, BaseModel):
    __abstract__ = True
    __table_args__ = {"schema": "spatial", "extend_existing": True}
    source_title = 'Spatial Data'
    source_link = ''
    source_org = 'IFPRI'

class PovertyXWalk(BaseSpatial):
    __tablename__ = "poverty_crosswalk"
    median_moe = 0

    iso3 = db.Column(db.String(), primary_key=True)
    svyl1cd = db.Column(db.String(), primary_key=True)
    adm0_code = db.Column(db.Integer(), primary_key=True)
    adm1_code = db.Column(db.Integer(), primary_key=True)
