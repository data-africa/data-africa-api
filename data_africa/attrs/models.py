'''Attribute database models'''
from data_africa.database import db
from sqlalchemy.dialects import postgresql
from data_africa.core.models import BaseModel

attr_map = {}


def register(cls):
    attr_map[cls.__tablename__] = cls


class BaseAttr(db.Model):
    __abstract__ = True
    __table_args__ = {"schema": "attrs"}
    id = db.Column(db.String(10), primary_key=True)
    name = db.Column(db.String())
    HEADERS = ["id", "name"]

    def serialize(self):
        return {key: val for key, val in self.__dict__.items()
                if not key.startswith("_")}

    def data_serialize(self):
        return [self.id, self.name]

    def __repr__(self):
        return '<{}, id: {}, name: {}>'.format(self.__class__,
                                               self.id, self.name)


class ImageAttr(db.Model):
    __abstract__ = True
    image_link = db.Column(db.String)
    image_author = db.Column(db.String)
    url_name = db.Column(db.String)

    HEADERS = ["id", "name", "image_link", "image_author", "url_name"]

    def data_serialize(self):
        return [self.id, self.name, self.image_link, self.image_author,
                self.url_name]


class Crop(BaseAttr, BaseModel):
    __tablename__ = 'crop'
    median_moe = 0
    parent = db.Column(db.String)
    children = db.Column(postgresql.ARRAY(db.String))
    internal_id = db.Column(db.Integer)
    crop = db.Column(db.String())
    crop_parent = db.Column(db.String())
    crop_name = db.Column(db.String())
    @classmethod
    def get_supported_levels(cls):
        return {
            "crop": ['all', 'lowest']
        }
class PovertyGeo(BaseAttr):
    iso3 = db.Column(db.String)


class Geo(BaseAttr):
    __tablename__ = 'geo'
    adm0_id = db.Column(db.Integer)
    adm1_id = db.Column(db.Integer)
    iso3 = db.Column(db.String)
    level = db.Column(db.String)
    url_name = db.Column(db.String)
    parent_name = db.Column(db.String)

    def child_filter(self, tbl):
        if self.id.startswith("040"):
            target = '050AF' + self.id[5:]
            return tbl.geo.startswith(target)
        return True
# class DHSGeo(BaseAttr):
#     __tablename__ = 'geo'
#     adm0_id = db.Column(db.Integer)
#     adm1_id = db.Column(db.Integer)
#     iso3 = db.Column(db.String)


class WaterSupply(BaseAttr):
    __tablename__ = 'water_supply'


def get_mapped_attrs():
    return {"crop": Crop, "geo": Geo, "water_supply": WaterSupply,
            "poverty_geo": PovertyGeo}
