from data_africa.database import db
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from sqlalchemy import or_, asc
from data_africa.attrs.consts import OR
from sqlalchemy.sql import text


attr_map = {}

def register(cls):
    attr_map[cls.__tablename__] = cls

class BaseAttr(db.Model):
    __abstract__ = True
    __table_args__ = {"schema": "attrs"}
    id = db.Column(db.String(10), primary_key=True)
    name = db.Column(db.String())
    HEADERS = ["id", "name"]

    @classmethod
    def parents(cls, attr_id):
        raise Exception("Not implemented!")

    @classmethod
    def children(cls, attr_id):
        raise Exception("Not implemented!")

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
        return [self.id, self.name, self.image_link, self.image_author, self.url_name]

@register
class Crop(BaseAttr):
    __tablename__ = 'crop'

    parent = db.Column(db.String)

@register
class Adm0(BaseAttr):
    __tablename__ = 'geo_adm0'

@register
class Adm1(BaseAttr):
    __tablename__ = 'geo_adm1'

@register
class WaterSupply(BaseAttr):
    __tablename__ = 'water_supply'

def get_mapped_attrs():
    return attr_map
