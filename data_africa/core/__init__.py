from sqlalchemy.orm.attributes import InstrumentedAttribute

def get_columns(tbl):
    return tbl.__table__.columns
