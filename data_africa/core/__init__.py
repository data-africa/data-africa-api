from sqlalchemy.orm.attributes import InstrumentedAttribute

def get_columns(tbl):
    return tbl.__mapper__.column_attrs
