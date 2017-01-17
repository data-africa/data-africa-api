def get_columns(tbl):
    return tbl.__mapper__.column_attrs


def str_tbl_columns(tbl):
    return [str(c.key) for c in tbl.__mapper__.column_attrs]
