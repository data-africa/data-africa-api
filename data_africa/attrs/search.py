from data_africa.attrs.models import Geo
from sqlalchemy import func


def query(query_args):
    q = query_args.get('q', '')
    limit = int(query_args.get('limit', 10))
    offset = int(query_args.get('offset', 0))
    # sumlevel = query_args.get('sumlevel', None)

    qry = Geo.query.filter(func.levenshtein(Geo.name, q) < 4)
    qry = qry.with_entities(Geo.id, Geo.name)
    qry = qry.order_by(func.levenshtein(Geo.name, q))
    qry = qry.limit(limit).offset(offset)
    data = [[attr_id, attr_name] for attr_id, attr_name in qry]
    headers = ["id", "name"]
    return {"data": data, "headers": headers}
