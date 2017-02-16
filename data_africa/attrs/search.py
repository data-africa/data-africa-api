from data_africa.attrs.models import Geo
from sqlalchemy import func, or_


def query(query_args):
    q = query_args.get('q', '')
    limit = int(query_args.get('limit', 10))
    offset = int(query_args.get('offset', 0))
    sumlevel = query_args.get('sumlevel', None)
    excluded = ['050AF0017065246', '050AF0015265268']
    focus_countries = ["040AF00094", "040AF00253", "040AF00170", "040AF00217", "040AF00042", "040AF00152", "040AF00270", "040AF00257", "040AF00079", "040AF00205", "040AF00182", "040AF00133"];
    adm0s = [int(country[-3:]) for country in focus_countries]
    qry = Geo.query.filter(Geo.name.ilike("%{}%".format(q)))
    cond = or_(Geo.id.in_(focus_countries), Geo.adm0_id.in_(adm0s))
    qry = qry.filter(cond)
    qry = qry.filter(~Geo.id.in_(excluded))
    if sumlevel:
        qry = qry.filter(Geo.level == sumlevel)
    qry = qry.order_by(Geo.level, func.levenshtein(Geo.name, q))
    qry = qry.limit(limit).offset(offset)
    return qry.all()
