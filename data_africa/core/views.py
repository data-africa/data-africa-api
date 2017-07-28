from flask import Blueprint, request, jsonify

from data_africa.core import table_manager
from data_africa.core import join_api
from data_africa.core.models import ApiObject
from data_africa.core.exceptions import DataAfricaException
from data_africa.attrs.consts import ADM0, ADM1

mod = Blueprint('core', __name__, url_prefix='/api')

manager = table_manager.TableManager()


def show_attrs(attr_obj):
    attrs = attr_obj.query.all()
    data = [a.serialize() for a in attrs]
    return jsonify(data=data)


def build_api_obj(default_limit=None):
    show = request.args.get("show", "")
    sumlevel = request.args.get("sumlevel", "").lower()
    required = request.args.get("required", "")
    force = request.args.get("force", "")
    where = request.args.get("where", "")
    order = request.args.get("order", "")
    sort = request.args.get("sort", "")
    limit = request.args.get("limit", default_limit)
    offset = request.args.get("offset", None)
    exclude = request.args.get("exclude", None)
    inside = request.args.get("inside", None)
    neighbors = request.args.get("neighbors", None)
    if neighbors:
        neighbors = neighbors.split(",")
    if inside:
        inside = [raw.split(":") for raw in inside.split(",")]
    auto_crosswalk = request.args.get("auto_crosswalk", False)
    display_names = request.args.get("display_names", False)

    shows = show.split(",")
    sumlevels = sumlevel.split(",")
    if shows and not sumlevel:
        sumlevels = ["all" for show in shows]
    values = required.split(",") if required else []

    shows_and_levels = {val: sumlevels[idx] for idx, val in enumerate(shows)}

    variables = manager.possible_variables
    vars_and_vals = {var: request.args.get(var, None) for var in variables}
    vars_and_vals = {k: v for k, v in vars_and_vals.items() if v}

    vars_needed = list(vars_and_vals.keys()) + shows + values
    api_obj = ApiObject(vars_needed=vars_needed, vars_and_vals=vars_and_vals,
                        shows_and_levels=shows_and_levels, values=values,
                        where=where, force=force, order=order,
                        sort=sort, limit=limit, exclude=exclude,
                        auto_crosswalk=auto_crosswalk,
                        display_names=display_names,
                        offset=offset, inside=inside, neighbors=neighbors)
    return api_obj


@mod.route("/")
@mod.route("/v1/")
@mod.route("/csv/", defaults={'csv': True})
def api_view(csv=None):
    raise DataAfricaException("This API view is no longer supported.")


@mod.route("/join/")
@mod.route("/join/csv/", defaults={'csv': True})
def api_join_view(csv=None):
    api_obj = build_api_obj(default_limit=10000)
    if api_obj.limit and api_obj.limit > 80000:
        raise DataAfricaException("Limit parameter must be less than 80,000")
    tables, joins = manager.required_table_joins(api_obj)
    data = join_api.joinable_query(tables, joins, api_obj, manager.table_years,
                                   csv_format=csv)
    return data


@mod.route("/logic/")
def logic_view():
    api_obj = build_api_obj()
    table_list = manager.all_tables(api_obj)
    return jsonify(tables=[table.info(api_obj) for table in table_list])


@mod.route("/variables/")
def view_variables():
    '''show available data tables and contained variables'''
    shows = request.args.get("show", "").split(",")
    sumlevels = request.args.get("sumlevel", "").split(",")
    list_all = sumlevels == [""] and shows == [""]
    if sumlevels == [""]:
        sumlevels = ["all"] * len(shows)
    combos = zip(shows, sumlevels)
    results = {}
    for show, sumlevel in combos:
        for table in table_manager.registered_models:
            if list_all or all([table.can_show(show, sumlevel)]):
                results[table.full_name()] = table.col_strs(short_name=True)
    return jsonify(metadata=results)


@mod.route('/table/variables/')
def all_table_vars():
    '''show all available data tables and contained variables'''
    results = {table.full_name(): table.col_strs(short_name=True)
               for table in table_manager.registered_models}
    return jsonify(metadata=results)


@mod.route("/geo/variables/")
def geo_variables():
    '''show available data tables and contained variables'''
    geo_data = {}
    for table in table_manager.registered_models:
        if table.get_schema_name() == 'spatial':
            continue
        if table.can_show("geo", "adm0"):
            levels = table.get_supported_levels()
            if 'year' not in levels:
                levels['year'] = ['all']

            for col in table.measures(short_name=True):
                if col in ['year', 'start_year']: continue
                obj = {}
                obj["column"] = col
                obj["levels"] = [levels]

                if col in geo_data:
                    geo_data[col]["levels"] += [levels]
                else:
                    geo_data[col] = obj


    return jsonify(metadata=[res for res in geo_data.values()])


@mod.route("/years/")
def years_view():
    years_data = manager.table_years_set
    return jsonify(data=years_data)


@mod.route("/poverty/")
def pov_map_qry():
    from data_africa.database import db
    import sqlalchemy
    sumlevel = request.args.get("show", "adm0")
    lvl_filt = "040" if sumlevel == "adm0" else "050"
    poverty_level = request.args.get("poverty_level", "ppp2")
    adm0_parta = ", ga.*, ga.name as geo_name, ga.parent_name as geo_parent_name" if sumlevel == "adm0" else ""
    adm0_partb = """LEFT JOIN spatial.pov_xwalk2 x ON a.poverty_geo = x.poverty_geo
                 LEFT JOIN attrs.geo ga ON x.geo = ga.geo""" if sumlevel == "adm0" else ""
    sql = """SELECT a.*, attrs.* {}
             FROM poverty.survey_ygl a
             LEFT JOIN attrs.poverty_geo attrs ON attrs.poverty_geo = a.poverty_geo
             {}
             WHERE a.poverty_geo LIKE '{}%'
             AND poverty_level=:poverty_level
             AND year = (SELECT max(year) from poverty.survey_ygl b
             WHERE substr(a.poverty_geo, 6, 3) = substr(b.poverty_geo, 6, 3))
             AND substr(a.poverty_geo, 6, 3) in
             ('BFA',
            'ETH',
            'GHA',
            'KEN',
            'MWI',
            'MLI',
            'MOZ',
            'NGA',
            'RWA',
            'SEN',
            'TZA',
            'UGA',
            'ZMB')""".format(adm0_parta, adm0_partb, lvl_filt, poverty_level)
    results = db.engine.execute(sqlalchemy.text(sql), poverty_level=poverty_level)
    data = [(dict(row.items())) for row in results]
    return jsonify(data=data)


@mod.route("/health/")
def dhs_map_qry():
    from data_africa.database import db
    import sqlalchemy
    sumlevel = request.args.get("show", "adm0")
    lvl_filt = "040" if sumlevel == "adm0" else "050"
    severity = request.args.get("severity", "severe")
    condition = request.args.get("condition", "wasted")

    adm0_parta = ", ga.*, ga.name as geo_name, ga.parent_name as geo_parent_name" if sumlevel == "adm0" else ""
    adm0_partb = """LEFT JOIN spatial.dhs_xwalk_focus x ON a.dhs_geo = x.dhs_geo
                 LEFT JOIN attrs.geo ga ON x.geo = ga.geo""" if sumlevel == "adm0" else ""
    sql = """SELECT a.*, attrs.* {}
             FROM health.conditions a
             LEFT JOIN attrs.dhs_geo attrs ON attrs.dhs_geo = a.dhs_geo
             {}
             WHERE a.dhs_geo LIKE '{}%'
             AND severity=:severity
             AND condition=:condition
             AND year = (SELECT max(year) from health.conditions b
             WHERE substr(a.dhs_geo, 6, 2) = substr(b.dhs_geo, 6, 2))
             AND substr(a.dhs_geo, 6, 2) in
             ('NG',
            'BF',
            'GH',
            'ET',
            'ML',
            'MW',
            'TZ',
            'SN',
            'RW',
            'UG',
            'ZM',
            'MZ',
            'KE')""".format(adm0_parta, adm0_partb, lvl_filt)
    results = db.engine.execute(sqlalchemy.text(sql), severity=severity, condition=condition)
    data = [(dict(row.items())) for row in results]
    return jsonify(data=data)


@mod.route("/harvested_area/")
def ha_qry():
    from data_africa.database import db
    import sqlalchemy
    sumlevel = request.args.get("show", "adm0")
    lvl_filt = "040" if sumlevel == "adm0" else "050"

    sql = """SELECT a.*, ga.*, ga.name as geo_name, ga.parent_name as geo_parent_name
             FROM crops.area a
             LEFT JOIN attrs.geo ga ON ga.geo = a.geo
             WHERE a.geo LIKE '{}%'
             AND year = (SELECT max(year) from crops.area b WHERE a.geo = b.geo)
             AND ga.iso3 in ('BFA',
                'ETH',
                'GHA',
                'KEN',
                'MWI',
                'MLI',
                'MOZ',
                'NGA',
                'RWA',
                'SEN',
                'TZA',
                'UGA',
                'ZMB')""".format(lvl_filt)
    results = db.engine.execute(sqlalchemy.text(sql))
    data = [(dict(row.items())) for row in results]
    return jsonify(data=data)
