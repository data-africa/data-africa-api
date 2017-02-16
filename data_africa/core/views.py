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
