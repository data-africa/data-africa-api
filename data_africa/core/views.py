from flask import Blueprint, request, jsonify

from data_africa.core import table_manager
from data_africa.core import join_api
from data_africa.core.models import ApiObject
from data_africa.core.crosswalker import crosswalk
from data_africa.core.exceptions import DataAfricaException


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
    auto_crosswalk = request.args.get("auto_crosswalk", False)
    display_names = request.args.get("display_names", False)

    shows = show.split(",")
    sumlevels = sumlevel.split(",")
    if shows and not sumlevel:
        sumlevels = ["all" for show in shows]
    values = required.split(",") if required else []

    shows_and_levels = {val:sumlevels[idx] for idx, val in enumerate(shows)}

    variables = manager.possible_variables
    vars_and_vals = {var:request.args.get(var, None) for var in variables}
    vars_and_vals = {k:v for k,v in vars_and_vals.items() if v}


    vars_needed = vars_and_vals.keys() + shows + values
    api_obj = ApiObject(vars_needed=vars_needed, vars_and_vals=vars_and_vals,
                        shows_and_levels=shows_and_levels, values=values,
                        where=where, force=force, order=order,
                        sort=sort, limit=limit, exclude=exclude,
                        auto_crosswalk=auto_crosswalk,
                        display_names=display_names,
                        offset=offset)
    return api_obj

@mod.route("/")
@mod.route("/v1/")
@mod.route("/csv/", defaults={'csv': True})
def api_view(csv=None):
    raise Exception("deprecated!")

@mod.route("/join/")
@mod.route("/join/csv/", defaults={'csv': True})
def api_join_view(csv=None):
    api_obj = build_api_obj(default_limit=10000)
    if api_obj.limit and api_obj.limit > 80000:
        raise DataAfricaException("Limit parameter must be less than 80,000")
    tables = manager.required_tables(api_obj)
    data = join_api.joinable_query(tables, api_obj, manager.table_years, csv_format=csv)
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
    results = {table.full_name(): table.col_strs(short_name=True) for table in table_manager.registered_models
               if list_all or all([table.can_show(show, sumlevel) for show,sumlevel in combos])}
    return jsonify(metadata=results)


@mod.route('/table/variables/')
def all_table_vars():
    '''show all available data tables and contained variables'''
    results = {table.full_name(): table.col_strs(short_name=True) for table in table_manager.registered_models}
    return jsonify(metadata=results)

@mod.route("/years/")
def years_view():
    years_data = manager.table_years_set
    return jsonify(data=years_data)
