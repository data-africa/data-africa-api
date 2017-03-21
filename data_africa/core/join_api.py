'''
Implementation of logic for joining variables across tables
'''
import itertools
from sqlalchemy import and_, or_
from sqlalchemy.orm import aliased

from data_africa.core.table_manager import TableManager

from data_africa.util.helper import splitter
from data_africa.attrs import consts

from data_africa.attrs.views import attr_map
from data_africa.core.streaming import stream_qry, stream_qry_csv
from data_africa.core.exceptions import DataAfricaException
from data_africa.core import get_columns
from data_africa.spatial.models import Cell5M

from data_africa.database import db


def parse_method_and_val(cond):
    if cond.startswith("^"):
        return "startswith", cond[1:], False
    elif cond.startswith("~^"):
        return "startswith", cond[2:], True
    elif cond.endswith("$"):
        return "endswith", cond[:-1], False
    elif cond.endswith("~$"):
        return "endswith", cond[:-2], True
    elif cond.startswith("str!"):
        return "ne", str(cond[4:]), False
    elif cond.startswith("!"):
        return "ne", int(cond[1:]), False
    elif cond.startswith(">"):
        return "gt", int(cond[1:]), False
    elif cond.startswith("<"):
        return "lt", int(cond[1:]), False
    elif cond.startswith("R<"):
        return "rt", float(cond[2:]), False
    elif cond.startswith("R>"):
        return "rg", float(cond[2:]), False
    else:
        return "like", cond, False


def use_attr_names(qry, cols):
    '''This method will return a query object with outer joins to include
    description names for all columns which have attribute data'''
    new_cols = []
    joins = {}
    for col in cols:
        full_name = str(col)
        _, var_name = full_name.rsplit(".", 1)

        if var_name in attr_map:
            attr_obj = attr_map[var_name]
            attr_alias = aliased(attr_obj)
            id_col_ref = attr_alias.id if hasattr(attr_obj, "id") else getattr(attr_alias, var_name)
            joins[full_name] = [attr_alias, col == id_col_ref]
            name_col_ref = attr_alias.name if hasattr(attr_obj, "name") else getattr(attr_alias, "{}_name".format(var_name))
            new_cols.append(name_col_ref.label(var_name + "_name"))

        new_cols.append(col)
    for my_joins in joins.values():
        qry = qry.join(*my_joins, isouter=True)
    return qry, new_cols


def sumlevel_filtering2(table, api_obj):
    '''This method provides the logic to handle sumlevel filtering.
    If auto-crosswalk mode is true the conditions will be simple, otherwise
    NULLs will be allowed so that NULL rows can be retained for the result
    sent back to the users'''
    shows_and_levels = api_obj.shows_and_levels
    filters = []
    for col, level in shows_and_levels.items():
        args = (table, "{}_filter".format(col))
        # join_args = (table, "{}_join_filter".format(col))
        if hasattr(*args):
            func = getattr(*args)
            expr = func(level)
            # TODO: test using .is_(None) to avoid flake8 warnings
            filters.append(or_(expr, getattr(table, col) == None))

    return filters




def parse_entities(tables, api_obj):
    '''Give a list of tables and required variables resolve
    the underlying objects'''
    values = api_obj.vars_needed

    # force the primary key columns to be returned to avoid potential confusion
    for table in tables:
        my_missing_pks = [col for col in table.__table__.columns
                          if col.primary_key and col.key not in values]
        values += [pkc.key for pkc in my_missing_pks]

    values = set(values)

    col_objs = [get_column_from_tables(tables, value) for value in values]

    return col_objs


def find_overlap(tbl1, tbl2):
    '''Given two table objects, determine the set of intersecting columns by
    column name'''
    cols1 = [c.key for c in get_columns(tbl1)]
    cols2 = [c.key for c in get_columns(tbl2)]
    myset = set(cols1).intersection(cols2)
    return myset




def make_filter(col, cond):
    '''Generate SQLAlchemy filter based on string'''
    method, value, negate = parse_method_and_val(cond)
    if method == "ne":
        expr = col != value
    elif method == "gt":
        expr = col > value
    elif method == "lt":
        expr = col < value
    # elif method == "rt":
        # expr = and_(cols[1] != 0, cols[0] / cols[1] < value)
    # elif method == "rg":
        # expr = and_(cols[1] != 0, cols[0] / cols[1] > value)
    else:
        if method == 'like' and "%" not in value:
            method = '__eq__'
        expr = getattr(col, method)(value)
    if negate:
        expr = ~expr

    return expr


def where_filters(tables, api_obj):
    '''Process the where query argument from an API call'''
    if not api_obj.where:
        return []
    filts = []

    wheres = splitter(api_obj.where)
    for where in wheres:
        colname, cond = where.split(":")
        target_var, filt_col = colname.rsplit(".", 1)

        if filt_col == 'sumlevel':
            filt_col = api_obj.shows_and_levels.keys()[0]
            cols = get_column_from_tables(tables, target_var, False)
            table = tables_by_col(tables, target_var, return_first=True)
            args = (table, "{}_filter".format(filt_col))
            if hasattr(*args):
                func = getattr(*args)
                filts.append(func(cond))
        else:
            cols = get_column_from_tables(tables, target_var, False)
            for col in cols:
                table = col.class_
                filt_col = getattr(table, filt_col)
                filt = make_filter(filt_col, cond)
                filts.append(filt)
    return filts


def tables_by_col(tables, col, return_first=False):
    '''Return a table or a list of tables that contain the given column'''
    acc = []
    for table in tables:
        if hasattr(table, col):
            if return_first:
                return table
            else:
                acc.append(table)
        elif "." in col:
            my_table_name, colname = col.rsplit(".", 1)
            if my_table_name == table.full_name() and hasattr(table, colname):
                if return_first:
                    return table
                else:
                    acc.append(table)

    return acc


def get_column_from_tables(tables, col, return_first=True):
    '''Given a list of tables return the reference to the column in a
    list of tables'''
    acc = []
    for table in tables:
        if hasattr(table, col):
            if return_first:
                return getattr(table, col)
            else:
                acc.append(getattr(table, col))
    return acc


def handle_ordering(tables, api_obj):
    '''Process sort and order parameters from the API'''
    sort = "desc" if api_obj.sort == "desc" else "asc"
    if api_obj.order not in TableManager.possible_variables:
        raise DataAfricaException("Bad order parameter", api_obj.order)
    my_col = get_column_from_tables(tables, api_obj.order)
    sort_expr = getattr(my_col, sort)()
    return sort_expr.nullslast()




def inside_filters(tables, api_obj):
    if not api_obj.inside:
        return []

    for attr_kind, attr_id in api_obj.inside:
        attr_class = attr_map[attr_kind]
        attr_obj = attr_class(id=attr_id)

        return [attr_obj.child_filter(table) for table in tables if hasattr(table, attr_kind)]
    return []

def handle_neighbors(qry, tables, api_obj):
    if not api_obj.neighbors:
        return qry

    for attr_id in api_obj.neighbors:
        target = Cell5M.query.get(attr_id)
        join_conds = [Cell5M.geo == tbl.geo
                      for tbl in tables if getattr(tbl, 'geo', None)]
        touches_filt = Cell5M.geom.ST_Touches(target.geom)
        # -- return the geo id itself and its neighbors
        geo_filt = or_(touches_filt, Cell5M.geo == attr_id)
        lvl_filt = Cell5M.geo.startswith(attr_id[:3])
        qry = qry.join(Cell5M, and_(*join_conds)).filter(geo_filt, lvl_filt)

    return qry

def simple_filter(qry, tables, api_obj):
    filts = []
    for tbl in tables:
        cols = set(tbl.col_strs(short_name=True))
        for col_name, val in api_obj.vars_and_vals.items():
            if col_name == consts.YEAR and val in [consts.LATEST, consts.OLDEST]:
                if col_name in cols:
                    years1 = TableManager.table_years[tbl.full_name()]
                    filts.append(tbl.year == years1[val])
            else:
                vals = val.split(",")
                if col_name in cols:
                    filts.append(getattr(tbl, col_name).in_(vals))
    return qry.filter(*filts)

def make_join_cond(tbl_a, tbl_b, api_obj):
    a_cols = set(tbl_a.col_strs(short_name=True))
    b_cols = tbl_b.col_strs(short_name=True)
    overlap = a_cols.intersection(b_cols)

    conds = [getattr(tbl_a, col_name) == getattr(tbl_b, col_name)
                for col_name in overlap]
    # TODO move to function...
    for col_name, val in api_obj.vars_and_vals.items():
        if col_name == consts.YEAR and val in [consts.LATEST, consts.OLDEST]:
            if col_name in a_cols:
                years1 = TableManager.table_years[tbl_a.full_name()]
                conds.append(tbl_a.year == years1[val])
            if col_name in b_cols:
                years2 = TableManager.table_years[tbl_b.full_name()]
                conds.append(tbl_b.year == years2[val])
        else:
            vals = val.split(",")
            if col_name in a_cols:
                conds.append(getattr(tbl_a, col_name).in_(vals))
            if col_name in b_cols:
                conds.append(getattr(tbl_b, col_name).in_(vals))
    # joined filters logic
    shows_and_levels = api_obj.shows_and_levels.items()
    for table in [tbl_a, tbl_b]:
        for col, level in shows_and_levels:
            args = (table, "{}_val_filter".format(col))
            if hasattr(*args):
                vals = getattr(*args)(level)
                if vals:
                    conds.append(~getattr(table, col).in_(vals))
    return and_(*conds)

def joinable_query(tables, joins, api_obj, tbl_years, csv_format=False):
    '''Entry point from the view for processing join query'''
    cols = parse_entities(tables, api_obj)

    tables = sorted(tables, key=lambda x: 1 if x.is_attr() else -1)
    qry = None
    joined_tables = []
    filts = []

    for table in tables:
        if hasattr(table, "crosswalk"):
            tables.append(table.crosswalk())
            joins.append(table.crosswalk())


    qry = db.session.query(tables[0]).select_from(tables[0])

    if joins:
        combos = list(itertools.product(tables[:1], tables[1:]))
        for tbl_a, tbl_b in combos:
            join_cond = make_join_cond(tbl_a, tbl_b, api_obj)
            qry = qry.join(tbl_b, join_cond)
    else:
        qry = simple_filter(qry, tables, api_obj)

    if not qry and len(tables) == 1:
        qry = tables[0].query

    if api_obj.display_names:
        qry, cols = use_attr_names(qry, cols)
    qry = qry.with_entities(*cols)

    if api_obj.order:
        sort_expr = handle_ordering(tables, api_obj)
        qry = qry.order_by(sort_expr)

    filts += where_filters(tables, api_obj)

    for table in tables:
        filts += sumlevel_filtering2(table, api_obj)

    filts += inside_filters(tables, api_obj)

    qry = handle_neighbors(qry, tables, api_obj)

    qry = qry.filter(*filts)

    if api_obj.limit:
        qry = qry.limit(api_obj.limit)

    if api_obj.offset:
        qry = qry.offset(api_obj.offset)

    if csv_format:
        return stream_qry_csv(cols, qry, api_obj)
    return stream_qry(tables, cols, qry, api_obj)
