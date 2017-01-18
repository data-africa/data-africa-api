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
            joins[full_name] = [attr_alias, col == attr_alias.id]
            new_cols.append(attr_alias.name.label(full_name + "_name"))

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


def multitable_value_filters(tables, api_obj):
    '''This method examines the values pased in query args (e.g. year=2014 or
    geo=04000US25), and applies the logic depending on the crosswalk mode.
    If the auto-crosswalk is not enabled, special logic (gen_combos)
    is required to preserve null values so the user will see that no
    value is available. Otherwise, if auto-crosswalk is enabled,
    treat each filter as an AND conjunction.
    Return the list of filters to be applied.
    '''
    filts = []

    for colname, val in api_obj.vars_and_vals.items():
        related_tables = tables_by_col(tables, colname)
        filts += gen_combos(related_tables, colname, val)

    return filts


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


def has_same_levels(tbl1, tbl2, col):
    '''Check if two tables have the same exact sumlevels for the
    given column'''
    levels1 = tbl1.get_supported_levels()[col]
    levels2 = tbl2.get_supported_levels()[col]
    return set(levels1) == set(levels2)


def gen_combos(tables, colname, val):
    '''Generate the required logical condition combinations to optionally
    join two tables'''
    combos = []
    relevant_tables = tables_by_col(tables, colname)

    possible_combos = list(itertools.combinations(relevant_tables, 2))
    if len(possible_combos) > 0:
        for table1, table2 in possible_combos:
            val1 = splitter(val)
            val2 = splitter(val)
            if colname == consts.YEAR and val in [consts.LATEST,
                                                  consts.OLDEST]:
                years1 = TableManager.table_years[table1.full_name()]
                years2 = TableManager.table_years[table2.full_name()]
                val1 = [years1[val]]
                val2 = [years2[val]]
            cond1 = and_(getattr(table1, colname).in_(val1), getattr(table2, colname).in_(val2))
            cond2 = and_(getattr(table1, colname).in_(val1), getattr(table2, colname) == None)
            cond3 = and_(getattr(table1, colname) == None, getattr(table2, colname).in_(val2))
            combos.append(or_(cond1, cond2, cond3))
    elif not len(possible_combos) and len(relevant_tables) == 1:
        # if we're just referencing a single table
        safe_colname = colname.rsplit(".", 1)[-1]
        val1 = splitter(val)
        combos.append(getattr(relevant_tables[0], safe_colname).in_(val1))
    return combos


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


def make_joins(tables, api_obj, tbl_years):
    '''Generate the joins required to combine tables'''
    my_joins = []
    filts = []

    tbl1 = tables[0]
    for idx, _ in enumerate(tables[:-1]):
        tbl2 = tables[idx + 1]
        overlap = find_overlap(tbl1, tbl2)

        # check if years overlap
        if hasattr(tbl1, "year") and hasattr(tbl2, "year"):
            years1 = sorted([int(v) for v in tbl_years[tbl1.full_name()].values()])
            years1[-1] += 1
            years2 = sorted([int(v) for v in tbl_years[tbl2.full_name()].values()])
            years2[-1] += 1
            years1range = range(*years1)
            years2range = range(*years2)
            yr_overlap = set(years1range).intersection(years2range)
        else:
            yr_overlap = False

        if not yr_overlap:
            api_obj.warn("Years do not overlap between {} and {}!".format(
                tbl1.full_name(), tbl2.full_name()))

        join_clause = True

        for idx, col in enumerate(overlap):
            if col == 'year':
                continue

            direct_join = getattr(tbl1, col) == getattr(tbl2, col)
            join_clause = and_(join_clause, direct_join)

        join_params = {"isouter": True, "full": True}
        my_joins.append([[tbl2, join_clause], join_params])

    return my_joins, filts


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


def process_joined_filters(tables, api_obj, qry):
    applied = {}
    for table in tables:
        shows_and_levels = api_obj.shows_and_levels
        for col, level in shows_and_levels.items():
            args = (table, "{}_filter_join".format(col))
            if hasattr(*args):
                func = getattr(*args)
                # expr = func(level)
                result = func(level)
                if result:
                    join_id, jtbl, filts = result
                    if join_id not in applied:
                        qry = qry.join(jtbl).filter(filts)
                        applied[join_id] = True
    return qry


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

def joinable_query(tables, joins, api_obj, tbl_years, csv_format=False):
    '''Entry point from the view for processing join query'''
    cols = parse_entities(tables, api_obj)
    tables = sorted(tables, key=lambda x: x.full_name())
    qry = None
    joined_tables = []
    filts = []

    for table in tables:
        if hasattr(table, "crosswalk"):
            joins.insert(0, table.crosswalk())

    if joins:
        while joins:
            involved_tables, join_info = joins.pop(0)
            kwargs = {"full": True}  # {"full": True, "isouter": True}
            tbl_a, tbl_b = involved_tables
            if not joined_tables:
                qry = db.session.query(tbl_a).select_from(tbl_a)
                table_to_join = tbl_b
                joined_tables += [tbl_a.full_name(), tbl_b.full_name()]
                qry = qry.join(table_to_join, join_info, **kwargs)
            elif tbl_b.full_name() in joined_tables and tbl_a.full_name() not in joined_tables:
                table_to_join = tbl_a
                joined_tables += [tbl_a.full_name()]
                qry = qry.join(table_to_join, join_info, **kwargs)
            elif tbl_a.full_name() in joined_tables and tbl_b.full_name() not in joined_tables:
                table_to_join = tbl_b
                joined_tables += [tbl_b.full_name()]
                qry = qry.join(table_to_join, join_info, **kwargs)
            else:
                raise NotImplementedError("Unhandled join case!")
    if not qry and len(tables) == 1:
        qry = tables[0].query

    qry = qry.with_entities(*cols)

    if api_obj.order:
        sort_expr = handle_ordering(tables, api_obj)
        qry = qry.order_by(sort_expr)

    filts += multitable_value_filters(tables, api_obj)
    filts += where_filters(tables, api_obj)

    for table in tables:
        filts += sumlevel_filtering2(table, api_obj)

    qry = process_joined_filters(tables, api_obj, qry)
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
