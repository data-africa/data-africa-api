import operator

from sqlalchemy import distinct
from sqlalchemy.sql import func

from data_africa.core import get_columns
from data_africa.core.registrar import registered_models
from data_africa.core.exceptions import DataAfricaException
from data_africa.core import crosswalker
from data_africa.attrs import consts

from data_africa import cache


def table_name(tbl):
    return "{}.{}".format(tbl.__table_args__["schema"],
                          tbl.__tablename__)


@cache.memoize()
def tbl_years_set():
    years_set = {}
    for tbl in registered_models:
        tbl_name = table_name(tbl)
        if hasattr(tbl, "year"):
            qry = tbl.query.with_entities(
                distinct(tbl.year).label("year"),
            )
            years_set[tbl_name] = [res.year for res in qry]
        else:
            years_set[tbl_name] = None
    return years_set

@cache.memoize()
def tbl_years():
    years = {}
    for tbl in registered_models:
        tbl_name = table_name(tbl)
        if hasattr(tbl, "year"):
            qry = tbl.query.with_entities(
                func.max(tbl.year).label("max_year"),
                func.min(tbl.year).label("min_year"),
            )
            res = qry.one()
            years[tbl_name] = {consts.LATEST: res.max_year,
                               consts.OLDEST: res.min_year}
        else:
            years[tbl_name] = None
    return years


def table_exists(full_tblname):
    return full_tblname in tbl_years()


@cache.memoize()
def tbl_sizes():
    sizes = {}
    for tbl in registered_models:
        tbl_name = table_name(tbl)
        sizes[tbl_name] = tbl.query.count()
    return sizes


class TableManager(object):
    possible_variables = list(set([col.key for t in registered_models
                          for col in get_columns(t)]))
    table_years_set = tbl_years_set()
    table_years = tbl_years()


    @classmethod
    def table_can_show(cls, table, api_obj):
        shows_and_levels = api_obj.shows_and_levels
        supported_levels = table.get_supported_levels()

        for show_col, show_level in shows_and_levels.items():
            if show_col not in supported_levels:
                # print show_col, supported_levels, "Supported Levels"
                return False
            else:
                if show_level not in supported_levels[show_col]:
                    return False

        if api_obj.force and table.full_name() != api_obj.force:
            return False


        return True

    @classmethod
    def required_tables(cls, api_obj):
        '''Given a list of X, do Y'''
        vars_needed = api_obj.vars_needed + api_obj.where_vars()
        if api_obj.order and api_obj.order in cls.possible_variables:
            vars_needed = vars_needed + [api_obj.order]
        universe = set(vars_needed)
        tables_to_use = []
        table_cols = []
        # Make a set of the variables that will be needed to answer the query
        while universe:
            # first find the tables with biggest overlap
            candidates = cls.list_partial_tables(universe, api_obj)
            # raise Exception(candidates)
            top_choices = sorted(candidates.items(), key=operator.itemgetter(1),
                                 reverse=True)
            # take the table with the biggest overlap
            tbl, overlap = top_choices.pop(0)
            # ensure the tables are joinable, for now that means
            # having atleast one column with the same name
            if tables_to_use:
                while not set(table_cols).intersection([str(c.key) for c in get_columns(tbl)]):
                    if top_choices:
                        tbl, overlap = top_choices.pop(0)
                    else:
                        raise DataAfricaException("can't join tables!")
            tables_to_use.append(tbl)
            tmp_cols = [str(c.key) for c in get_columns(tbl)]
            table_cols += tmp_cols
            # remove the acquired columns from the universe
            universe = universe - set(tmp_cols)
        return tables_to_use

    @classmethod
    def list_partial_tables(cls, vars_needed, api_obj):
        candidates = {}
        for table in registered_models:
            overlap_size = TableManager.table_has_some_cols(table, vars_needed)
            if overlap_size > 0:
                if TableManager.table_can_show(table, api_obj):
                    # to break ties, we'll use median moe to penalize and subtract
                    # since larger values will be chosen first.
                    penalty = (1 - (1.0 / table.median_moe)) if table.median_moe > 0 else 0
                    candidates[table] = overlap_size - penalty
        if not candidates:
            raise DataAfricaException("No tables can match the specified query.")
        return candidates

    @classmethod
    def table_has_some_cols(cls, table, vars_needed):
        '''
        Go through the list of required variables find tables that have
        atleast 2 variables (if more than one variable is needed). The reason atleast
        2 are required is allow a join to occur (one for the value, one to potentially join).
        '''
        table_cols = get_columns(table)
        cols = set([col.key for col in table_cols])
        # min_overlap = 2 if len(vars_needed) > 1 else 1
        intersection = set(vars_needed).intersection(cols)

        if intersection:
            return len(intersection)
        return None # TODO review this

    @classmethod
    def table_has_cols(cls, table, vars_needed):
        table_cols = get_columns(table)
        cols = set([col.key for col in table_cols])
        return set(vars_needed).issubset(cols)

    @classmethod
    def select_best(cls, table_list, api_obj):
        # Ordering is sorted in table_list based on moe
        return table_list[0]

    @classmethod
    def all_tables(cls, api_obj):
        vars_needed = api_obj.vars_needed
        candidates = []
        for table in registered_models:
            if api_obj.order and api_obj.order in cls.possible_variables:
                vars_needed = vars_needed + [api_obj.order]
            if TableManager.table_has_cols(table, vars_needed):
                if TableManager.table_can_show(table, api_obj):
                    candidates.append(table)
        candidates = sorted(candidates, key=operator.attrgetter('median_moe'))
        if not candidates:
            raise DataAfricaException("No tables can match the specified query.")
        return candidates

    @classmethod
    def multi_crosswalk(cls, tables, api_obj):
        for tbl in tables:
            api_obj = crosswalker.crosswalk(tbl, api_obj)
        return api_obj

    @classmethod
    def crosswalk(cls, table, api_obj):
        return crosswalker.crosswalk(table, api_obj)
