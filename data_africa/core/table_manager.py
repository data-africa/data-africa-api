import operator

from sqlalchemy import distinct, and_
from sqlalchemy.sql import func

from data_africa.core import get_columns, str_tbl_columns
from data_africa.core.registrar import registered_models
from data_africa.core.exceptions import DataAfricaException
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
        count = 0
        for show_col, show_level in shows_and_levels.items():
            if show_col in supported_levels and show_level in supported_levels[show_col]:
                count += 1

        if api_obj.force and table.full_name() != api_obj.force:
            return False

        return count >= 1

    @staticmethod
    def find_max_overlap(tables_to_use, top_choices):
        for cand_tbl, size_overlap in top_choices:
            max_overlap = 0
            best_tbl = None
            for tbl in tables_to_use:
                overlap = set(str_tbl_columns(tbl)).intersection(str_tbl_columns(cand_tbl))
                if len(overlap) > max_overlap:
                    max_overlap = len(overlap)
                    best_tbl = tbl
            if best_tbl:
                # TODO alias?
                cond = True
                for col in overlap:
                    if col not in ['year']:
                        cond = and_(cond, getattr(cand_tbl, col) == getattr(best_tbl, col))
                join_args = [[(cand_tbl, best_tbl), cond]]
                return cand_tbl, overlap, join_args
        return None, None, None

    @staticmethod
    def is_feasible(vars_needed, candidates):
        cols = []
        for tbl in candidates:
            cols += tbl.col_strs(short_name=True)
        missing = any([need not in cols for need in vars_needed])
        return not missing

    @classmethod
    def required_table_joins(cls, api_obj):
        '''Given a list of X, do Y'''
        vars_needed = api_obj.vars_needed + api_obj.where_vars()
        if api_obj.order and api_obj.order in cls.possible_variables:
            vars_needed = vars_needed + [api_obj.order]
        universe = set(vars_needed)
        tables_to_use = []
        table_cols = []
        join_args = []
        candidates = cls.list_partial_tables(universe, api_obj)
        feasibile = cls.is_feasible(vars_needed, candidates)

        if not feasibile:
            raise DataAfricaException("Sorry, query is not feasible!")

        # Make a set of the variables that will be needed to answer the query
        while universe:
            # first find the tables with biggest overlap
            candidates = cls.list_partial_tables(universe, api_obj)
            top_choices = sorted(candidates.items(), key=operator.itemgetter(1),
                                 reverse=True)
            # raise Exception(candidates)
            # ensure the tables are joinable, for now that means
            # having atleast one column with the same name
            if tables_to_use:
                tbl, col_overlap, tmp_joins_args = cls.find_max_overlap(tables_to_use, top_choices)
                if not tbl: continue
                join_args += tmp_joins_args
            else:
                tbl = top_choices[0][0]
            tables_to_use.append(tbl)
            universe = universe - set(str_tbl_columns(tbl))
        return tables_to_use, join_args

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
        # if not candidates:
            # raise DataAfricaException("No tables can match the specified query.")
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
        return 0

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
