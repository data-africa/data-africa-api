from pyparsing import Word, nums
from pyparsing import oneOf, opAssoc, Literal, srange
from pyparsing import operatorPrecedence as opPrec
from data_africa.core.exceptions import DataAfricaException
from sqlalchemy import func
from flask import request

op_lookup = {
    "*": '__mul__',
    "/": '__div__',
    "+": '__add__',
    "-": '__sub__'
}


def resolve(result, lookup):
    if isinstance(result, list):
        left, operator, right = result
        left_obj = resolve(left, lookup)
        right_obj = resolve(right, lookup)
        if operator == "^":
            return func.pow(left_obj, right_obj)
        elif operator == '/' and isinstance(left_obj, float):
            # handle special case for python3 division
            return left_obj / right_obj
        else:
            op = op_lookup[operator]
            return getattr(left_obj, op)(right_obj)
    elif result in lookup:
        return lookup[result]
    else:
        try:
            val = float(result)
            return val
        except ValueError:
            raise DataAfricaException("invalid number", result)


def parse(cols, api_obj):
    lookup = {str(col.key): col for col in cols}
    number = Word(nums + '.')
    variable = Word(srange("[a-zA-Z0-9_]"), srange("[a-zA-Z0-9_]"))
    operand = number | variable

    expop = Literal('^')
    signop = oneOf('+ -')
    multop = oneOf('* /')
    plusop = oneOf('+ -')


    # raise Exception(request.query_string)

    expr = opPrec(operand, [(expop, 2, opAssoc.RIGHT),
                            (signop, 1, opAssoc.RIGHT),
                            (multop, 2, opAssoc.LEFT),
                            (plusop, 2, opAssoc.LEFT)])
    raw_exprs = api_obj.computed

    if not raw_exprs:
        return []

    computed_cols = []
    for raw_expr in raw_exprs.split(","):
        if ":" in raw_expr:
            label, raw_expr = raw_expr.split(":")
        else:
            label = "computed_{}".format(len(computed_cols))
        result = expr.parseString(raw_expr).asList()
        resolved_exprs = resolve(result[0], lookup)
        computed_cols.append(resolved_exprs.label(label))
    return computed_cols
