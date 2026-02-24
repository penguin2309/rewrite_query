from pyspark.sql.types import StructType
from sqlglot import *
from typing import List,Set,Optional
#from TableStructure import *

def validate_spjg(ast):
    #print(repr(ast))
    flag = False
    if ast.args.get("group") or ast.args.get("having"):
        flag = True
    if ast.args.get("distinct"):
        exprs = ast.args.get("expressions") or []
        if ast.args.get("group") or ast.args.get("having"):
            ast.set("distinct", None)
        else:
            if not expressions:
                raise ValueError("distinct with empty select")
            group_exprs = []
            for expr in exprs:
                item = expr
                if isinstance(expr, expressions.Alias):
                    item = expr.this
                if isinstance(item, (expressions.Sum, expressions.Avg, expressions.Count,
                                     expressions.Min, expressions.Max, expressions.Anonymous)):
                    raise ValueError("distinct with aggregate")
                if isinstance(item, (expressions.Column, expressions.Literal)):
                    group_exprs.append(item)
                    continue
                raise ValueError("distinct with complex expression")
            ast.set("group", expressions.Group(expressions=group_exprs))
            ast.set("distinct", None)
    if ast.args.get("order") is not None or ast.args.get("limit") is not None:
        #raise ValueError("order and limit are not supported")
        print("find order/limit in spjg_exp_checker")
    if _contains_subquery(ast):
        raise ValueError("subquery")
    allowed_anonymous = {"COUNT_BIG", "SUBSTR", "SUBSTRING", "NVL", "COALESCE", "DATEADD", "DATEDIFF"}
    for expr in ast.expressions:
        if isinstance(expr, expressions.Star):
            raise ValueError("*")
        if isinstance(expr, expressions.Alias):
            k = expr.this
            if not isinstance(k, expressions.Column):
                if (flag and isinstance(k, (expressions.Sum, expressions.Avg, expressions.Count, expressions.Min, expressions.Max))) \
                        or (flag and isinstance(k, expressions.Anonymous) and (k.name or "").upper() in allowed_anonymous):
                    continue
                elif isinstance(k, (expressions.Round, expressions.Literal, expressions.Cast, expressions.DPipe,
                                    expressions.Case, expressions.Coalesce, expressions.Substring,
                                    expressions.Abs, expressions.Upper, expressions.Lower,
                                    expressions.DateAdd, expressions.DateDiff, expressions.Anonymous)):
                    continue
                else:
                    print('\033[91m',type(k),'\033[0m')
                    #raise ValueError("err1 in validate spjg")
        elif not isinstance(expr, (expressions.Column, expressions.Round, expressions.Cast, expressions.DPipe,
                                   expressions.Case, expressions.Coalesce, expressions.Substring,
                                   expressions.Abs, expressions.Upper, expressions.Lower,
                                   expressions.DateAdd, expressions.DateDiff, expressions.Anonymous)):
            print('\033[91m',type(expr),'\033[0m')
            #raise ValueError("err2 in validate spjg")


def _contains_subquery(node):
    for child in node.walk():
        #print("child", child,type(child))
        if isinstance(child, (exp.Subquery, exp.Exists)):
            return True
        if isinstance(child, exp.In):
            query = child.args.get("query")
            if isinstance(query, exp.Select):
                return True
    return False
