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
        raise ValueError("distinct are not supported")
    if ast.args.get("order") is not None or ast.args.get("limit") is not None:
        #raise ValueError("order and limit are not supported")
        print("order/limit")
    if _contains_subquery(ast):
        raise ValueError("subquery")
    for expr in ast.expressions:
        if isinstance(expr, expressions.Star):
            raise ValueError("*")
        if isinstance(expr, expressions.Alias):
            k = expr.this
            if not isinstance(k, expressions.Column):
                if (flag and isinstance(k, expressions.Sum)) or (flag and isinstance(k, expressions.Avg)) \
                        or (flag and isinstance(k, expressions.Count)) \
                        or (flag and isinstance(k, expressions.Anonymous) and str(k).upper() == "COUNT_BIG(*)"):
                    continue
                elif isinstance(k, expressions.Round):
                    continue
                elif isinstance(k, expressions.Literal):
                    continue
                elif isinstance(k, expressions.Cast):
                    continue
                elif isinstance(k, expressions.DPipe):
                    continue
                else:
                    print('\033[91m',type(k),'\033[0m')
                    #raise ValueError("err1 in validate spjg")
        elif not isinstance(expr, expressions.Column) and not isinstance(expr, expressions.Round):
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