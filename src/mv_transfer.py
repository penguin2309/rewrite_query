from sqlglot import parse,expressions
import os
def mv_transfer(file_path):
    with open(file_path) as f:
        mvs = f.read()
    parse_res=parse(mvs)
    mv_dict={}
    for row in parse_res:
        if isinstance(row,expressions.Create):
            sql=row.expression
            name=row.this.this.this
            mv_dict[name]=str(sql)
    return mv_dict
