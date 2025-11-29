from SPJGExpression import SPJGExpression,column
from sqlglot import expressions
class ra_p:
    def __init__(self,col,op,num):
        self.col=col
        self.num=num
        self.op=op
    def __eq__(self, other):
        return isinstance(other, ra_p) and self.col == other.col and self.num == other.num and self.op == other.op
    def __hash__(self):
        return hash((self.num, self.col,self.op))
    def __repr__(self):
        return f"{self.col}{self.op}{str(self.num)}"

def _parse_literal(lit: expressions.Literal):
    if lit.is_string:
        return None
    else:
        # 尝试转为数字
        try:
            if '.' in lit.this:
                return float(lit.this)
            else:
                return int(lit.this)
        except ValueError:
            return None

def is_range_p(predicate):
    #return is_legal,col op number
    l=predicate.this
    r=predicate.expression
    op=type(predicate)
    flag=False
    if isinstance(l,expressions.Literal) and isinstance(r,expressions.Column):
        #5>a
        col = column(r.table, r.name)
        num=_parse_literal(l)
        flag=True
        if num is None:
            return 0, None, None, None
    elif isinstance(r,expressions.Literal) and isinstance(l,expressions.Column):
        col=column(l.table,l.name)
        num=_parse_literal(r)
        if num is None:
            return 0, None, None, None
    else:
        if isinstance(l,expressions.Column) and isinstance(r,expressions.Column) and isinstance(predicate,expressions.EQ):
            return 2,None,None,None#PE
        return 0,None,None,None
    match type(predicate):
        case expressions.EQ:
            op="=="
        case expressions.GT:
            if flag:
                op="<"
            else:
                op=">"
        case expressions.GTE:
            if flag:
                op="<="
            else:
                op=">="

        case expressions.LT:
            if flag:
                op=">"
            else:
                op="<"
        case expressions.LTE:
            if flag:
                op=">="
            else:
                op="<="
        case _:
            return 0, None, None, None
    return 1, op, col, num

def classify_predicates(spj_expr):
    #PE=spj_expr.get_all_EQpredicates()
    PR=[]
    PU=[]
    for p in spj_expr.where_predicates:
        flag,op,col,num=is_range_p(p)
        if flag==1:
            pr=ra_p(col,op,num)
            PR.append(pr)
        elif flag==0:
            PU.append(p)
    return PR,PU


