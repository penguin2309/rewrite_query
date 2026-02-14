from typing import List, Optional, Set
from sqlglot import parse_one
from sqlglot import expressions as exp
from TableStructure import find_table
from spjg_exp_checker import validate_spjg

class column:
    def __init__(self,tab,col,alias=None):
        if tab is None or tab=="":
            tab=find_table(col)
        self.table=tab
        self.col=col
        if alias is not None and alias!="":
            self.alias=alias#rewrite的时候使用
        else:
            self.alias=""
    def __eq__(self,other):
        return isinstance(other,column) and self.col==other.col and self.table==other.table
    def __hash__(self):
        return hash((self.table,self.col))
    def __repr__(self):
        return f"{self.table}.{self.col}"
    def __lt__(self, other):
        if not isinstance(other, column):
            return NotImplemented
        return (self.table, self.col) < (other.table, other.col)

class SPJGExpression:
    def __init__(self,sql,tables_structure=None):
        self.origin_sql=sql
        self.ast=parse_one(self.origin_sql)
        if len(self.ast.args.get("expressions"))==1 and \
                isinstance(self.ast.args.get("expressions")[0],exp.Literal):
            self.ast.set("expressions",[])#select 1 ...
        self.tables=set()
        self.col=[]
        self.old_col=[]
        self.where_predicates=[]
        self.group_by=[]
        self.group_by_rollup=[]
        self.aggregates=[]
        self.aggr_exprs=[]
        self.select_exprs=[]#处理select a/b,round(a/b),...情况/cast
        self.select_exprs_alias=[]
        self.literal_expr=[]#select 'c' from xxx
        self.literal_expr_alias=[]
        #self.cast_exprs=[]
        #self.cast_exprs_alias=[]
        self.added_eq_classes=[]#3.2节
        self.tables_structure=[]
        validate_spjg(self.ast)
        self.get(tables_structure)


    @staticmethod
    def _flatten_and(node: exp.Expression) -> List[exp.Expression]:
        if isinstance(node, exp.And):
            return SPJGExpression._flatten_and(node.this) + SPJGExpression._flatten_and(node.expression)
        else:
            return [node]

    def get(self,tables_structure):
        self.tables_structure=tables_structure
        joins=self.ast.args.get("joins")
        if joins:
            for join in joins:
                k=join.args.get("kind")
                s=join.args.get("side")
                if (k is not None and k.upper()=="INNER") or \
                        (s is None):
                    self.where_predicates+=self._flatten_and(join.args.get("on"))
                else:
                    #外连接
                    print("OUTER JOIN")
        fr=self.ast.args.get("from")
        if fr:
            for tab in self.ast.find_all(exp.Table):
                self.tables.add(tab.name)

        for expr in self.ast.expressions:
            #print("$$:::",expr.this,type(expr.this))
            if isinstance(expr, exp.Alias) and isinstance(expr.this, exp.Sum):
                self.aggregates.append(expr)
                self.aggr_exprs.append(expr.this)
            elif isinstance(expr, exp.Alias) and isinstance(expr.this, exp.Avg):
                self.aggregates.append(expr)
                self.aggr_exprs.append(expr.this)
            elif isinstance(expr, exp.Alias) and isinstance(expr.this, exp.Count):
                self.aggregates.append(expr)
                self.aggr_exprs.append(expr.this)
            elif isinstance(expr, exp.Alias) and isinstance(expr.this,exp.Max):
                self.aggregates.append(expr)
                self.aggr_exprs.append(expr.this)
            elif isinstance(expr, exp.Alias) and isinstance(expr.this, exp.Min):
                self.aggregates.append(expr)
                self.aggr_exprs.append(expr.this)
            elif str(expr.this).upper()=="COUNT_BIG(*)":
                #print("COUNT_BIG(*)")
                self.aggregates.append(expr)
                self.aggr_exprs.append(expr.this)
            elif self.is_exp(self,expr):
                if isinstance(expr,exp.Alias):
                    self.select_exprs.append(expr.this)
                    self.select_exprs_alias.append(expr.alias)
                else:
                    self.select_exprs.append(expr)
                    self.select_exprs_alias.append("")
            else:
                alias_name = None
                if isinstance(expr, exp.Alias):
                    alias_name=expr.alias
                    expr=expr.this
                if isinstance(expr, exp.Literal):
                    self.literal_expr.append(expr)
                    self.literal_expr_alias.append(alias_name)
                    continue
                tb = expr.table
                if tb == "":
                    tb=find_table(str(expr.name))
                if tb is None or tb=="":
                    raise ValueError(f"{expr.name}'s table not found")
                self.col.append(column( tab=tb,col=expr.name,alias=alias_name))
                self.old_col.append(column(tab=tb, col=expr.name,alias=alias_name))

        group_by_expr = self.ast.args.get("group")
        if group_by_expr:
            for expr in group_by_expr.expressions:
                self.group_by.append(expr)
            rollup_exprs = group_by_expr.args.get('rollup') or []
            for expr in rollup_exprs:
                self.group_by_rollup.append(expr)

        p = self.ast.args.get("where")
        new_predicates = []
        if p:
            self.where_predicates += self._flatten_and(p.this)
        if self.where_predicates:
            for pred in self.where_predicates:
                if pred is None:
                    continue
                new_pred = pred.copy()
                for col_node in new_pred.find_all(exp.Column):
                    if not col_node.table:
                        #print("$$:",col_node, col_node.name, col_node.alias)
                        tb = find_table(str(col_node.name))
                        if tb is None or tb == "":
                            raise ValueError(f"Column '{col_node.name}' in WHERE must specify table")
                        else:
                            col_node.set("table", exp.Identifier(this=tb))
                if isinstance(new_pred, exp.Between):
                    p=self.split_between_expression(self,new_pred)
                    new_predicates.append(p[0])
                    new_predicates.append(p[1])
                else:
                    new_predicates.append(new_pred)
            #print(new_predicates)
            self.where_predicates = new_predicates  # 更新存储的内容

    def get_all_columns(self):
        cols=set(self.col)
        for p in self.where_predicates:
            for col_node in p.find_all(exp.Column):
                cols.add(column(col_node.table,col_node.name))
        return cols

    def get_all_EQpredicates(self):
        EQpredicates=self.added_eq_classes
       # print("here",EQpredicates)
        for p in self.where_predicates:
            if isinstance(p,exp.EQ):
                l,r=p.this,p.expression
                if isinstance(r,exp.Column) and isinstance(l,exp.Column):
                    EQpredicates.append((column(l.table,l.name,self.tables_structure),column(r.table,r.name,self.tables_structure)))
        EQpredicates=list(set(tuple(sorted(t_))for t_ in EQpredicates))
        return EQpredicates

    def get_all_eq_columns(self):
        eqpredicates=self.get_all_EQpredicates()
        eq_cols=set()
        for (a,b) in eqpredicates:
            eq_cols.add(a)
            eq_cols.add(b)
        return eq_cols

    def get_all_group_by_columns(self):
        columns = set()
        #print("*",self.tables_structure)
        for expr in self.group_by:
            for col_node in expr.find_all(exp.Column):
                #print(col_node)
                columns.add(column(col_node.table, col_node.name,self.tables_structure))
        return list(columns)

    @staticmethod
    def get_aggregate_type(agg_expr):
        """获取聚合函数类型（count, sum, avg等）"""
        if isinstance(agg_expr, exp.Count):
            return "count"
        elif isinstance(agg_expr, exp.Sum):
            return "sum"
        elif isinstance(agg_expr, exp.Avg):
            return "avg"
        return None
    @staticmethod
    def is_exp(self,expr):
        if isinstance(expr, exp.Alias):
            if isinstance(expr.this, exp.Round):
                return True
            if isinstance(expr.this, exp.Add):
                return True
            if isinstance(expr.this, exp.Sub):
                return True
            if isinstance(expr.this, exp.Mul):
                return True
            if isinstance(expr.this, exp.Div):
                return True
            if isinstance(expr.this, exp.Cast):
                return True
            if isinstance(expr.this, exp.DPipe):
                return True
        else:
            if isinstance(expr, exp.Round):
                return True
            if isinstance(expr, exp.Add):
                return True
            if isinstance(expr, exp.Sub):
                return True
            if isinstance(expr, exp.Mul):
                return True
            if isinstance(expr, exp.Div):
                return True
            if isinstance(expr, exp.Cast):
                return True
            if isinstance(expr, exp.DPipe):
                return True
        return False
    @staticmethod
    def split_between_expression(self,between_expr):
        this = between_expr.this  # 被比较的表达式
        low = between_expr.args.get('low')  # 下限
        high = between_expr.args.get('high')  # 上限
        symmetric = between_expr.args.get('symmetric', False)  # 是否对称
        if symmetric:
            lower_bound = exp.GT(this=this, expression=low)
            upper_bound = exp.LT(this=this, expression=high)  # this < high
        else:
            # 普通BETWEEN: 下限 <= 表达式 <= 上限
            lower_bound = exp.GTE(this=this, expression=low)
            upper_bound = exp.LTE(this=this, expression=high)  # this <= high
        return [lower_bound, upper_bound]
