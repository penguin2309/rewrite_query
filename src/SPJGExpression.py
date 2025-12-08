from pyspark.sql.types import StructType
from sqlglot import *
from typing import List,Set,Optional
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
                isinstance(self.ast.args.get("expressions")[0],expressions.Literal):
            self.ast.set("expressions",[])#select 1 ...
        self.tables=set()
        self.col=[]
        self.old_col=[]
        self.where_predicates=[]
        self.group_by=[]
        self.aggregates=[]
        self.aggr_exprs=[]
        self.select_exprs=[]#处理select a/b,round(a/b),...情况
        self.select_exprs_alias=[]
        self.literal_expr=[]#select 'c' from xxx
        self.literal_expr_alias=[]
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
            for tab in self.ast.find_all(expressions.Table):
                self.tables.add(tab.name)

        for expr in self.ast.expressions:
            #print("$$:::",expr.this,type(expr.this))
            if isinstance(expr, expressions.Alias) and isinstance(expr.this, expressions.Sum):
                self.aggregates.append(expr)
                self.aggr_exprs.append(expr.this)
            elif isinstance(expr, expressions.Alias) and isinstance(expr.this, expressions.Avg):
                self.aggregates.append(expr)
                self.aggr_exprs.append(expr.this)
            elif isinstance(expr, expressions.Alias) and isinstance(expr.this, expressions.Count):
                self.aggregates.append(expr)
                self.aggr_exprs.append(expr.this)
            elif str(expr.this).upper()=="COUNT_BIG(*)":
                #print("COUNT_BIG(*)")
                self.aggregates.append(expr)
                self.aggr_exprs.append(expr.this)
            elif self.is_exp(self,expr):
                if isinstance(expr,expressions.Alias):
                    self.select_exprs.append(expr.this)
                    self.select_exprs_alias.append(expr.alias)
                else:
                    self.select_exprs.append(expr)
                    self.select_exprs_alias.append("")
            else:
                alias_name = None
                if isinstance(expr, expressions.Alias):
                    alias_name=expr.alias
                    expr=expr.this
                if isinstance(expr, expressions.Literal):
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
                new_predicates.append(new_pred)

            self.where_predicates = new_predicates  # 更新存储的内容

    def get_all_columns(self):
        cols=set(self.col)
        for p in self.where_predicates:
            for col_node in p.find_all(expressions.Column):
                cols.add(column(col_node.table,col_node.name))
        return cols

    def get_all_EQpredicates(self):
        EQpredicates=self.added_eq_classes
       # print("here",EQpredicates)
        for p in self.where_predicates:
            if isinstance(p,expressions.EQ):
                l,r=p.this,p.expression
                if isinstance(r,expressions.Column) and isinstance(l,expressions.Column):
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
        if isinstance(agg_expr, expressions.Count):
            return "count"
        elif isinstance(agg_expr, expressions.Sum):
            return "sum"
        elif isinstance(agg_expr, expressions.Avg):
            return "avg"
        return None
    @staticmethod
    def is_exp(self,expr):
        if isinstance(expr, expressions.Alias):
            if isinstance(expr.this, expressions.Round):
                return True
            if isinstance(expr.this, expressions.Add):
                return True
            if isinstance(expr.this, expressions.Sub):
                return True
            if isinstance(expr.this, expressions.Mul):
                return True
            if isinstance(expr.this, expressions.Div):
                return True
        else:
            if isinstance(expr, expressions.Round):
                return True
            if isinstance(expr, expressions.Add):
                return True
            if isinstance(expr, expressions.Sub):
                return True
            if isinstance(expr, expressions.Mul):
                return True
            if isinstance(expr, expressions.Div):
                return True
        return False


