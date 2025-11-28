from sqlglot.optimizer.qualify import qualify
from spj_view_matcher import spj_view_match
from sqlglot import expressions as exp
from typing import Dict,Tuple,Optional
from SPJGExpression import SPJGExpression
from EquivalenceClassManager import EquivalenceClassManager
from PredicateClassifier import *
from TableStructure import *
from join_eliminator import eliminate_joins
from agg_matcher import check_agg
import re
from spjg_exp_checker import _contains_subquery,validate_spjg
from tpc_query import Colors

def sql_rewrite(query_sql,c1,c2,c3,changed_select_cols,rewrite_map,view_name="VIEW"):
    query_sql=parse_one(query_sql)
    where_add_str=str("WHERE ")
    for (col1,col2) in c1:
        where_add_str=where_add_str+str(col1)+" = "+str(col2)+"\nAND "
    flag=True
    for i,(col,op,num) in enumerate(c2):
        if op==">" and i<len(c2)-1:
            if c2[i+1][1]=="<" and 0<c2[i+1][2]-num<1e-7:
                where_add_str = where_add_str + str(col) + " = " + str((num+c2[i+1][2])/2) + "\nAND "
                flag=True
            else:
                flag=False
        if not flag:
            where_add_str=where_add_str+str(col)+" "+op+" "+str(num)+"\nAND "
        if op=="<":
            flag=False
    for e in c3:
        where_add_str=where_add_str+str(e)+"\nAND "
    where_add_str=where_add_str[:-4]
    if len(c1)==0 and len(c2)==0 and len(c3)==0:
        where_add_str=""

    col_replacement_map={}
    for col_new,col_old in changed_select_cols:
        if col_new.alias=="" or col_new.alias is None:
            col_new_name=col_new.table
        else:
            col_new_name=col_new.alias
        #if query_sql.find(str(col_old))!=-1:
            #query_sql=query_sql.replace(str(col_old),col_new_name)
        #else:
            #query_sql=query_sql.replace(str(col_old.col),str(col_new_name))
        key = (col_old.table, col_old.col)
        new_col_expr=exp.Column(
            this=exp.Identifier(this=col_new_name),
            table=exp.Identifier(this=col_new.table) if col_new.table else None
        )
        col_replacement_map[key] = new_col_expr
    def replace_columns(node):
        if isinstance(node, exp.Column):
            if node.table is None or node.table=="":
                table_name=find_table(node.name)
            else:
                table_name = node.table
            col_name = node.name
            key = (table_name, col_name)
            if key in col_replacement_map:
                return col_replacement_map[key]
        return node
    query_sql= str(query_sql.transform(replace_columns))

    for expr_old in rewrite_map:
        expr_new=rewrite_map[expr_old]
        #query_sql=query_sql.replace(str(expr_old),str(expr_new))
        query_sql = re.sub(re.escape(expr_old), expr_new, query_sql, flags=re.IGNORECASE)
    fr_ = query_sql.upper().find("FROM")
    wh_ = query_sql.upper().find("WHERE")
    if fr_!=-1 and wh_!=-1:
        query_sql=query_sql[:fr_]+"FROM "+view_name+"\n"+query_sql[wh_:]

    wh_ = query_sql.upper().find("WHERE")
    gr_ = query_sql.upper().find("GROUP")
    if wh_!=-1 and gr_!=-1:
        query_sql=query_sql[:wh_]+where_add_str+"\n"+query_sql[gr_:]

    gr_ = query_sql.upper().find("GROUP")
    if gr_!=-1:
        query_sql = query_sql[:gr_-1]+query_sql[gr_:]
    return query_sql

def _spjg_view_match(query_sql,view_sql,detail=True):
    tables_structure = tpc_build_tables_structure()
    query_spj=SPJGExpression(query_sql,tables_structure)
    view_spj=SPJGExpression(view_sql,tables_structure)
    try:
        flag,query_spj=eliminate_joins(query_spj,view_spj,tables_structure)
        if not flag:
            print("false0")
    except:
        print("Query preparation failed")
    #print(query_spj.col,query_spj.tables,query_spj.get_all_EQpredicates())
    eq_classes_q=EquivalenceClassManager(query_spj.get_all_columns(),query_spj.get_all_EQpredicates())
    eq_classes_v=EquivalenceClassManager(view_spj.get_all_columns(),view_spj.get_all_EQpredicates())
    (PR_q,PU_q)=classify_predicates(query_spj)
    (PR_v,PU_v)=classify_predicates(view_spj)
    flag1,c1,c2,c3,changed_select_cols=spj_view_match(query_spj,view_spj,PR_q,PU_q,PR_v,PU_v,eq_classes_q,eq_classes_v)
    if not flag1:
        print("false1")
        return False, None, None, None,None,None,None
    (flag6,rewrite_map)=check_agg(query_spj,view_spj,eq_classes_q,eq_classes_v)
    if not flag6:
        print("false6")
        return False, None, None, None,None,None,None
    new_query_sql=sql_rewrite(query_sql,c1,c2,c3,changed_select_cols,rewrite_map)
    print(Colors.YELLOW,"NewNew:",c1,c2,c3,changed_select_cols,re,Colors.END)
    if detail:
        return True,c1,c2,c3,changed_select_cols,rewrite_map,new_query_sql
    else:
        return new_query_sql

def join_to_from(sql):
    ast = parse_one(sql)

    def _convert_select_joins(select_node: exp.Select) -> exp.Select:
        from_clause = select_node.args.get("from")
        joins = select_node.args.get("joins")
        print("*&^&____",joins)
        if not joins:
            return select_node

        tables = []
        print("**___", from_clause.args)
        if from_clause and from_clause.expressions:
            tables.extend(from_clause.expressions)
        else:
            pass
        on_conditions = []
        for join in joins:
            kind = join.args.get("kind")
            if kind and kind.upper() != "INNER":
                pass

            tables.append(join.this)
            on_cond = join.args.get("on")
            if on_cond:
                on_conditions.append(on_cond)

        new_from = exp.From(expressions=tables)

        current_where = select_node.args.get("where")
        if on_conditions:
            combined_on = on_conditions[0]
            for cond in on_conditions[1:]:
                combined_on = exp.And(this=combined_on, expression=cond)

            if current_where:
                new_where_expr = exp.And(this=current_where.this, expression=combined_on)
            else:
                new_where_expr = combined_on

            new_where = exp.Where(this=new_where_expr)
        else:
            new_where = current_where

        new_select = select_node.copy()
        new_select.set("from", new_from)
        new_select.set("joins", [])
        if new_where:
            new_select.set("where", new_where)
        else:
            new_select.args.pop("where", None)

        return new_select

    def _recursive_convert(node):
        if isinstance(node, exp.Select):
            new_node = node.copy()
            for key, value in new_node.args.items():
                if value is None:
                    continue
                if isinstance(value, list):
                    new_value = []
                    for item in value:
                        new_item = _recursive_convert(item)
                        new_value.append(new_item)
                    new_node.set(key, new_value)
                else:
                    new_converted = _recursive_convert(value)
                    if new_converted is not value:
                        new_node.set(key, new_converted)
            final_node = _convert_select_joins(new_node)
            return final_node

        elif isinstance(node, (exp.Subquery, exp.CTE)):
            new_node = node.copy()
            for key, value in new_node.args.items():
                if value is None:
                    continue
                new_converted = _recursive_convert(value)
                if new_converted is not value:
                    new_node.set(key, new_converted)
            return new_node

        elif isinstance(node, list):
            return [_recursive_convert(item) for item in node]

        elif hasattr(node, 'args') and isinstance(node.args, dict):
            # 通用递归：处理其他表达式节点（如 Where, From, Join 等）
            new_node = node.copy()
            for key, value in new_node.args.items():
                if value is None:
                    continue
                new_converted = _recursive_convert(value)
                if new_converted is not value:
                    new_node.set(key, new_converted)
            return new_node

        else:
            return node

    return _recursive_convert(ast)

def _optimize_condition_in_place(cond, view):
    if isinstance(cond, exp.In):
        # 发现 IN 子查询 → 优化内部 SELECT
        if isinstance(cond.args.get("query"), exp.Select):
            new_query = _match_all(cond.args["query"], view)
            cond.set("query", new_query)

    elif isinstance(cond, exp.Exists):
        # 发现 EXISTS → 优化内部 SELECT
        new_inner = _match_all(cond.this, view)
        cond.set("this", new_inner)

    elif isinstance(cond, (exp.And, exp.Or, exp.Not)):
        # 逻辑操作符：递归处理子表达式
        if hasattr(cond, 'this') and cond.this:
            _optimize_condition_in_place(cond.this, view)
        if hasattr(cond, 'expression') and cond.expression:
            _optimize_condition_in_place(cond.expression, view)

def _match_all(query_ast_node,view):
    if not isinstance(query_ast_node, exp.Select):
        return query_ast_node

    new_node = query_ast_node.copy()
    with_clause = new_node.args.get("with")
    if with_clause:
        new_ctes = []
        for cte in with_clause.expressions:
            opt_body = _match_all(cte.this, view)
            if opt_body is not cte.this:
                new_cte = cte.copy()
                new_cte.set("this", opt_body)
                new_ctes.append(new_cte)
            else:
                new_ctes.append(cte)
        new_node.set("with", exp.With(expressions=new_ctes))

    if _contains_subquery(query_ast_node):
        from_clause = new_node.args.get("from")
        if from_clause:
            for join_or_table in from_clause.expressions:
                table = join_or_table.this if hasattr(join_or_table, 'this') else join_or_table
                if isinstance(table, exp.Subquery):
                    new_inner_select = _match_all(table.this, view)
                    if new_inner_select is not table.this:
                        table.set("this", new_inner_select)

        where_clause = new_node.args.get("where")
        if where_clause:
            for in_node in where_clause.find_all(exp.In):
                if isinstance(in_node.args.get("query").this, exp.Select):
                    new_query = _match_all(in_node.args["query"].this, view)
                    in_node.set("query", new_query)

            for exists_node in where_clause.find_all(exp.Exists):
                new_inner = _match_all(exists_node.this, view)
                exists_node.set("this", new_inner)

        new_expressions = []
        for expr in new_node.expressions:
            if isinstance(expr, exp.Subquery):
                optimized_inner = _match_all(expr.this, view)
                if optimized_inner is not expr.this:
                    expr = expr.copy()
                    expr.set("this", optimized_inner)
                new_expressions.append(expr)

            elif isinstance(expr, exp.Alias) and isinstance(expr.this, exp.Subquery):
                optimized_inner = _match_all(expr.this.this, view)
                if optimized_inner is not expr.this.this:
                    expr = expr.copy()
                    expr.this.set("this", optimized_inner)
                new_expressions.append(expr)

            else:
                new_expressions.append(expr)

        new_node.set("expressions", new_expressions)
        #有问题!!
        having_clause = new_node.args.get("having")
        if having_clause:
            _optimize_condition_in_place(having_clause, view)
        return new_node
    else:
        return _spjg_view_match(str(query_ast_node),view,False)

def _match_top(query_sql,view_sql):
    query_ast=parse_one(query_sql)
    new_query_ast=_match_all(query_ast,view_sql)
    return new_query_ast

sqlll="""
select a
from t1
left join b on xx=yy
right join c on xx=zz
full inner join c on xx=yyy
join cz on xx=zzzzz
where aaaaa=bbbbb;
"""
#print(repr(parse_one(sqlll)))
#print(join_to_from(sqlll))
a=SPJGExpression(sqlll)