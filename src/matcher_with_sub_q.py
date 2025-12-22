from sqlglot.optimizer.qualify import qualify
from spj_view_matcher import spj_view_match
from sqlglot import expressions as exp
from typing import Dict,Tuple,Optional
from SPJGExpression import column
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
        if len(c2)==1 and op=="<":
            where_add_str=where_add_str+str(col)+" "+op+" "+str(num)+"\nAND "
            break
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
        if not str(type(col_old))=="<class 'SPJGExpression.column'>":
            key=col_old
            if isinstance(col_new,str):
                new_col_expr=exp.Column(
                    this=exp.Identifier(this=col_new),
                    #table=exp.Identifier(this="")
                )
            else:
                new_col_expr=col_new
        else:
            if col_new.alias=="" or col_new.alias is None:
                col_new_name=col_new.col
            else:
                col_new_name=col_new.alias
            key = (col_old.table, col_old.col)
            new_col_expr=exp.Column(
                this=exp.Identifier(this=col_new_name),
                table=exp.Identifier(this=col_new.table)
            )
        col_replacement_map[key] = new_col_expr
    def replace_columns(node):
        if node in col_replacement_map:
            return col_replacement_map[node]
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
    elif fr_!=-1:
        query_sql=query_sql[:fr_]+"FROM "+view_name
    #print("****",where_add_str)
    wh_ = query_sql.upper().find("WHERE")
    gr_ = query_sql.upper().find("GROUP")
    if wh_!=-1 and gr_!=-1:
        query_sql=query_sql[:wh_]+where_add_str+"\n"+query_sql[gr_:]
    elif wh_!=-1:
        hav_=query_sql.upper().find("HAVING") if query_sql.upper().find("HAVING")!=-1 else 99999999
        ord_=query_sql.upper().find("ORDER") if query_sql.upper().find("ORDER")!=-1 else 99999999
        lim_=query_sql.upper().find("LIMIT") if query_sql.upper().find("LIMIT")!=-1 else 99999999
        if hav_!=99999999 or ord_!=99999999 or lim_!=99999999:
            k=min(hav_,ord_,lim_)
            query_sql=query_sql[:wh_]+"\n"+where_add_str+"\n"+query_sql[k:]
        else:
            query_sql=query_sql[:wh_]+"\n"+where_add_str
    gr_ = query_sql.upper().find("GROUP")
    if gr_!=-1:
        query_sql = query_sql[:gr_-1]+query_sql[gr_:]
    return query_sql

def _spjg_view_match(query_sql,view_sql,view_name="VIEW",detail=True):
    print('\033[94mq:::',view_name)
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
        return None
    (flag6,rewrite_map)=check_agg(query_spj,view_spj,eq_classes_q,eq_classes_v)
    if not flag6:
        print("false6")
        return None
    print(Colors.YELLOW, "New￥:",
          "\nc1:", c1,
          "\nc2:", c2,
          "\nc3:", c3,
          "\nselect:", changed_select_cols,
          "\nagg:", rewrite_map, Colors.END)
    new_query_sql=sql_rewrite(query_sql,c1,c2,c3,changed_select_cols,rewrite_map,view_name)

    if detail:
        return True,c1,c2,c3,changed_select_cols,rewrite_map,new_query_sql
    else:
        return new_query_sql

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

def _match_all(query_ast_node,views):
    if not isinstance(query_ast_node, exp.Select) and not isinstance(query_ast_node, exp.Union):
        return query_ast_node
    #print('\033[92m', query_ast_node)
    new_node = query_ast_node.copy()
    if isinstance(query_ast_node, exp.Union):
        new_exp=_match_all(query_ast_node.expression, views)
        new_this=_match_all(query_ast_node.this, views)
        new_node.set("this",new_this)
        new_node.set("expression",new_exp)
        #print('\033[94m:________',new_this,"\033[0m")
        return new_node

    with_clause = new_node.args.get("with")
    if with_clause:
        new_ctes = []
        for cte in with_clause.expressions:
            #print('\033[94mcte',cte.this)
            opt_body = _match_all(cte.this, views)
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
            #print('\033[91mfrom_clause________',repr(from_clause))
            if isinstance(from_clause.this, exp.Subquery):
                new_from=_match_all(from_clause.this.this, views)
                if new_from is not from_clause.this:
                    from_clause.set("this", new_from)

        where_clause = new_node.args.get("where")
        if where_clause:
            for in_node in where_clause.find_all(exp.In):
                if in_node.args.get("query") is not None and isinstance(in_node.args.get("query").this, exp.Select):
                    new_query = _match_all(in_node.args["query"].this, views)
                    in_node.set("query", new_query)

            for exists_node in where_clause.find_all(exp.Exists):
                new_inner = _match_all(exists_node.this, views)
                exists_node.set("this", new_inner)

        new_expressions = []
        for expr in new_node.expressions:
            if isinstance(expr, exp.Subquery):
                optimized_inner = _match_all(expr.this, views)
                if optimized_inner is not expr.this:
                    expr = expr.copy()
                    expr.set("this", optimized_inner)
                new_expressions.append(expr)

            elif isinstance(expr, exp.Alias) and isinstance(expr.this, exp.Subquery):
                optimized_inner = _match_all(expr.this.this, views)
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
            _optimize_condition_in_place(having_clause, views)
        #print(type(new_node))
        return new_node
    else:
        for name in views:
            #print(view,type(view))
            if True:#False:debug mode
                try:
                    # print(str(query_ast_node)," $$$")
                    match_res = _spjg_view_match(str(query_ast_node), views[name],name, False)
                    if match_res and match_res != "":
                        return parse_one(match_res)
                except Exception as e:
                    print(f"处理失败: {e}")
            else:
                match_res = _spjg_view_match(str(query_ast_node), views[name],name, False)
                if match_res and match_res != "":
                    return parse_one(match_res)
        return new_node#是否正确？

def _match_top(query_sql:str,view_sqls:List[str])->str:
    query_ast=parse_one(query_sql)
    #print("\033[92m",repr(query_ast),"\033[0m")
    new_query=_match_all(query_ast,view_sqls).sql(pretty=True)
    return new_query
