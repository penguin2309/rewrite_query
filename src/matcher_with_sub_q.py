from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional, Set, Tuple
from sqlglot import parse_one
from sqlglot.optimizer.qualify import qualify
from spj_view_matcher import spj_view_match
from sqlglot import expressions as exp
from SPJGExpression import column,SPJGExpression
from EquivalenceClassManager import EquivalenceClassManager
from PredicateClassifier import classify_predicates
from TableStructure import find_table, tpc_build_tables_structure
from join_eliminator import eliminate_joins
from agg_matcher import check_agg
import re
from spjg_exp_checker import _contains_subquery,validate_spjg
from tpc_query import Colors


@dataclass(frozen=True)
class _ViewInfo:
    name: str
    sql: str
    ast: exp.Expression
    signature: Tuple


def _flatten_and(node: exp.Expression) -> List[exp.Expression]:
    if isinstance(node, exp.And):
        return _flatten_and(node.this) + _flatten_and(node.expression)
    return [node]


def _build_and(parts: List[exp.Expression]) -> Optional[exp.Expression]:
    if not parts:
        return None
    out = parts[0]
    for p in parts[1:]:
        out = exp.And(this=out, expression=p)
    return out


def _normalize_conditions_in_place(node: exp.Expression) -> None:
    if isinstance(node, exp.Select):
        where = node.args.get("where")
        if where and where.this:
            parts = _flatten_and(where.this)
            parts.sort(key=lambda e: e.sql(normalize=True))
            where.set("this", _build_and(parts))

        joins = node.args.get("joins") or []
        for j in joins:
            on_expr = j.args.get("on")
            if on_expr:
                parts = _flatten_and(on_expr)
                parts.sort(key=lambda e: e.sql(normalize=True))
                j.set("on", _build_and(parts))

    if isinstance(node, exp.Union):
        if node.this:
            _normalize_conditions_in_place(node.this)
        if node.expression:
            _normalize_conditions_in_place(node.expression)


def _canonicalize_ast(node: exp.Expression) -> exp.Expression:
    expr = node.copy()
    try:
        expr = qualify(expr, quote_identifiers=False, identify=False)
    except Exception:
        pass
    _normalize_conditions_in_place(expr)
    return expr


def _collect_tables(node: exp.Expression) -> Set[str]:
    return {t.name for t in node.find_all(exp.Table)}


def _signature(node: exp.Expression) -> Tuple:
    if isinstance(node, exp.Union):
        return ("UNION", bool(node.args.get("distinct")), _signature(node.this), _signature(node.expression))
    if isinstance(node, exp.Select):
        group = node.args.get("group")
        group_keys = []
        if group:
            group_keys = [g.sql(normalize=True) for g in group.expressions]
            group_keys.sort()
        return (
            "SELECT",
            tuple(sorted(_collect_tables(node))),
            bool(group),
            tuple(group_keys),
        )
    return ("OTHER", node.__class__.__name__)


def _prepare_views(views: Dict[str, str]) -> Dict[str, _ViewInfo]:
    prepared: Dict[str, _ViewInfo] = {}
    for name, sql in views.items():
        try:
            ast = _canonicalize_ast(parse_one(sql))
            prepared[name] = _ViewInfo(name=name, sql=sql, ast=ast, signature=_signature(ast))
        except Exception:
            continue
    return prepared


def _cte_names(with_clause: exp.With) -> Set[str]:
    names: Set[str] = set()
    for cte in with_clause.expressions:
        try:
            names.add(cte.alias)
        except Exception:
            continue
    return names


def _cte_deps(with_clause: exp.With, names: Set[str]) -> Dict[str, Set[str]]:
    deps: Dict[str, Set[str]] = {n: set() for n in names}
    for cte in with_clause.expressions:
        cte_name = cte.alias
        used = set()
        for t in cte.this.find_all(exp.Table):
            if t.name in names and t.name != cte_name:
                used.add(t.name)
        deps[cte_name] = used
    return deps


def _topo_sort(deps: Dict[str, Set[str]]) -> List[str]:
    deps = {k: set(v) for k, v in deps.items()}
    out: List[str] = []
    ready = [k for k, v in deps.items() if not v]
    ready.sort()
    while ready:
        n = ready.pop(0)
        out.append(n)
        for k in list(deps.keys()):
            if n in deps[k]:
                deps[k].remove(n)
                if not deps[k]:
                    ready.append(k)
                    ready.sort()
    if len(out) != len(deps):
        return list(deps.keys())
    return out


def _apply_cte_table_rewrites(node: exp.Expression, cte_to_view: Dict[str, str]) -> exp.Expression:
    if not cte_to_view:
        return node

    def transform(n: exp.Expression) -> exp.Expression:
        if isinstance(n, exp.Table) and n.name in cte_to_view:
            return exp.Table(this=exp.Identifier(this=cte_to_view[n.name]))
        if isinstance(n, exp.Column) and n.table and str(n.table) in cte_to_view:
            return exp.Column(
                this=exp.Identifier(this=n.name),
                table=exp.Identifier(this=cte_to_view[str(n.table)]),
            )
        return n

    return node.transform(transform)


def _cte_mapped_view_from_select(select_node: exp.Select, view_names: Set[str]) -> Optional[str]:
    from_clause = select_node.args.get("from")
    if not from_clause or not from_clause.this:
        return None
    if select_node.args.get("joins"):
        return None

    if isinstance(from_clause.this, exp.Table):
        name = from_clause.this.name
        if name in view_names:
            return name
        return None

    if isinstance(from_clause.this, exp.Subquery) and isinstance(from_clause.this.this, exp.Table):
        inner_table = from_clause.this.this.name
        if inner_table not in view_names:
            return None
        if select_node.args.get("where") or select_node.args.get("group") or select_node.args.get("having"):
            return None
        for e in select_node.expressions:
            if isinstance(e, exp.Column):
                continue
            if isinstance(e, exp.Alias) and isinstance(e.this, exp.Column):
                continue
            return None
        return inner_table

    return None


def _try_match_union_as_whole(query_union: exp.Union, views: Dict[str, _ViewInfo]) -> Optional[str]:
    q_norm = _canonicalize_ast(query_union).sql(normalize=True)
    for v in views.values():
        if isinstance(v.ast, exp.Union):
            v_norm = v.ast.sql(normalize=True)
            if q_norm == v_norm:
                return v.name
    return None


def _signature_allows(query_node: exp.Expression, view_info: _ViewInfo) -> bool:
    q_tables = _collect_tables(query_node)
    v_tables = _collect_tables(view_info.ast)
    if q_tables and not q_tables.issubset(v_tables):
        return False
    q_sig = _signature(query_node)
    if q_sig[0] == "UNION" and view_info.signature[0] != "UNION":
        return False
    if q_sig[0] == "SELECT" and view_info.signature[0] != "SELECT":
        return False
    return True

def sql_rewrite(query_sql,c1,c2,c3,changed_select_cols,rewrite_map,view_name="VIEW"):
    query_sql=parse_one(query_sql)
    def _fmt(x):
        if getattr(x, "__class__", None) is not None and x.__class__.__name__ == "column":
            return f"{view_name}.{x.col}"
        return str(x)
    where_add_str=str("WHERE ")
    for (col1,col2) in c1:
        where_add_str=where_add_str+_fmt(col1)+" = "+_fmt(col2)+"\nAND "
    flag=True
    for i,(col,op,num) in enumerate(c2):
        if len(c2)==1 and op=="<":
            where_add_str=where_add_str+_fmt(col)+" "+op+" "+str(num)+"\nAND "
            break
        if op==">" and i<len(c2)-1:
            if c2[i+1][1]=="<" and 0<c2[i+1][2]-num<1e-7:
                where_add_str = where_add_str + _fmt(col) + " = " + str((num+c2[i+1][2])/2) + "\nAND "
                flag=True
            else:
                flag=False
        if not flag:
            where_add_str=where_add_str+_fmt(col)+" "+op+" "+str(num)+"\nAND "
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
                table=exp.Identifier(this=view_name)
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

    drop_group_by = False
    if rewrite_map and rewrite_map.get("__drop_group_by__") == "1":
        drop_group_by = True
        rewrite_map = dict(rewrite_map)
        rewrite_map.pop("__drop_group_by__", None)

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
    if drop_group_by and gr_ != -1:
        hav_=query_sql.upper().find("HAVING") if query_sql.upper().find("HAVING")!=-1 else 99999999
        ord_=query_sql.upper().find("ORDER") if query_sql.upper().find("ORDER")!=-1 else 99999999
        lim_=query_sql.upper().find("LIMIT") if query_sql.upper().find("LIMIT")!=-1 else 99999999
        k = min(hav_, ord_, lim_)
        if k == 99999999:
            query_sql = query_sql[:gr_].rstrip()
        else:
            query_sql = (query_sql[:gr_].rstrip() + "\n" + query_sql[k:].lstrip())
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
    return _match_all_ctx(query_ast_node, views, cte_to_view={})


def _match_all_ctx(query_ast_node: exp.Expression, views: Dict[str, _ViewInfo], cte_to_view: Dict[str, str]) -> exp.Expression:
    if not isinstance(query_ast_node, (exp.Select, exp.Union)):
        return query_ast_node
    #print('\033[92m', query_ast_node)
    new_node = query_ast_node.copy()
    if isinstance(query_ast_node, exp.Union):
        new_exp = _match_all_ctx(query_ast_node.expression, views, cte_to_view)
        new_this = _match_all_ctx(query_ast_node.this, views, cte_to_view)
        new_node.set("this", new_this)
        new_node.set("expression", new_exp)
        whole = _try_match_union_as_whole(new_node, views)
        if whole:
            return exp.Table(this=exp.Identifier(this=whole))
        return new_node

    with_clause = new_node.args.get("with")
    if with_clause:
        names = _cte_names(with_clause)
        deps = _cte_deps(with_clause, names)
        order = _topo_sort(deps)

        cte_expr_by_name = {cte.alias: cte for cte in with_clause.expressions}
        new_ctes = []
        local_cte_to_view = dict(cte_to_view)

        for name in order:
            cte = cte_expr_by_name.get(name)
            if not cte:
                continue
            body = _apply_cte_table_rewrites(cte.this, local_cte_to_view)
            body = _canonicalize_ast(body)
            opt_body = _match_all_ctx(body, views, local_cte_to_view)

            matched_union_view = None
            if isinstance(opt_body, exp.Union):
                matched_union_view = _try_match_union_as_whole(opt_body, views)
                if matched_union_view:
                    local_cte_to_view[name] = matched_union_view
            elif isinstance(opt_body, exp.Select):
                mapped = _cte_mapped_view_from_select(opt_body, set(views.keys()))
                if mapped:
                    local_cte_to_view[name] = mapped

            new_cte = cte.copy()
            new_cte.set("this", opt_body)
            new_ctes.append(new_cte)

        new_node.set("with", exp.With(expressions=new_ctes))
        new_node = _apply_cte_table_rewrites(new_node, local_cte_to_view)
        cte_to_view = local_cte_to_view

    if _contains_subquery(query_ast_node):
        from_clause = new_node.args.get("from")
        if from_clause:
            #print('\033[91mfrom_clause________',repr(from_clause))
            if isinstance(from_clause.this, exp.Subquery):
                old_alias = from_clause.this.alias
                optimized_inner = _match_all_ctx(from_clause.this.this, views, cte_to_view)
                if isinstance(optimized_inner, exp.Table):
                    from_clause.set("this", optimized_inner)
                    if old_alias:
                        new_table = optimized_inner.name
                        def _rebind(n: exp.Expression) -> exp.Expression:
                            if isinstance(n, exp.Column) and str(n.table) == old_alias:
                                return exp.Column(
                                    this=exp.Identifier(this=n.name),
                                    table=exp.Identifier(this=new_table),
                                )
                            return n
                        new_node = new_node.transform(_rebind)
                elif optimized_inner is not from_clause.this.this:
                    from_clause.this.set("this", optimized_inner)

        where_clause = new_node.args.get("where")
        if where_clause:
            for in_node in where_clause.find_all(exp.In):
                if in_node.args.get("query") is not None and isinstance(in_node.args.get("query").this, exp.Select):
                    new_query = _match_all_ctx(in_node.args["query"].this, views, cte_to_view)
                    in_node.set("query", new_query)

            for exists_node in where_clause.find_all(exp.Exists):
                new_inner = _match_all_ctx(exists_node.this, views, cte_to_view)
                exists_node.set("this", new_inner)

        new_expressions = []
        for expr in new_node.expressions:
            if isinstance(expr, exp.Subquery):
                optimized_inner = _match_all_ctx(expr.this, views, cte_to_view)
                if isinstance(optimized_inner, exp.Table):
                    expr = optimized_inner
                elif optimized_inner is not expr.this:
                    expr = expr.copy()
                    expr.set("this", optimized_inner)
                new_expressions.append(expr)

            elif isinstance(expr, exp.Alias) and isinstance(expr.this, exp.Subquery):
                optimized_inner = _match_all_ctx(expr.this.this, views, cte_to_view)
                if isinstance(optimized_inner, exp.Table):
                    expr = expr.copy()
                    expr.set("this", optimized_inner)
                elif optimized_inner is not expr.this.this:
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
        query_for_match = _canonicalize_ast(_apply_cte_table_rewrites(new_node, cte_to_view))

        for v in views.values():
            if not _signature_allows(query_for_match, v):
                continue
            if False:
                try:
                    match_res = _spjg_view_match(
                        query_for_match.sql(normalize=True),
                        v.ast.sql(normalize=True),
                        v.name,
                        False,
                    )
                    if match_res and match_res != "":
                        return parse_one(match_res)
                except Exception as e:
                    print(f"处理失败: {e}")
            else:
                match_res = _spjg_view_match(
                        query_for_match.sql(normalize=True),
                        v.ast.sql(normalize=True),
                        v.name,
                        False,
                    )
                if match_res and match_res != "":
                    return parse_one(match_res)
        return query_for_match#是否正确？


def _match_top(query_sql: str, view_sqls: Dict[str, str]) -> str:
    views = _prepare_views(view_sqls)
    query_ast = _canonicalize_ast(parse_one(query_sql))
    new_query = _match_all_ctx(query_ast, views, cte_to_view={}).sql(pretty=True)
    new_query = re.sub(r"\s+AS\s+\"?_col_\d+\"?\b", "", new_query, flags=re.IGNORECASE)
    return new_query
