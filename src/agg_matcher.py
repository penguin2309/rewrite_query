from typing import Dict, Optional, Tuple
from sqlglot import expressions as exp
from EquivalenceClassManager import EquivalenceClassManager
from SPJGExpression import SPJGExpression
from expr_checker import is_exp_eq

def _group_by_equivalent(query_spj, view_spj, eq_classes_q, eq_classes_v) -> bool:
    q_cols = query_spj.get_all_group_by_columns()
    v_cols = view_spj.get_all_group_by_columns()
    q_roll = query_spj.group_by_rollup
    v_roll = view_spj.group_by_rollup

    if len(q_cols) != len(v_cols):
        return False
    if len(q_roll) != len(v_roll):
        return False

    for col in q_roll:
        if col not in v_roll:
            return False

    for q_col in q_cols:
        eq_cols = eq_classes_q.get_all_eq_cols(q_col) or []
        if q_col in v_cols:
            continue
        if not any(c in v_cols for c in eq_cols):
            return False

    for v_col in v_cols:
        eq_cols = eq_classes_v.get_all_eq_cols(v_col) or []
        if v_col in q_cols:
            continue
        if not any(c in q_cols for c in eq_cols):
            return False

    return True
def check_group_by_subset(query_spj, view_spj,eq_classes_q):
    query_cols = query_spj.get_all_group_by_columns()
    view_cols = view_spj.get_all_group_by_columns()
    query_rollup=query_spj.group_by_rollup
    view_rollup=view_spj.group_by_rollup
    #print("query_cols",query_cols)
    #print("view_cols",view_cols)
    if len(view_cols)==0 and len(view_rollup)==0:
        return True
    if len(view_cols)!=0:
        for col in query_cols:
            eq_cols = eq_classes_q.get_all_eq_cols(col)
            if eq_cols is None or len(eq_cols) == 0:
                if col in view_cols:
                    continue
                else:
                    return False
            flag = False
            for eq_col in eq_cols:
                if eq_col in view_cols:
                    flag = True
                    break
            if not flag:
                return False
    for roll_col in query_rollup:
        if roll_col in view_rollup:
            continue
        else:
            return False
    return True

def test_aggregation2(query_spj: SPJGExpression, view_spj: SPJGExpression,
                      eq_manager_q: EquivalenceClassManager,eq_manager_v:EquivalenceClassManager,
                      needs_rollup: bool) -> Tuple[bool, Optional[Dict]]:
    rewrite_map: Dict[str, str] = {}
    v_count_big_output_name = None
    # V 的 SUM(C) 参数的规范列到 V 输出列名的映射 {Canonical_Col: V_Output_Name}
    #不管里面是表达式or列，统一按照表达式处理
    v_sum_exp_args: list[tuple[exp.Expression, str]] = []
    v_min_exp_args: list[tuple[exp.Expression, str]] = []
    v_max_exp_args: list[tuple[exp.Expression, str]] = []
    for i, v_agg in enumerate(view_spj.aggr_exprs):
        v_output_name = view_spj.aggregates[i].alias
        if str(v_agg.this).upper() == "COUNT_BIG":#有问题?
            v_count_big_output_name =v_output_name
        elif isinstance(v_agg, exp.Sum):
            v_sum_exp_args.append((v_agg.this, v_output_name))
        elif isinstance(v_agg, exp.Min):
            v_min_exp_args.append((v_agg.this, v_output_name))
        elif isinstance(v_agg, exp.Max):
            v_max_exp_args.append((v_agg.this, v_output_name))
    v_raw_cols: list[tuple[exp.Expression, str]] = []
    for c in view_spj.col:
        out_name = c.alias if c.alias else c.col
        col_expr = exp.Column(
            this=exp.Identifier(this=c.col),
            table=exp.Identifier(this=c.table) if c.table else None
        )
        v_raw_cols.append((col_expr, out_name))

    # 2. 检查 Q 的聚合函数并记录重写信息
    for q_agg in query_spj.aggr_exprs:
        q_type = query_spj.get_aggregate_type(q_agg)
        q_arg_expr = q_agg.this

        # A. COUNT(*) 重写检查
        if q_type == "count" and str(q_arg_expr) == "*":
            if not v_count_big_output_name:
                print("Test Aggregation Failed: COUNT(*) needs COUNT_BIG(*), which is missing.")
                return False, None
            if needs_rollup:
                rewrite_map[str(q_agg)] = f"SUM({v_count_big_output_name})"
            else:
                rewrite_map[str(q_agg)] = f"{v_count_big_output_name}"

        # B. SUM(E) 检查
        elif q_type == "sum":
            flag=False
            for v_arg_expr, v_out in v_sum_exp_args:
                if is_exp_eq(q_arg_expr, v_arg_expr, eq_manager_q.get_all_equivalences()):
                    if needs_rollup:
                        rewrite_map[str(q_agg)] =f"SUM({v_out})"
                    else:
                        rewrite_map[str(q_agg)] =f"{v_out}"
                    flag=True
                    break
            if not flag:
                for v_arg_expr, v_out in v_raw_cols:
                    if is_exp_eq(q_arg_expr, v_arg_expr, eq_manager_q.get_all_equivalences()):
                        rewrite_map[str(q_agg)] = f"SUM({v_out})"
                        flag=True
                        break
            if not flag:
                print(
                    f"Test Aggregation Failed: SUM argument '{q_arg_expr}' not found in view's canonical SUM arguments.")
                return False, None
            else:
                continue

        # C. AVG(E) 重写检查
        elif q_type == "avg":
            # 1. 检查 SUM(E) 部分
            flag = False
            for v_arg_expr, v_out in v_sum_exp_args:
                if is_exp_eq(q_arg_expr, v_arg_expr, eq_manager_q.get_all_equivalences()):
                    v_sum_output_name = v_out
                    flag = True
                    break
            if not flag:
                print(
                    f"Test Aggregation Failed: AVG argument '{q_arg_expr}' not found in view's canonical SUM arguments.")
                return False, None

            # 2. 检查 COUNT_BIG(*) 部分
            if not v_count_big_output_name:
                print("Test Aggregation Failed: AVG(E) needs COUNT_BIG(*), which is missing.")
                return False, None

            # 记录重写：AVG(Q_Arg) -> SUM(V_Sum_Name) / SUM(V_Count_Big_Name)
            #v_sum_output_name = v_sum_exp_args_map[q_arg_canon]
            sum_part = f"{v_sum_output_name}"
            count_part = f"{v_count_big_output_name}"
            if needs_rollup:
                rewrite_map[str(q_agg)] = f"SUM({sum_part}) / SUM({count_part})"
            else:
                rewrite_map[str(q_agg)] = f"{sum_part} / {count_part}"

        elif q_type == "min":
            flag = False
            for v_arg_expr, v_out in v_min_exp_args:
                if is_exp_eq(q_arg_expr, v_arg_expr, eq_manager_q.get_all_equivalences()):
                    if needs_rollup:
                        rewrite_map[str(q_agg)] = f"MIN({v_out})"
                    else:
                        rewrite_map[str(q_agg)] = f"{v_out}"
                    flag = True
                    break
            if not flag:
                return False, None

        elif q_type == "max":
            flag = False
            for v_arg_expr, v_out in v_max_exp_args:
                if is_exp_eq(q_arg_expr, v_arg_expr, eq_manager_q.get_all_equivalences()):
                    if needs_rollup:
                        rewrite_map[str(q_agg)] = f"MAX({v_out})"
                    else:
                        rewrite_map[str(q_agg)] = f"{v_out}"
                    flag = True
                    break
            if not flag:
                return False, None

        # D. 忽略其他聚合函数

    return True, rewrite_map

def check_agg(query_spj, view_spj,eq_classes_q,eq_classes_v):
    flag1=check_group_by_subset(query_spj, view_spj,eq_classes_q)#query的group by列被包含在view
    if not flag1:
        print("false:check_group_by_subset")
        return False, None
    needs_rollup = not _group_by_equivalent(query_spj, view_spj, eq_classes_q, eq_classes_v)
    flag2,rewrite_map = test_aggregation2(query_spj, view_spj, eq_classes_q,eq_classes_v, needs_rollup)
    if not flag2:
        return False, None
    if not needs_rollup and (len(query_spj.get_all_group_by_columns()) > 0 or len(query_spj.group_by_rollup) > 0):
        rewrite_map["__drop_group_by__"] = "1"
    return True, rewrite_map
