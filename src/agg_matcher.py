from spj_view_matcher import spj_view_match
from sqlglot import expressions as exp
from typing import Dict,Tuple,Optional
from SPJGExpression import SPJGExpression
from EquivalenceClassManager import EquivalenceClassManager
from PredicateClassifier import *
from TableStructure import *
from expr_checker import is_exp_eq
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
                      eq_manager_q: EquivalenceClassManager,eq_manager_v:EquivalenceClassManager) -> Tuple[bool, Optional[Dict]]:
    rewrite_map: Dict[str, str] = {}
    v_count_big_output_name = None
    # V 的 SUM(C) 参数的规范列到 V 输出列名的映射 {Canonical_Col: V_Output_Name}
    #不管里面是表达式or列，统一按照表达式处理
    v_sum_exp_args_map: Dict[str, str] = {}
    for i, v_agg in enumerate(view_spj.aggr_exprs):
        v_output_name = view_spj.aggregates[i].alias
        if str(v_agg.this).upper() == "COUNT_BIG":#有问题?
            v_count_big_output_name =v_output_name
        elif isinstance(v_agg, expressions.Sum):
            v_sum_exp_args_map[str(v_agg.this)] = v_output_name

    # 2. 检查 Q 的聚合函数并记录重写信息
    for q_agg in query_spj.aggr_exprs:
        q_type = query_spj.get_aggregate_type(q_agg)
        if str(q_agg.this) == "*":
            q_arg_canon = None
        else:
            q_arg_canon = str(q_agg.this) #表达式 e.g.2*a+b或 单纯的列

        # A. COUNT(*) 重写检查
        if q_type == "count" and str(q_agg.this) == "*":
            if not v_count_big_output_name:
                print("Test Aggregation Failed: COUNT(*) needs COUNT_BIG(*), which is missing.")
                return False, None
            # 重写：COUNT(*) -> SUM(V_Count_Big_Name)
            rewrite_map[str(q_agg)] = f"SUM({v_count_big_output_name})"

        # B. SUM(E) 检查
        elif q_type == "sum":
            flag=False
            for v_arg in v_sum_exp_args_map:
                if is_exp_eq(q_arg_canon,v_arg,eq_manager_q.get_all_equivalences()):
                    rewrite_map[str(q_agg)] =f"SUM({v_sum_exp_args_map[v_arg]})"
                    flag=True
                    break
            if not flag:
                print(
                    f"Test Aggregation Failed: SUM argument '{q_arg_canon}' not found in view's canonical SUM arguments.")
                return False, None
            else:
                continue

        # C. AVG(E) 重写检查
        elif q_type == "avg":
            # 1. 检查 SUM(E) 部分
            flag = False
            for v_arg in v_sum_exp_args_map:
                if is_exp_eq(q_arg_canon, v_arg, eq_manager_q.get_all_equivalences()):
                    v_sum_output_name =v_sum_exp_args_map[v_arg]
                    flag = True
                    break
            if not flag:
                print(
                    f"Test Aggregation Failed: AVG argument '{q_arg_canon}' not found in view's canonical SUM arguments.")
                return False, None

            # 2. 检查 COUNT_BIG(*) 部分
            if not v_count_big_output_name:
                print("Test Aggregation Failed: AVG(E) needs COUNT_BIG(*), which is missing.")
                return False, None

            # 记录重写：AVG(Q_Arg) -> SUM(V_Sum_Name) / SUM(V_Count_Big_Name)
            #v_sum_output_name = v_sum_exp_args_map[q_arg_canon]
            sum_part = f"{v_sum_output_name}"
            count_part = f"{v_count_big_output_name}"
            rewrite_map[str(q_agg)] = f"SUM({sum_part}) / SUM({count_part})"

        # D. 忽略其他聚合函数

    return True, rewrite_map

def check_agg(query_spj, view_spj,eq_classes_q,eq_classes_v):
    flag1=check_group_by_subset(query_spj, view_spj,eq_classes_q)#query的group by列被包含在view
    if not flag1:
        print("false:check_group_by_subset")
        return False, None
    flag2,rewrite_map = test_aggregation2(query_spj, view_spj, eq_classes_q,eq_classes_v)
    if not flag2:
        return False, None
    return True, rewrite_map
