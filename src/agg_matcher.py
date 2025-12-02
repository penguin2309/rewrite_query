from spj_view_matcher import spj_view_match
from sqlglot import expressions as exp
from typing import Dict,Tuple,Optional
from SPJGExpression import SPJGExpression
from EquivalenceClassManager import EquivalenceClassManager
from PredicateClassifier import *
from TableStructure import *

def check_group_by_subset(query_spj, view_spj,eq_classes_q):
    query_cols = query_spj.get_all_group_by_columns()
    view_cols = view_spj.get_all_group_by_columns()
    #print("query_cols",query_cols)
    #print("view_cols",view_cols)
    if len(view_cols)==0:
        return True
    for col in query_cols:
        eq_cols=eq_classes_q.get_all_eq_cols(col)
        flag=False
        for eq_col in eq_cols:
            if eq_col in view_cols:
                flag=True
                break
        if not flag:
            return False
    return True

def test_aggregation2(query_spj: SPJGExpression, view_spj: SPJGExpression,
                      eq_manager_q: EquivalenceClassManager,eq_manager_v:EquivalenceClassManager) -> Tuple[bool, Optional[Dict]]:
    rewrite_map: Dict[str, str] = {}
    v_count_big_output_name = None
    # V 的 SUM(C) 参数的规范列到 V 输出列名的映射 {Canonical_Col: V_Output_Name}
    v_sum_args_map: Dict[column, str] = {}
    v_sum_exp_args_map: Dict[str, str] = {}
    for i, v_agg in enumerate(view_spj.aggr_exprs):
        v_output_name = view_spj.aggregates[i].alias
        print("\n\n\n\n\n\n\n\n******&(***(:",type(v_agg.this),v_agg.this
              ,"\n\n\n\n\n\n\n\n")
        if str(v_agg.this).upper() == "COUNT_BIG":#有问题！！
            v_count_big_output_name =v_output_name
        elif isinstance(v_agg, expressions.Sum):
            if isinstance(v_agg.this,expressions.Column):
                v_arg_col = column(v_agg.this.table, v_agg.this.name)
                v_sum_args_map[v_arg_col] = v_output_name
            else:
                v_sum_exp_args_map[str(v_agg.this)] = v_output_name
            print("\n\n\n\n\n\n\n\n******&(***(\n\n\n\n\n\n\n\n")

    # 2. 检查 Q 的聚合函数并记录重写信息
    for q_agg in query_spj.aggr_exprs:
        q_type = query_spj.get_aggregate_type(q_agg)
        if str(q_agg.this) == "*":
            q_arg_canon = None
        else:
            if isinstance(q_agg.this,expressions.Column):
                q_arg_canon = column(q_agg.this.table, q_agg.this.name)
            else:
                q_arg_canon = str(q_agg.this) #表达式 e.g.2*a+b

        # A. COUNT(*) 重写检查
        if q_type == "count" and str(q_agg.this) == "*":
            if not v_count_big_output_name:
                print("Test Aggregation Failed: COUNT(*) needs COUNT_BIG(*), which is missing.")
                return False, None
            # 重写：COUNT(*) -> SUM(V_Count_Big_Name)
            rewrite_map[str(q_agg)] = f"SUM({v_count_big_output_name})"

        # B. SUM(E) 检查
        elif q_type == "sum":
            # 检查 V 是否有 SUM(E') 且 E' 与 E 语义等价 (q_arg_canon 必须在 V 的映射中)
            if isinstance(q_arg_canon,str):
                for v_arg in v_sum_exp_args_map:
                    continue
            if q_arg_canon not in v_sum_args_map:
                if len(view_spj.get_all_group_by_columns())==0:
                    eq_cols=eq_manager_v.get_all_eq_cols(q_arg_canon)
                    if eq_cols is not None and len(eq_cols)!=0:
                        for eq_col in eq_cols:
                            if eq_col in view_spj.col:
                                rewrite_map[str(q_agg)] = f"SUM({eq_col})"
                        continue
                print(
                    f"Test Aggregation Failed: SUM argument '{q_arg_canon}' not found in view's canonical SUM arguments.")
                return False, None

            v_output_name = v_sum_args_map[q_arg_canon]
            # 重写：SUM(Q_Arg) -> SUM(V_Output_Name)
            rewrite_map[str(q_agg)] = f"SUM({v_output_name})"

        # C. AVG(E) 重写检查
        elif q_type == "avg":
            # 1. 检查 SUM(E) 部分
            if q_arg_canon not in v_sum_args_map:
                print(f"Test Aggregation Failed: AVG(E) needs SUM(E), and E's canonical form is missing.")
                return False, None

            # 2. 检查 COUNT_BIG(*) 部分
            if not v_count_big_output_name:
                print("Test Aggregation Failed: AVG(E) needs COUNT_BIG(*), which is missing.")
                return False, None

            # 记录重写：AVG(Q_Arg) -> SUM(V_Sum_Name) / SUM(V_Count_Big_Name)
            v_sum_output_name = v_sum_args_map[q_arg_canon]
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
