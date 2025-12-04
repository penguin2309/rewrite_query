from sqlglot import expressions as exp
from numpy.f2py.auxfuncs import throw_error
from sqlglot.expressions import false
from typing import Dict, Tuple, Optional
from SPJGExpression import SPJGExpression
from EquivalenceClassManager import EquivalenceClassManager
from PredicateClassifier import *
from TableStructure import *
from SPJGExpression import *
def test1(eq_classes_q, eq_classes_v):
    v_to_q = {}
    compensation = set()
    for j, v_class in enumerate(eq_classes_v):
        flag = False
        for i, q_class in enumerate(eq_classes_q):
            if v_class <= q_class:
                flag = True
                v_to_q[j] = i
                break
        if not flag and len(v_class) > 1:
            print("in test1,false1")
            return False, None
    # 分类（按照query）
    from collections import defaultdict
    q_groups = defaultdict(list)
    for j, i in v_to_q.items():
        q_groups[i].append(j)
    for i, q_class in enumerate(eq_classes_q):
        indices_from_q = q_groups.get(i, [])
        view_classes = [eq_classes_v[j] for j in indices_from_q]
        covered = set().union(*view_classes) if view_classes else set()
        missing = q_class - covered
        components = []
        for T in view_classes:
            if T:
                components.append(T)
        for x in missing:
            components.append({x})
        # e.g. component= [{1, 2},{3, 4}, {5}]
        if not components:
            continue
        if len(components) == 1:
            continue
        rep = next(iter(components[0]))
        for comp in components[1:]:
            other_elem = next(iter(comp))
            compensation.add(frozenset({rep, other_elem}))
    compensation = [tuple(sorted(c)) for c in compensation]
    return True, compensation

def test2(PR_q, PR_v, eq_classes_q):
    small_number = 1e-10
    ranges_q = [(-float('inf'), float('inf')) for _ in range(len(eq_classes_q) + len(PR_q) + 100)]
    ranges_v = [(-float('inf'), float('inf')) for _ in range(len(eq_classes_q) + len(PR_q) + 100)]
    idx = {}
    representative = [column(None, None) for _ in range(len(eq_classes_q) + len(PR_q) + 100)]
    offset = 0
    base = len(eq_classes_q)
    for pr in PR_q:
        col, op, num = pr.col, pr.op, pr.num
        if idx.get(col) is None:
            for ii, eq_class in enumerate(eq_classes_q):
                if col in eq_class:
                    idx[col] = ii
                    break
        if idx.get(col) is None:
            idx[col] = offset + base
            representative[offset + base] = col
            offset += 1
        i = idx[col]
        match op:
            case '>':
                ranges_q[i] = (max(ranges_q[i][0], num), ranges_q[i][1])
            case '>=':
                ranges_q[i] = (max(ranges_q[i][0], num - small_number), ranges_q[i][1])
            case '<':
                ranges_q[i] = (ranges_q[i][0], min(ranges_q[i][1], num))
            case '<=':
                ranges_q[i] = (ranges_q[i][0], min(ranges_q[i][1], num + small_number))
            case '==':
                ranges_q[i] = (max(ranges_q[i][0], num - small_number), min(ranges_q[i][1], num + small_number))
            case _:
                throw_error("error in test2")

    for pr in PR_v:
        col, op, num = pr.col, pr.op, pr.num
        if idx.get(col) is None:
            for i, eq_class in enumerate(eq_classes_q):
                if col in eq_class:
                    idx[col] = i
                    break
        i = idx[col]
        match op:
            case '>':
                ranges_v[i] = (max(ranges_v[i][0], num), ranges_v[i][1])
            case '>=':
                ranges_v[i] = (max(ranges_v[i][0], num - small_number), ranges_v[i][1])
            case '<':
                ranges_v[i] = (ranges_v[i][0], min(ranges_v[i][1], num))
            case '<=':
                ranges_v[i] = (ranges_v[i][0], min(ranges_v[i][1], num + small_number))
            case '==':
                ranges_v[i] = (max(ranges_v[i][0], num - small_number), min(ranges_v[i][1], num + small_number))
            case _:
                throw_error("error in test2")
    for i in range(0, base + offset):
        if i < base:
            representative[i] = min(eq_classes_q[i])

    compensation = []
    for i, (small_query, big_query) in enumerate(ranges_q):
        (small_view, big_view) = ranges_v[i]
        # small_view small_query and big_query big_view
        if small_view > small_query or big_query > big_view:
            return False, None
        else:
            if small_view < small_query:
                compensation.append((representative[i], ">", small_query))
            if big_view > big_query:
                compensation.append((representative[i], "<", big_query))

    return True, compensation

def test3(PU_q, PU_v, eq_classes_q,legal_view_col_set):
    col_to_rep = {}
    for cls in eq_classes_q:
        rep = min(cls)
        for col in cls:
            col_to_rep[col] = rep
    def replace_column(node):
        if isinstance(node, exp.Column):
            current_col = column(tab=node.table or "", col=node.name)
            if current_col in col_to_rep:
                rep_col = col_to_rep[current_col]
                return exp.Column(
                    this=exp.Identifier(this=rep_col.col),
                    table=exp.Identifier(this=rep_col.table) if rep_col.table else None
                )
        return node

    norm_v_keys = set()
    for pred in PU_v:
        try:
            canon_pred = pred.transform(replace_column)
            key = canon_pred.sql(dialect="spark", normalize=False)
            norm_v_keys.add(key)
        except Exception:
            continue

    norm_q_keys = set()
    for pred in PU_q:
        try:
            canon_pred = pred.transform(replace_column)
            key = canon_pred.sql(dialect="spark", normalize=False)
            norm_q_keys.add(key)
        except Exception:
            return False
    dif=norm_v_keys.difference(norm_q_keys)^norm_q_keys.difference(norm_v_keys)

    #print("test3_____",dif)
    if dif.issubset(norm_q_keys):
        # 检查输出列是否满足条件（3.2节）：
        replacement_map = {}
        for d in dif:
            p = parse_one(d)
            cols = list(p.find_all(exp.Column))
            for c in cols:
                c_obj = column(c.table,c.name)
                if c_obj not in legal_view_col_set:
                    replacement = find_col_to_replace(legal_view_col_set, eq_classes_q, c_obj)
                    if replacement:
                        replacement_map[c_obj] = replacement
                    else:
                        return False, None
        if replacement_map:
            #print("replacement map_:",replacement_map)
            replaced_results = []
            for d in dif:
                p = parse_one(d)
                def replace_(node):
                    if isinstance(node, exp.Column):
                        current_col = column(node.table, node.name)
                        if current_col in replacement_map:
                            new_col = replacement_map[current_col]
                            return exp.Column(
                                this=exp.Identifier(this=new_col.name),
                                table=exp.Identifier(this=new_col.table) if new_col.table else None
                            )
                    return node
                replaced_expr = p.transform(replace_)
                replaced_results.append(replaced_expr.sql())
            return True, replaced_results
        else:
            return True, list(dif)
    else:
        return False,None

def find_eq_cols(col,eq_classes):
    for cls in eq_classes:
        if col in cls:
            return cls
    return None

def find_col_to_replace(legal_view_col_set,eq_classes,col,eq_classes_q=None):
    eq_class = find_eq_cols(col, eq_classes)
    if eq_class:
        col_q=None
        for c in eq_class:
            if c in legal_view_col_set:
                if eq_classes_q is None:
                    return c
                col_q = c
                break
        for cols in eq_classes_q:
            for c in cols:
                if c==col_q:
                    #print(c.alias,"###")
                    return c
    return None

def can_express_compensating_predicates(legal_view_col_set,eq_classes,compensation_eq,compensation_ra):
    changed_compensation_eq=[]
    changed_compensation_ra=[]
    for(col1,col2) in compensation_eq:
        if col1 in legal_view_col_set and col2 in legal_view_col_set:
            changed_compensation_eq.append((col1,col2))
        elif col1 in legal_view_col_set and col2 not in legal_view_col_set:
            c=find_col_to_replace(legal_view_col_set,eq_classes,col2)
            if c:
                changed_compensation_eq.append((col1,c))
            else:
                return False,None,None
        elif col1 not in legal_view_col_set and col2 in legal_view_col_set:
            c=find_col_to_replace(legal_view_col_set,eq_classes,col1)
            if c:
                changed_compensation_eq.append((c,col2))
            else:
                return False,None,None
        else:
            c1=find_col_to_replace(legal_view_col_set,eq_classes,col1)
            c2=find_col_to_replace(legal_view_col_set,eq_classes,col2)
            if c1 and c2:
                changed_compensation_eq.append((c1,c2))
            else:
                return False,None,None
    for (col,op,num) in compensation_ra:
        if col in legal_view_col_set:
            changed_compensation_ra.append((col,op,num))
        else:
            c=find_col_to_replace(legal_view_col_set,eq_classes,col)
            #print(legal_view_col_set,eq_classes,col)
            if c:
                changed_compensation_ra.append((c,op,num))
            else:
                return False,None,None
    return True,changed_compensation_eq,changed_compensation_ra

def can_compute_query_output_from_view(cols_q,cols_v,eq_classes_q,eq_classes_v):
    changed_select_cols=[]
    for col in cols_q:
        c=find_col_to_replace(cols_v,eq_classes_q,col,eq_classes_v)
        if c:
            changed_select_cols.append((c,col))
        else:
            return False,None
    changed_select_cols=list(set(changed_select_cols))
    return True,changed_select_cols

def spj_view_match(query_spj,view_spj,PR_q,PU_q,PR_v,PU_v,eq_classes_q,eq_classes_v):
    (flag1,compensation_eq)=test1(eq_classes_q.get_all_equivalences(),eq_classes_v.get_all_equivalences())
    if not flag1:
        print("false1")
        return False, None, None, None, None
    (flag2,compensation_ra)=test2(PR_q,PR_v,eq_classes_q.get_all_equivalences())
    if not flag2:
        print("false2")
        return False, None, None, None, None
    flag3,c3=test3(PU_q,PU_v,eq_classes_q.get_all_equivalences(),set(view_spj.col))
    if not flag3:
        print("false3")
        return False, None, None, None, None
    (flag4,c1,c2)=can_express_compensating_predicates(set(view_spj.col),eq_classes_q.get_all_equivalences(),compensation_eq,compensation_ra)
    if not flag4:
        print("false4")
        return False, None, None, None, None
    (flag5,changed_select_cols)=can_compute_query_output_from_view(query_spj.old_col,view_spj.col,eq_classes_q.get_all_equivalences(),eq_classes_v.get_all_equivalences())
    if not flag5:
        print("false5")
        return False, None, None, None, None

    return True,c1,c2,c3,changed_select_cols