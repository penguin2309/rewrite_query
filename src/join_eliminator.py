
def ValidateCardinalityPreservingJoin(col_l,col_r,tables_structure):
    #l->r 子->父（被引用表）
    for k in tables_structure[str(col_l.table)].foreign_keys:
        #col_l是非空外键 引用到col_r（而且是唯一键）
        if k.local_columns==str(col_l.col) and \
            k.ref_table==str(col_r.table) and\
                k.ref_columns==str(col_r.col) and \
                    k.is_nullable==False and \
                        k.ref_columns in tables_structure[str(col_r.table)].unique_keys:
            return True
    return False
def exe(ext,v_tables,in_degree,out_degree,graph):
    for t_ in v_tables:
        if t_ in ext:
            if in_degree[t_] == 1 and out_degree[t_] == 0:
                ext.remove(t_)
                for xx in graph:
                    if t_ in graph[xx]:
                        graph[xx].remove(t_)
                        out_degree[xx] -= 1
                        return True,ext,out_degree
    return False,None,None
def QueryPreparation(query_spj,view_spj,tables_structure):
    q_tables = query_spj.tables
    v_tables = view_spj.tables
    if not q_tables.issubset(v_tables):
        return True,query_spj
    elif q_tables == v_tables:
        return True,query_spj

    extra_tables=v_tables-q_tables
    graph = {t_: [] for t_ in v_tables}
    in_degree = {t_: 0 for t_ in v_tables}
    out_degree = {t_: 0 for t_ in v_tables}
    add_eq_classes=[]
    add_tabs=set()
    eq=view_spj.get_all_EQpredicates()
    for (col_l,col_r) in eq:
        if col_l.table!=col_r.table:
            #l->r
            col_l_table=str(col_l.table)
            col_r_table=str(col_r.table)
            if ValidateCardinalityPreservingJoin(col_l,col_r,tables_structure):
                graph[col_l_table].append(col_r_table)
                out_degree[col_l_table]=out_degree.get(col_l_table,0)+1
                in_degree[col_r_table]=in_degree.get(col_r_table,0)+1
                add_eq_classes.append((col_l,col_r))
                add_tabs.add(col_l_table)
                add_tabs.add(col_r_table)
            if ValidateCardinalityPreservingJoin(col_r,col_l,tables_structure):
                graph[col_r_table].append(col_l_table)
                out_degree[col_r_table]=out_degree.get(col_r_table,0)+1
                in_degree[col_l_table]=in_degree.get(col_l_table,0)+1
                add_eq_classes.append((col_r,col_l))
                add_tabs.add(col_l_table)
                add_tabs.add(col_r_table)
    ext=v_tables.copy()
    while True:
        (flag,ext_,out_)=exe(ext,v_tables,in_degree,out_degree,graph)
        if not flag:
            break
        else:
            out_degree=out_
            ext=ext_
    if ext is None or ext.issubset(extra_tables):
        for (col_l,col_r) in add_eq_classes:
            query_spj.added_eq_classes.append((col_l,col_r))
            if col_l not in query_spj.col:
                query_spj.col.append(col_l)
            if col_r not in query_spj.col:
                query_spj.col.append(col_r)
            query_spj.tables.add(str(col_l.table))
            query_spj.tables.add(str(col_r.table))
        return True,query_spj
    else:
        return False,query_spj

def eliminate_joins(query_spj,view_spj,tables_structure):
    return QueryPreparation(query_spj,view_spj,tables_structure)