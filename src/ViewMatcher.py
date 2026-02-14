from typing import Dict
from matcher_with_sub_q import _match_top

def view_match(query_sql: str, view_sql: Dict[str, str]) -> str:
    return _match_top(query_sql,view_sql)

"""
def sql_rewrite(query_sql,c1,c2,c3,changed_select_cols,rewrite_map,view_name="VIEW"):
    query_sql=parse_one(query_sql)
    print(query_sql)
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
                print(type(node.name))
                table_name=find_table(node.name)
            else:
                table_name = node.table
            col_name = node.name
            key = (table_name, col_name)
            print("key",key)
            print(col_replacement_map)
            if key in col_replacement_map:
                return col_replacement_map[key]
        return node
    query_sql= str(query_sql.transform(replace_columns))

    print(query_sql,"**********")
    for expr_old in rewrite_map:
        expr_new=rewrite_map[expr_old]
        #query_sql=query_sql.replace(str(expr_old),str(expr_new))
        query_sql = re.sub(re.escape(expr_old), expr_new, query_sql, flags=re.IGNORECASE)
    fr_ = query_sql.upper().find("FROM")
    wh_ = query_sql.upper().find("WHERE")
    query_sql=query_sql[:fr_]+"FROM "+view_name+"\n"+query_sql[wh_:]

    wh_ = query_sql.upper().find("WHERE")
    gr_ = query_sql.upper().find("GROUP")
    query_sql=query_sql[:wh_]+where_add_str+"\n"+query_sql[gr_:]

    gr_ = query_sql.upper().find("GROUP")
    query_sql = query_sql[:gr_-1]+query_sql[gr_:]
    return query_sql

def _view_match(query_sql,view_sql,detail=True):
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
    #print(type(c1),type(c2),type(changed_select_cols),type(rewrite_map))

    new_query_sql=sql_rewrite(query_sql,c1,c2,c3,changed_select_cols,rewrite_map)
    if detail:
        return True,c1,c2,c3,changed_select_cols,rewrite_map,new_query_sql
    else:
        return new_query_sql
"""






